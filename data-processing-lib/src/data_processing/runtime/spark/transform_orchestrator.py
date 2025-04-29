# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import os
import socket
from logging import Logger
from typing import Any

import yaml
from data_processing.data_access import DataAccessFactory
from data_processing.runtime import TransformOrchestrator
from data_processing.transform import TransformStatistics
from data_processing.utils import GB
from data_processing.runtime.spark import (
    SparkTransformExecutionConfiguration,
    SparkTransformFileProcessor,
    SparkTransformRuntimeConfiguration,
)
from pyspark.sql import SparkSession


def _init_spark(logger: Logger, runtime_config: SparkTransformRuntimeConfiguration) -> SparkSession:
    server_port_https = int(os.getenv("KUBERNETES_SERVICE_PORT_HTTPS", "-1"))
    if server_port_https == -1:
        # running locally
        spark_config = {"spark.driver.host": "127.0.0.1"}
        return SparkSession.builder.appName(runtime_config.get_name()).config(map=spark_config).getOrCreate()
    else:
        # running in Kubernetes, use spark_profile.yml and
        # environment variables for configuration
        server_port = os.environ["KUBERNETES_SERVICE_PORT"]
        master_url = f"k8s://https://kubernetes.default:{server_port}"

        # Read Spark configuration profile
        config_filepath = os.path.abspath(
            os.path.join(os.getenv("SPARK_HOME"), "work-dir", "config", "spark_profile.yml")
        )
        with open(config_filepath, "r") as config_fp:
            spark_config = yaml.safe_load(os.path.expandvars(config_fp.read()))
        spark_config["spark.submit.deployMode"] = "client"

        # configure the executor pods from template
        executor_pod_template_file = os.path.join(
            os.getenv("SPARK_HOME"),
            "work-dir",
            "src",
            "templates",
            "spark-executor-pod-template.yml",
        )
        spark_config["spark.kubernetes.executor.podTemplateFile"] = executor_pod_template_file
        spark_config["spark.kubernetes.container.image.pullPolicy"] = "Always"

        # Pass the driver IP address to the workers for callback
        myservice_url = socket.gethostbyname(socket.gethostname())
        spark_config["spark.driver.host"] = myservice_url
        spark_config["spark.driver.bindAddress"] = "0.0.0.0"
        spark_config["spark.decommission.enabled"] = True
        logger.info(f"Launching Spark Session with configuration\n" f"{yaml.dump(spark_config, indent=2)}")
        app_name = spark_config.get("spark.app.name", "my-spark-app")
        return SparkSession.builder.master(master_url).appName(app_name).config(map=spark_config).getOrCreate()


class SparkTransformOrchestrator(TransformOrchestrator):
    """
    Class implementing transform orchestration for Python
    """

    def __init__(
        self,
        execution_params: SparkTransformExecutionConfiguration,
        data_access_factory: list[DataAccessFactory],
        runtime_config: SparkTransformRuntimeConfiguration,
        logger: Logger,
    ):
        super().__init__(
            execution_params=execution_params,
            runtime_config=runtime_config,
            data_access_factory=data_access_factory,
            logger=logger,
        )

    def get_resources(self) -> None:
        """
        Get resources and statistics (Runtime specific)
        """
        # create statistics
        self.statistics = TransformStatistics()

    def process_data(self) -> None:
        """
        Data processing for Spark
        """
        # initialize Spark
        spark_session = _init_spark(logger=self.logger, runtime_config=self.runtime_config)
        sc = spark_session.sparkContext
        # broadcast parameters to all executors
        spark_runtime_config = sc.broadcast(self.runtime_config)
        daf = sc.broadcast(self.data_access_factory)
        # Get resources
        cpus = sc.defaultParallelism
        executors = sc._jsc.sc().getExecutorMemoryStatus()
        memory = 0.0
        for i in range(executors.size()):
            memory += executors.toList().apply(i)._2()._1()
        self.resources = {"cpus": cpus, "gpus": 0, "memory": round(memory / GB, 2)}
        self.logger.info(f"Spark cluster resources: {self.resources}")
        # process data on executors

        def process_partition(iterator):
            """
            process partitions
            :param iterator: iterator of records
            :return:
            """
            # local statistics
            statistics = TransformStatistics()
            # create transformer runtime
            d_access_factory = daf.value
            runtime_conf = spark_runtime_config.value
            runtime = runtime_conf.create_transform_runtime()
            # create file processor
            file_processor = SparkTransformFileProcessor(
                data_access_factory=d_access_factory,
                runtime_configuration=runtime_conf,
                statistics=statistics,
                is_folder=self.is_folder,
            )
            first = True
            for f in iterator:
                # for every file
                if first:
                    self.logger.debug(f"partition {f}")
                    # add additional parameters
                    transform_params = runtime.get_transform_config(
                        partition=int(f[1]), data_access_factory=d_access_factory, statistics=statistics
                    )
                    # create transform with partition number
                    file_processor.create_transform(transform_params)
                    first = False
                # process file
                file_processor.process_file(f_name=f[0])
            # flush
            file_processor.flush()
            # enhance statistics
            runtime.compute_execution_stats(statistics.get_execution_stats())
            # return partition's statistics
            return list(statistics.get_execution_stats().items())

        # process data
        self.logger.debug("Begin processing files")
        # process files split by partitions
        self.logger.debug(f"parallelization {self.execution_params.parallelization}")
        if self.execution_params.parallelization > 0:
            source_rdd = sc.parallelize(self.files_to_process, self.execution_params.parallelization)
        else:
            source_rdd = sc.parallelize(self.files_to_process)
        num_partitions = source_rdd.getNumPartitions()
        self.resources = self.resources | {"num partitions": num_partitions}
        self.logger.info(f"Parallelizing execution. Using {num_partitions} partitions")
        stats_rdd = source_rdd.zipWithIndex().mapPartitions(process_partition)
        # build overall statistics
        overall_stats = dict(stats_rdd.reduceByKey(lambda a, b: a + b).collect())
        self._publish_stats(overall_stats)

    def _publish_stats(self, stats: dict[str, Any]) -> None:
        """
        Publishing execution statistics
        :param stats: update to Statistics
        :return: None
        """
        if len(stats) > 0:
            self.statistics.add_stats(stats=stats)

    def _get_stats(self) -> dict[str, Any]:
        """
        get statistics
        """
        return self.statistics.get_execution_stats()
