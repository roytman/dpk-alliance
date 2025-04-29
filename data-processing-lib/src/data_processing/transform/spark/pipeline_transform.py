from typing import Any

from data_processing.transform import AbstractPipelineTransform, TransformRuntime


class SparkPipelineTransform(AbstractPipelineTransform):
    """
    Transform that executes a set of base transforms sequentially. Data is passed between
    participating transforms in memory
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initializes pipeline execution for the list of transforms
        :param config - configuration parameters - list of transforms in the pipeline.
        Note that transforms will be executed in the order they are defined
        """
        self.partition = config.get("partition_index", 0)
        print("jhjhh")
        print(config)
        super().__init__(config)

    def _get_transform_params(self, runtime: TransformRuntime) -> dict[str, Any]:
        """
        get transform parameters
        :param runtime - runtime
        :return: transform params
        """
        return runtime.get_transform_config(
            partition=self.partition, data_access_factory=self.data_access_factory, statistics=self.statistics
        )

    def _compute_execution_statistics(self, stats: dict[str, Any]) -> None:
        """
        Compute execution statistics
        :param stats: current statistics from flush
        :return: None
        """
        self.statistics.add_stats(stats)
        return
        for _, runtime in self.participants:
            runtime.compute_execution_stats(stats=self.statistics)