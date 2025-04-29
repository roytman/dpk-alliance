# (C) Copyright IBM Corp. 2025.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from data_processing.examples.noop.spark import NOOPSparkTransformConfiguration
from data_processing.examples.resize.spark import ResizeSparkTransformConfiguration
from data_processing.transform import PipelineTransformConfiguration
from data_processing.transform.spark import SparkPipelineTransform
from data_processing.data_access import DataAccess, DataAccessFactory
from data_processing.transform.spark.transform_runtime import DefaultSparkTransformRuntime
from data_processing.utils import get_logger
from data_processing.runtime.spark import (
    SparkTransformLauncher,
    SparkTransformRuntimeConfiguration,
)
from typing import Any

logger = get_logger(__name__)

class PipelineSparkTransformConfiguration(SparkTransformRuntimeConfiguration):
    """
    Implements the PythonTransformConfiguration for NOOP as required by the PythonTransformLauncher.
    NOOP does not use a RayRuntime class so the superclass only needs the base
    python-only configuration.
    """

    def __init__(self):
        """
        Initialization
        """
        super().__init__(
            transform_config=PipelineTransformConfiguration(
                pipeline=[
                    ResizeSparkTransformConfiguration(),
                    NOOPSparkTransformConfiguration(),
                ],
                transform_class=SparkPipelineTransform,
            )
        )


if __name__ == "__main__":
    # launcher = NOOPRayLauncher()
    launcher = SparkTransformLauncher(PipelineSparkTransformConfiguration())
    logger.info("Launching resize/noop transform")
    launcher.launch()