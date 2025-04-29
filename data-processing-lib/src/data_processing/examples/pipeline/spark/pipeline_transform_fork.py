from data_processing.runtime.spark import (
    SparkTransformLauncher,
    SparkTransformRuntimeConfiguration,
)
from data_processing.examples.noop.spark import (
    NOOPSparkTransformConfiguration,
    NOOP1SparkTransformConfiguration,
)
from data_processing.examples.resize.spark import ResizeSparkTransformConfiguration
from data_processing.transform import PipelineTransformConfiguration
from data_processing.transform.spark import SparkPipelineTransform
from data_processing.utils import get_logger


logger = get_logger(__name__)


class PipelineForkSparkTransformConfiguration(SparkTransformRuntimeConfiguration):

    def __init__(self):
        """
        Initialization
        """
        super().__init__(
            transform_config=PipelineTransformConfiguration(
                pipeline=[
                    ResizeSparkTransformConfiguration(),
                    [
                        NOOPSparkTransformConfiguration(),
                        NOOP1SparkTransformConfiguration(),
                    ],
                ],
                transform_class=SparkPipelineTransform,
            )
        )


if __name__ == "__main__":
    launcher = SparkTransformLauncher(PipelineForkSparkTransformConfiguration())
    logger.info("Launching resize/noop transform")
    launcher.launch()