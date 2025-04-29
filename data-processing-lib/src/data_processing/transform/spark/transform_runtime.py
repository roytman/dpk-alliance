from typing import Any

from data_processing.data_access import DataAccess, DataAccessFactory
from data_processing.transform import TransformRuntime, TransformStatistics

class DefaultSparkTransformRuntime(TransformRuntime):
    """
    Transformer runtime used by processor to to create Transform specific environment
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create/config this runtime.
        :param params: parameters, often provided by the CLI arguments as defined by a Transform Configuration.
        """
        super().__init__(params)

    def get_transform_config(
        self, partition: int, data_access_factory: list[DataAccessFactory], statistics: TransformStatistics
    ) -> dict[str, Any]:
        """
        Get the dictionary of configuration that will be provided to the transform's initializer.
        This is the opportunity for this runtime to create a new set of configuration based on the
        config/params provided to this instance's initializer.
        :param partition - the partition assigned to this worker, needed by transforms like doc_id
        :param data_access_factory - data access factory class being used by the RayOrchestrator.
        :param statistics - reference to statistics actor
        :return: dictionary of transform init params
        """
        return self.params | {"data_access_factory": data_access_factory} | {"statistics": statistics}

    def get_bcast_params(self, data_access_factory: DataAccessFactory) -> dict[str, Any]:
        """Allows retrieving and broadcasting to all the workers very large
        configuration parameters, like the list of document IDs to remove for
        fuzzy dedup, or the list of blocked web domains for block listing. This
        function is called by the spark runtime after spark initialization, and
        before spark_context.parallelize()
        :param data_access_factory - creates data_access object to download the large config parameter
        """
        return {}