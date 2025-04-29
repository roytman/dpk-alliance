import os
import sys

from data_processing.utils import ParamsUtils
from data_processing.runtime.spark import SparkTransformLauncher
from pipeline_transform_run import PipelineSparkTransformConfiguration
from data_processing.data_access import compute_data_location
from data_processing.data_access import DataAccessFactory

# create launcher


# create parameters
# create parameters
#input_folder = compute_data_location("/data/revital/dpk/transforms/universal/resize/spark/test-data/input/")
input_folder = compute_data_location("test-data/resize/input") # "/data/revital/dpk/transforms/universal/resize/spark/test-data/input/"
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "output"))
#local_conf = {
#    "input_folder": input_folder,
#    "output_folder": output_folder,
#}

local_conf_input = {
    "input_folder": input_folder,
}
local_conf_output = {
    "output_folder": output_folder,
}
worker_options = {"num_cpus": 0.8}
params = {
    # Data access. Only required parameters are specified
    #"data_local_config": ParamsUtils.convert_to_ast(local_conf),
    # Data access. Only required parameters are specified
    "input_local_config": ParamsUtils.convert_to_ast(local_conf_input),
    "output_local_config": ParamsUtils.convert_to_ast(local_conf_output),
    # resize configuration
    # "resize_max_mbytes_per_table":  0.02,
    "resize_max_rows_per_table": 250,
    "noop_sleep_sec": 0,

}

sys.argv = ParamsUtils.dict_to_req(d=params)
launcher = SparkTransformLauncher(runtime_config=PipelineSparkTransformConfiguration(),
                                  data_access_factory=[DataAccessFactory(cli_arg_prefix="input_"), DataAccessFactory(cli_arg_prefix="output_")],)



# launch
launcher.launch()