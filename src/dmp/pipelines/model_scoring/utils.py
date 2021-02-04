import logging
import os

import pai
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PAI_ENV_VARIABLE = "PAI_RUNS"


def get_model_path_from_pai_run(run_id: str) -> str:
    """
    Fetches model pickle path from PAI run_id.

    :param run_id: PAI run id containing a model
    :return: Model pickle path
    """
    pai_path = pai.get_storage()
    if pai_path.startswith("file://"):
        pai_path = "/" + pai_path[5:].lstrip("/")
    pickle_file = pai.list_artifacts(run_id).get("model/model")
    return os.path.join(pai_path, pickle_file)


def get_param_value_from_pai_run(run_id: str, param: str) -> str:
    """
    Fetches a parameter value from PAI run_id.

    :param run_id: PAI run id containing the parameter
    :param param: The parameter for which value is to be fetched
    :return: Parameter value of interest, cast to a string
    """
    params = pai.load_params(run_id=run_id)
    if param not in params:
        raise LookupError(f"{param} not found in {params.columns}")
    return str(params.loc[0, param])


def register_mllib(path: str) -> SparkSession:
    """
    Registers packaged mllib dependencies to the spark with `addPyFile`.

    :param path: Path value used for registration
    :return: SparkSession with dependencies registered

    """
    if not os.path.exists(path):
        logger.error(f"projlib doesn't found in {path}, run `build_mllib.sh`")

    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.addPyFile(path)
    return spark
