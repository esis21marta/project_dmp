import subprocess

from sqlalchemy import Column, MetaData, String, Table, create_engine

from utils import get_config_parameters
from utils.get_kedro_context import GetKedroContext


def run_cmd(args_list):
    """
    run linux commands
    """
    # import subprocess
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err


def get_hive_path(hivepath: str) -> str:
    db, table = hivepath.split(".")
    return f"hdfs:///user/hive/warehouse/{db}.db/{table}"


def get_last_modified_date(filepath, file_format) -> str:
    if file_format == "hive":
        filepath = get_hive_path(filepath)
    (ret, out, err) = run_cmd(["hadoop", "fs", "-stat", '"%y"', filepath])

    modif_date = ""

    if not err:
        modif_date = str(out.decode("utf-8").rstrip()).replace('"', "")

    return modif_date


def get_datasetname(filepath: str, file_format: str) -> str:
    datasetname = ""
    if file_format == "hive":
        datasetname = filepath.split(".")[-1]
    elif file_format in ["parquet", "csv"]:
        datasetname = filepath.split("/")[-1].split(".")[0]

    return datasetname


def log_to_db(**data):
    context = GetKedroContext.get_context()
    conf_params = get_config_parameters(
        project_context=context, config="parameters.yml"
    )
    conf_session = get_config_parameters(
        project_context=context, config="credentials.yml"
    )

    """
    get database and table info from parameters.yml
    log_dataset_pipeline:
        enable_log: True
        table_name: "table_name"
    """
    if conf_params["log_dataset_pipeline"]["enable_log"]:
        con = conf_session["qa_credentials_con_string"]["con"]
        table_name = conf_params["log_dataset_pipeline"]["table_name"]

        engine = create_engine(con, echo=False)
        meta = MetaData(engine)

        table = Table(
            table_name,
            meta,
            Column("dataset_name", String(100)),
            Column("filepath", String(300)),
            Column("data_type", String(50)),
            Column("version", String(50)),
            Column("start_date", String(20)),
            Column("end_date", String(20)),
            Column("last_modified", String(50)),
            Column("run_time", String(50)),
        )

        # add modification_date & datasetname
        data["last_modified"] = get_last_modified_date(
            data["filepath"], data["data_type"]
        )
        data["dataset_name"] = get_datasetname(data["filepath"], data["data_type"])

        # If table don't exist, Create.
        table.create(engine, checkfirst=True)

        with engine.connect() as connection:
            connection.execute(table.insert(), data)
