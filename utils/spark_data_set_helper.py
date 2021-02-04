import logging
import os
import subprocess
from datetime import datetime

import pytz
from kedro.io import DataSetError

from utils import GetKedroContext, get_config_parameters, get_project_context

logger = logging.getLogger(__name__)


def get_file_path(filepath) -> str:
    """
    :param filepath: File Path
    :return: Complete File Path with HDFS Base Path
    """
    if "://" not in str(filepath):
        hdfs_base_path = GetKedroContext.get_params()["hdfs_base_path"]
        filepath = os.path.relpath(filepath)
        filepath = os.path.join(hdfs_base_path, filepath)
    return filepath


def get_versioned_save_file_path(filepath: str, version: datetime = None) -> str:
    """Updating file path to added created time stamp to version the generated data set

    Args:
        filepath (str): filepath
        version (datetime, optional): If versioned path needs be generated for a given datatime. Defaults to None.

    Returns:
        str: Filepath with version
    """
    if not version:
        version = datetime.now(pytz.timezone("Asia/Jakarta"))

    version_path = f"created_at={version.strftime('%Y-%m-%d')}"
    filepath = os.path.join(filepath, version_path)
    return filepath


def get_versioned_load_file_path(
    filepath: str, version: str = None, version_index: int = None
) -> str:
    """
    Updating file path to added provided version/ latest version to the file path
    :param filepath: file path
    :param version: version time stamp
    :return: versioned file path
    """

    if version_index and (not isinstance(version_index, int) or (version_index < 1)):
        raise IOError("version_index in load_args should be positive integer")

    if version:
        version_path = f"created_at={version}"
    else:
        version_path = (
            f"created_at={get_latest_version(filepath, version_index=version_index)}"
        )

    filepath = os.path.join(filepath, version_path)

    return filepath


def get_latest_version(
    load_path, partition_col="created_at", version_index=None
) -> str:
    """
    Get the most recent partition of a dataset

    :param load_path: path on hdfs
    :param partition_col: partition column used to sort by

    :return: most recent partitonKey
    """
    cmds = f"hdfs dfs -ls -r {load_path}"
    try:
        # list all partitions on hdfs
        process = subprocess.Popen(
            cmds.split(" "), stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()
        stdout = stdout.decode("utf-8")
        stderr = stderr.decode("utf-8")

        if stderr:
            raise IOError(stderr)

        rows = stdout.split("\n")

        # skip first row b/c it's hdfs output
        for row in rows[1:]:
            # search for occurrence of partitionKey in the row
            if f"/{partition_col}=" in row:
                if version_index is not None and version_index > 0:
                    version_index -= 1
                else:
                    key_index = row.index(partition_col)
                    # extract the value of the partitionKey and return
                    partition_version = row[key_index + len(partition_col) + 1 :]
                    return partition_version

    except IOError as error:
        raise IOError(error)

    raise DataSetError("No version found for {}".format(load_path))


def get_catalog_file_path(catalog_name: str, project_context=None) -> str:
    if project_context is None:
        project_context = get_project_context()

    source_catalog = get_config_parameters(project_context, "catalog").get(catalog_name)

    if source_catalog is None:
        logger.info(f"{catalog_name} not found.")
    else:
        return get_file_path(filepath=str(source_catalog.get("filepath")))
