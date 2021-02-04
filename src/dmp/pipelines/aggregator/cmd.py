import logging
import subprocess
from typing import Any, Dict, Tuple

import pysftp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def shell_cmd(*args) -> Tuple[Any]:
    """
    The function executes linux shell commands via python interface.
    Returns:
        Tuple: returns command output and error
    """
    command = " ".join([*args])
    logger.info(f"Running command: {command}")
    process = subprocess.Popen([*args], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (output, error) = process.communicate()
    if process.returncode:
        logger.error(
            f"Error running command: {command}. Return code: {process.returncode}, Error: {error}"
        )
    return output, error


def sftp_transfer(server: Dict[str, Any], src: str, dst: str) -> None:
    """
    The function transfers file via sftp.
    Args:
        server (Dict[str, Any]): [description]
        src (str): [description]
        dst (str): [description]
    """
    with pysftp.Connection(
        host=server["host_ip"], username=server["username"], password=server["password"]
    ) as sftp:
        sftp.put(src, dst)
