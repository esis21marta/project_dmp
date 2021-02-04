from datetime import datetime as dt
from typing import Any, Dict, List, Union

import pytz
from kedro.config import ConfigLoader


def load_module(conf_path: str, file: Union[str, List[str]]) -> Dict[str, Any]:
    return ConfigLoader(conf_path).get(*file)


def get_param(module: Dict[str, Any], key: str) -> str:
    return module[key]


def append_current_timestamp(file_path: str) -> str:
    """
    The function appends current timestamp at the
    end of a path or a string.
    Args:
        file_path (str): Input string or a file path.

    Returns:
        str: current timestamp appened string.
    """
    timestamp = current_timestamp()
    return "_".join([file_path, timestamp])


def current_timestamp():
    """
    The function returns current timestamp.
    Returns:

    """
    tz = pytz.timezone("Asia/Jakarta")
    return dt.now(tz=tz).strftime("%Y%m%d%H%M%S")
