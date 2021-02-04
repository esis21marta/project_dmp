# Copyright 2018-present QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import re
import subprocess
from typing import List

import pyspark
import yaml

logger = logging.getLogger(__name__)


def load_yaml(file_name: str) -> dict:
    """
    Loads the YAML file and returns as dictionary

    Arguments:
        file_name {str} -- File name for which we want YAML content

    Returns:
        dict -- YAML content as dictionary
    """
    with open(file_name, "r") as file:
        return yaml.full_load(file)


def write_yaml(file_name, data_dictionary):
    with open(file_name, "w") as file:
        yaml.dump(data_dictionary, file, width=200)


def get_layer(catalog_name: str, catalog_details: dict) -> str:
    """
    Returns the layer information by reading the path

    Arguments:
        catalog_name {str} -- catalog name
        catalog_details {dict} -- catalog details

    Returns:
        str -- layer name
    """
    if "database" in catalog_details:
        return "raw"

    elif catalog_details.get("file_format", None) in ["parquet"]:
        filepath = catalog_details.get("filepath", "")
        if "01_aggregation" in filepath:
            return "aggregation"
        elif "02_primary" in filepath:
            return "primary"
        elif "04_features" in filepath:
            return "feature"
        elif "05_master" in filepath:
            return "master"
        elif "06_qa_check" in filepath:
            return "qa"

    logger.warn(f"Unable to find layer info for {catalog_name}")
    return None


def get_domain(catalog_name: str, catalog_details: dict) -> str:
    """
    Returns domain name of catalog

    Arguments:
        catalog_name {str} -- catalog name
        catalog_details {dict} -- Catalog detail of file

    Returns:
        str -- domain name
    """
    if catalog_details.get("file_format", None) == "parquet":
        regex = r"(01_aggregation|02_primary|04_features)/(.*)/"
        result = re.findall(regex, catalog_details.get("filepath", ""))
        if len(result) != 0:
            return result[0][1]

    logger.warn(f"Unable to find domain name for {catalog_name}")
    return None


def get_path(
    catalog_name: str,
    layer: str,
    aggregation_catalog_dict: dict,
    training_catalog_dict: dict,
    scoring_catalog_dict: dict,
) -> dict:
    """
    Return path of the file

    Arguments:
        catalog_name {str} -- catalog name
        layer {str} -- layer name
        aggregation_catalog_dict {dict} -- aggregation catalog dictionary
        training_catalog_dict {dict} -- training catalog dictionary
        scoring_catalog_dict {dict} -- scoring catalog dictionary

    Returns:
        dict -- path for catalog name, if primary or feature or master layer then will return training and scoring path
    """
    if layer == "aggregation":
        return {
            "aggregation": aggregation_catalog_dict.get(catalog_name, {}).get(
                "filepath", None
            )
        }
    elif layer in ["primary", "feature", "master"]:
        return {
            "training": training_catalog_dict.get(catalog_name, {}).get(
                "filepath", None
            ),
            "scoring": scoring_catalog_dict.get(catalog_name, {}).get("filepath", None),
        }
    return None


def get_file_size(filepath: str) -> str:
    """
    Return the file size of the file

    Arguments:
        filepath {str} -- filepath

    Returns:
        str -- file size
    """
    file_details_command = f"hdfs dfs -du -h {os.path.dirname(filepath)}"
    logger.info(f"Executing {file_details_command}")
    files_details = (
        subprocess.check_output(file_details_command, shell=True)
        .decode("ISO-8859-1")
        .split("\n")
    )
    for file_detail in files_details:
        if filepath in file_detail:
            return re.findall(r"\s*(\d*.{0,1}\d*\s*[KMGT])\s", file_detail)[0]
    logger.error(f"Unable to find file size for {filepath}")


def get_file_format(catalog_name: str, catalog_details: dict) -> str:
    """
    Return the file format of the catalog item

    Arguments:
        catalog_name {str} -- catalog name
        catalog_details {dict} -- catalog details

    Returns:
        str -- file format
    """
    if "file_format" in catalog_details:
        return catalog_details["file_format"]
    elif "database" in catalog_details:
        return "hive"
    logger.error(f"Unable to find file format for {catalog_name}")


def has_pii_level_data(df: pyspark.sql.DataFrame, pii_level_columns: List[str]) -> str:
    """
    Check of any column contains sensitive personal data

    Arguments:
        df {pyspark.sql.DataFrame} -- Pyspark dataframe
        pii_level_columns {List[str]} --

    Returns:
        str -- [description]
    """
    for column in df.columns:
        if column in pii_level_columns:
            return "sensitive personal data"
    return None


def has_sensitive_data(column_name: str, pii_level_columns: List[str]) -> str:
    if column_name in pii_level_columns:
        return "Yes"
    else:
        return "No"


def get_table_description(catalog_name: str, catalog_details: dict) -> str:
    """
    Generate table description name

    Arguments:
        catalog_name {str} -- catalog name
        catalog_details {dict} -- catalog details

    Returns:
        str -- table description
    """
    if "filtered" in str(catalog_name):
        description = "filtered"
    elif (
        ("series" in str(catalog_name))
        or ("scaffold" in str(catalog_name))
        or ("final" in str(catalog_name))
    ):
        description = "scaffold"
    else:
        description = ""
    return f"{get_layer(catalog_name, catalog_details)} {description} layer for {get_domain(catalog_name, catalog_details)}"


def get_user_agg(df):
    user_agg_columns = ["msisdn"]
    for column in df.columns:
        if column in user_agg_columns:
            return "msisdn"
    return None


def join_list(list_items, join_string):
    if list_items is None or list_items == "":
        list_items = []
    return join_string.join(list_items)


def get_source_tables(
    catalog_names: List[str],
    training_catalog_dict: dict,
    aggregation_catalog_dict: dict,
) -> List[str]:
    """
    Convert list catalog name into list of actual paquet file path or table name

    Arguments:
        catalog_names {List[str]} -- catalog name
        training_catalog_dict {dict} -- training catalog dictionary
        aggregation_catalog_dict {dict} -- aggregation catalog dictionary

    Returns:
        List[str] -- List of paquet file paths or hive tables
    """
    source_tables = []
    for catalog_name in catalog_names:
        if catalog_name is None:
            source_tables.append("")
        elif catalog_name in training_catalog_dict:
            if "filepath" in training_catalog_dict[catalog_name]:
                source_tables.append(training_catalog_dict[catalog_name]["filepath"])
            elif "database" in training_catalog_dict[catalog_name]:
                source_tables.append(
                    f"{training_catalog_dict[catalog_name]['database']}.{training_catalog_dict[catalog_name]['table']}"
                )
            else:
                logger.error(f"filepath or database not found for {catalog_name}.")
        elif catalog_name in aggregation_catalog_dict:
            if "filepath" in aggregation_catalog_dict[catalog_name]:
                source_tables.append(aggregation_catalog_dict[catalog_name]["filepath"])
            elif "database" in aggregation_catalog_dict[catalog_name]:
                source_tables.append(
                    f"{aggregation_catalog_dict[catalog_name]['database']}.{aggregation_catalog_dict[catalog_name]['table']}"
                )
            else:
                logger.error(f"filepath or database not found for {catalog_name}.")
        else:
            logger.error(
                f"{catalog_name} not found in training or aggregation catalog."
            )
    return source_tables


def get_possible_values(dtype: str) -> str:
    """Returns the Possible Value field

    Arguments:
        dtype {str} -- data type of the column

    Returns:
        str -- possible value description for given data type
    """
    if dtype in ["string"]:
        return "Any string"
    elif dtype in ["bigint", "double", "int", "tinyint", "float"]:
        return "Any number"
    elif dtype.startswith("decimal"):
        return "Any number"
    elif dtype in ["date"]:
        return "Any date"
    elif dtype.startswith("array"):
        list_type = re.findall(r"array<(.+?)>", dtype)[0]
        return f"Any list of {list_type}"
    else:
        logger.error(f"New type found : {dtype}")
    return None
