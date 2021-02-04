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
from typing import Any

import pandas as pd
from pyspark.sql import SparkSession

from src.dmp.pipelines.dmp._07_data_dictionary.data_dictionary_helper import (
    get_domain,
    get_layer,
    get_possible_values,
    has_sensitive_data,
    join_list,
    load_yaml,
)

logger = logging.getLogger(__name__)


def get_details_from_old_data_dictionary(
    old_data_dict, catalog_name: str, column_name: str, data_dictionary_element: str
) -> Any:
    result = (
        old_data_dict.get(catalog_name, {})
        .get("columns", {})
        .get(column_name, {})
        .get(data_dictionary_element, None)
    )
    if result is None:
        logger.info(
            f"Details not found in old Data Dictionary: Catalog Name: {catalog_name}\t\t - Column Name: {column_name}\t\t - {data_dictionary_element}"
        )
    return result


def data_dictionary_column_tabular(
    old_data_dict: dict, hdfs_base_path: str, pii_level_columns: list
) -> pd.DataFrame:
    data_dictionary_column_tabular = []
    training_catalog_dict = load_yaml("conf/dmp/training/catalog.yml")
    spark = SparkSession.builder.getOrCreate()

    for catalog_name, catalog_details in training_catalog_dict.items():

        # Skip for catalog files which are not in aggregation primary or feature
        layer = get_layer(catalog_name, catalog_details)
        if layer in ["aggregation", "primary", "feature"]:

            # Try to read catalog file
            logger.info(f"Processing {catalog_name}")
            file_path = catalog_details.get("filepath", None)
            if not file_path.startswith("hdfs"):
                file_path = hdfs_base_path + file_path
            try:
                df = spark.read.parquet(file_path)
            except:
                logger.warn(f"Failed to read {file_path}")
                continue

            for column_name, column_type in df.dtypes:
                data_dictionary_column_tabular.append(
                    {
                        "Table Name": catalog_name,
                        "File Path": file_path,
                        "Data Element": column_name,
                        "Data Type": column_type,
                        "Bizterm Group": get_domain(catalog_name, catalog_details),
                        "Layer": get_layer(catalog_name, catalog_details),
                        "Business Term": get_details_from_old_data_dictionary(
                            old_data_dict, catalog_name, column_name, "business_term"
                        ),
                        "Description": get_details_from_old_data_dictionary(
                            old_data_dict, catalog_name, column_name, "description"
                        ),
                        "Biz Logic": get_details_from_old_data_dictionary(
                            old_data_dict, catalog_name, column_name, "biz_logic"
                        ),
                        "Origin (Source Data)": join_list(
                            get_details_from_old_data_dictionary(
                                old_data_dict,
                                catalog_name,
                                column_name,
                                "origin_source_data",
                            ),
                            ",",
                        ),
                        "Sensitivity (PII/non PII)": has_sensitive_data(
                            column_name, pii_level_columns
                        ),
                        "Impact": "Low",
                        "Time Aggregation": "Weekly",
                        "Possible Values": get_possible_values(column_type),
                    }
                )
    return pd.DataFrame(data_dictionary_column_tabular)
