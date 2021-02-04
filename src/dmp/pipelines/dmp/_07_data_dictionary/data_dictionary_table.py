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

from pyspark.sql import SparkSession

from src.dmp.pipelines.dmp._07_data_dictionary.data_dictionary_helper import (
    get_domain,
    get_file_format,
    get_file_size,
    get_layer,
    get_path,
    get_table_description,
    get_user_agg,
    has_pii_level_data,
    load_yaml,
)

logger = logging.getLogger(__name__)


def get_details_from_old_data_dictionary(
    old_data_dict, catalog_name: str, data_dictionary_element: str
) -> Any:
    result = old_data_dict.get(catalog_name, {}).get(data_dictionary_element, None)
    if result is None or result.strip() == "":
        logger.info(
            f"Details not found in old Data Dictionary: Catalog Name: {catalog_name}\t\t - {data_dictionary_element}"
        )
    return result


def data_dictionary_table(
    data_dictionary_df, data_dictionary_table_raw_df, hdfs_base_path, pii_level_columns
):
    df_data_dictionary_table_new = {}
    spark = SparkSession.builder.getOrCreate()

    aggregation_catalog_dict = load_yaml("conf/dmp/aggregation/catalog.yml")
    training_catalog_dict = load_yaml("conf/dmp/training/catalog.yml")
    scoring_catalog_dict = load_yaml("conf/dmp/scoring/catalog.yml")

    for catalog_name, catalog_details in training_catalog_dict.items():
        logger.info(f"Data Dictionary Table - Processing {catalog_name}")

        # Skip if not in raw, aggregate, primary, feature and master
        architecture_layer = get_layer(catalog_name, catalog_details)
        if architecture_layer in ["aggregation", "primary", "feature", "master"]:
            data_dict_details = {
                "file_format": get_file_format(catalog_name, catalog_details),
            }

            # Load Table
            if data_dict_details["file_format"] == "parquet":
                file_path = catalog_details.get("filepath", None)
                if not file_path.startswith("hdfs"):
                    file_path = hdfs_base_path + file_path
                try:
                    df = spark.read.parquet(file_path)
                except:
                    logger.warn(f"Unable to read file {file_path}")
                    continue

            pii_level = get_details_from_old_data_dictionary(
                data_dictionary_df, catalog_name, "pii_level"
            )
            description = get_details_from_old_data_dictionary(
                data_dictionary_df, catalog_name, "description"
            )

            if pii_level == None or description == "":
                pii_level = has_pii_level_data(df, pii_level_columns)
            if description == None or description == "":
                description = get_table_description(catalog_name, catalog_details)

            data_dict_details.update(
                {
                    "catalog_table_name": catalog_name,
                    "user_agg": get_user_agg(df),
                    "pii_level": pii_level,
                    "description": description,
                    "part_col": catalog_details.get("save_args", {}).get(
                        "partitionBy", None
                    ),
                    "part_col_dt_format": scoring_catalog_dict.get(catalog_name, {})
                    .get("load_args", {})
                    .get("partition_date_format", None),
                    "architecture_layer": architecture_layer,
                    "bizgroup": get_domain(catalog_name, catalog_details),
                    "impact_tier": "low",
                    "time_agg": catalog_details.get("save_args", {}).get(
                        "partitionBy", None
                    ),
                    "columns": [],
                }
            )

            if data_dict_details["file_format"] in ["parquet"]:
                data_dict_details.update(
                    {
                        "path": get_path(
                            catalog_name,
                            architecture_layer,
                            aggregation_catalog_dict,
                            training_catalog_dict,
                            scoring_catalog_dict,
                        ),
                        "part_size": get_file_size(file_path),
                    }
                )

            df_data_dictionary_table_new[catalog_name] = data_dict_details

    df_data_dictionary_table_new.update(data_dictionary_table_raw_df)

    return df_data_dictionary_table_new
