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

from typing import Any, Dict, List

import yaml

from utils import GetKedroContext
from utils.spark_data_set_helper import get_catalog_file_path


def _build_parquet_catalog(filepath: str) -> Dict[str, Any]:
    return {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": filepath,
    }


def _build_aggregation_1_weekstart_input_catalog(filepath: str) -> Dict[str, Any]:
    return {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "filepath": filepath,
        "save_args": {"mode": "overwrite", "partitionBy": "weekstart"},
        "load_args": {
            "partition_filter_period": "1w",
            "partition_column": "weekstart",
            "partition_date_format": "%Y-%m-%d",
        },
    }


def _build_int_parquet_catalogs(
    pipeline: str, key: str, layer: str, file: str
) -> Dict[str, Any]:

    if pipeline == "training":
        return _build_parquet_catalog(
            f"mck_dmp_training/06_qa_check/{layer}/{key}_{layer}/{file}.parquet"
        )
    elif pipeline == "scoring":
        return _build_parquet_catalog(
            f"mck_dmp_score/06_qa_check/{layer}/{key}_{layer}/{file}.parquet"
        )
    else:
        raise Exception(f"QA Catalog Generator doesn't support {pipeline} pipeline.")


def _build_combined_parquet_catalogs(
    pipeline: str, layer: str, file: str
) -> Dict[str, Any]:

    if pipeline == "training":
        return _build_parquet_catalog(
            f"mck_dmp_training/06_qa_check/{layer}/{file}.parquet"
        )
    elif pipeline == "scoring":
        return _build_parquet_catalog(
            f"mck_dmp_score/06_qa_check/{layer}/{file}.parquet"
        )
    else:
        raise Exception(f"QA Catalog Generator doesn't support {pipeline} pipeline.")


def _get_catalog_entries_without_domain(params: Dict[str, Any]) -> List[str]:
    items = {}
    for value in params["catalog"].values():
        for subdict in value:
            items.update(subdict)
    return items


def generate_catalog(
    pipeline: str, aggregation_params: Dict[str, Any], feature_params: Dict[str, Any],
):
    """
    Given a list of aggregation and feature parameters, dynamically create the catalog for QA output
    Args:
        pipeline: taining/scoring
        aggregation_params: Parameters in aggregation layer.
        feature_params: Parameters in feature layer.
    """

    env = GetKedroContext.get_environment()

    qa_catalogs: Dict[str, Any] = {}

    layer_catalog_dict = {
        "aggregation": _get_catalog_entries_without_domain(aggregation_params),
        "feature": _get_catalog_entries_without_domain(feature_params),
    }

    # 0) Aggregation QA Input Catalog
    for value in layer_catalog_dict["aggregation"].values():
        qa_catalogs[f"{value}_qa_1w"] = _build_aggregation_1_weekstart_input_catalog(
            get_catalog_file_path(value)
        )

    # 1) Intermediate QA paths
    for layer, catalog_entries in layer_catalog_dict.items():
        for key in catalog_entries.keys():
            qa_catalogs[f"l4_qa_{layer}_{key}_metrics"] = _build_int_parquet_catalogs(
                pipeline, key, layer, "metrics"
            )
            qa_catalogs[
                f"l4_qa_{layer}_{key}_monthly_unique_msisdn"
            ] = _build_int_parquet_catalogs(
                pipeline, key, layer, "monthly_unique_msisdn"
            )
            qa_catalogs[f"l4_qa_{layer}_{key}_outliers"] = _build_int_parquet_catalogs(
                pipeline, key, layer, "outliers"
            )

    # 2) Joined QA Paths
    for layer in ["aggregation", "feature", "master"]:
        qa_catalogs[f"l4_qa_{layer}_metrics"] = _build_combined_parquet_catalogs(
            pipeline, layer, "metrics"
        )
        qa_catalogs[
            f"l4_qa_{layer}_monthly_unique_msisdn"
        ] = _build_combined_parquet_catalogs(pipeline, layer, "monthly_unique_msisdn")
        qa_catalogs[f"l4_qa_{layer}_outliers"] = _build_combined_parquet_catalogs(
            pipeline, layer, "outliers"
        )

    with open(f"conf/{env}/catalog_qa.yml", "w+") as qa_catalog_yaml:
        yaml.dump(qa_catalogs, qa_catalog_yaml, default_flow_style=False)

    return True
