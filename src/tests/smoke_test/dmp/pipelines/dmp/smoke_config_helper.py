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

from typing import Any, Dict

import yaml
from kedro.config import ConfigLoader

SMOKE_CATALOG_BASE_PATH = "smoke_run/smoke_test/"
SMOKE_CATALOG_PATH = "conf/" + SMOKE_CATALOG_BASE_PATH + "catalog.yml"

SPARK_HIVE_DATASET = "src.dmp.io.spark_hive_dataset.SparkHiveDataSet"
SPARK_PARQUET_DATASET = "src.dmp.io.spark_data_set.SparkDataSet"


def load_configuration_dicts(mode: str) -> Dict[str, Dict[str, Any]]:
    """
    Load all catalogs
    Args:
        mode: Catalog to look at (aggregation or training)
    Returns:

    """
    if mode == "aggregation":
        prod_conf_path = ["conf/dmp/aggregation"]
        smoke_conf_path = ["conf/smoke_run/aggregation"]

    elif mode == "training":
        prod_conf_path = ["conf/dmp/training"]
        smoke_conf_path = ["conf/smoke_run/training"]

    else:
        raise Exception("Please pass a valid mode in list ('aggregation', 'training') ")

    prod_conf_loader: ConfigLoader = ConfigLoader(prod_conf_path)
    prod_conf_catalog: Dict[str, Any] = prod_conf_loader.get("catalog.yml")

    smoke_conf_loader: ConfigLoader = ConfigLoader(smoke_conf_path)
    smoke_conf_catalog: Dict[str, Any] = smoke_conf_loader.get(
        "catalog*", "catalog*/**"
    )

    return {
        "prod_conf": prod_conf_catalog,
        "smoke_conf": smoke_conf_catalog,
    }


def prepare_smoke_test_configuration(mode: str) -> Dict[str, Any]:
    """
    Given real catalog configurations, modify parquet paths for smoke testing purposes.
    Args:
        mode: aggregation or training
    Returns:
        Dictionary of catalog entries.
    """
    catalog_out: Dict[str, Any] = {}

    configuration_dicts = load_configuration_dicts(mode)
    configuration_dicts.get("smoke_db_dir")
    prod_conf_catalog = configuration_dicts["prod_conf"]
    smoke_conf_catalog = configuration_dicts["smoke_conf"]

    for prod_catalog_name, prod_catalog_dict in prod_conf_catalog.items():
        modified_catalog = smoke_conf_catalog.get(prod_catalog_name)
        catalog_type_str = prod_catalog_dict.get("type")

        """
            Account Balance Table is so big and don't have msisdn on it.
            So we just refer to original table, not smoke table.
        """

        if modified_catalog is None:
            raise Exception(
                f"The catalog name '{prod_catalog_name}' cannot be found in smoke testing '{mode}' catalog."
            )

        if catalog_type_str == SPARK_PARQUET_DATASET:
            prod_catalog_dict.update(
                {"filepath": modified_catalog, "partitions": 1,}
            )

        elif catalog_type_str == SPARK_HIVE_DATASET:
            prod_catalog_dict.update(
                {
                    "database": "mck",  # TODO: make as parameter
                    "table": modified_catalog,
                }
            )

        else:
            raise Exception(
                f"Please pass a valid catalog type: ({SPARK_PARQUET_DATASET} and {SPARK_HIVE_DATASET}"
            )

        catalog_out.update({prod_catalog_name: prod_catalog_dict})

    return catalog_out


def retrieve_assertion_paths(
    catalog: Dict[str, Any], subfolder_filter: str
) -> Dict[str, str]:
    """
    Retrieve specific parquet paths.
    Args:
        catalog: Catalog dictionary.
        subfolder_filter: String filter.
    Returns:
        Dictionary of catalog names and parquet paths.
    """
    output_dict = {}

    for catalog_name, catalog_dict in catalog.items():

        if subfolder_filter in catalog_dict:
            output_dict.update({catalog_name: catalog_dict})

    return output_dict


def create_smoke_testing_catalog() -> str:
    """
    Returns combined agg and training catalog.
    Returns:
        Combined agg and training catalog.
    """
    agg_catalog = prepare_smoke_test_configuration("aggregation")
    training_catalog = prepare_smoke_test_configuration("training")
    catalogs_merged = {
        **training_catalog,
        **agg_catalog,
    }  # We put agg catalog in back, because agg has partition which training doesn't

    with open(SMOKE_CATALOG_PATH, "w+") as smoke_test_catalog:
        yaml.dump(catalogs_merged, smoke_test_catalog, default_flow_style=False)

    return SMOKE_CATALOG_BASE_PATH
