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
from typing import Dict

from kedro.config import MissingConfigException
from kedro.pipeline import node

from src.dmp.pipelines.dmp._06_qa.data_quality_helper.qa_union import qa_union
from src.dmp.pipelines.dmp._06_qa.metrics.metrics import (
    get_calculate_qa_metrics_node_wrapper,
)
from utils import get_config_parameters
from utils.spark_data_set_helper import get_catalog_file_path

logger = logging.getLogger(__name__)


def _get_catalog(project_context, layer: str) -> Dict[str, str]:
    name_to_catalog_name_dict = {}
    try:

        catalog = get_config_parameters(project_context, "parameters_qa")[
            f"qa_{layer}"
        ]["catalog"]

        if catalog is None:
            logger.info(f"There are no catalog entries in qa_{layer}.")
            return {}
        else:
            for k, v in catalog.items():
                for sublist in v:
                    name_to_catalog_name_dict.update(sublist)

        return name_to_catalog_name_dict

    except MissingConfigException:
        logger.error("parameters_qa.yml file is missing.")
        return {}


def get_qa_nodes(project_context, layer: str):
    """
    Returns a list of nodes to execute in QA pipeline relating to calculation of metrics from source data.
    Args:
        project_context: Project Context.
        layer: Layer of nodes

    Returns:
        List of nodes
    """
    name_to_catalog_name_dict = _get_catalog(project_context, layer)

    if len(name_to_catalog_name_dict) == 0:
        return []

    nodes = []
    for name, catalog_name in name_to_catalog_name_dict.items():
        file_path = get_catalog_file_path(catalog_name, project_context)

        nodes.append(
            node(
                get_calculate_qa_metrics_node_wrapper(file_path, layer),
                inputs={
                    "df": f"{catalog_name}_qa_1w"
                    if layer == "aggregation"
                    else catalog_name,
                    "mode": f"params:pipeline",
                    "df_sample_msisdn": "l4_qa_sample_unique_msisdn",
                    "qa_params": f"params:qa_{layer}",
                },
                outputs={
                    "metric_output": f"l4_qa_{layer}_{name}_metrics",
                    "outlier_output": f"l4_qa_{layer}_{name}_outliers",
                    "monthly_unique_msisdn_output": f"l4_qa_{layer}_{name}_monthly_unique_msisdn",
                },
                tags=["de_qa", f"de_qa_{layer}", f"de_qa_metrics_{layer}_{name}"],
            )
        )
    return nodes


def get_master_qa_nodes(project_context):
    """
    Returns a list of master layer nodes to execute in QA pipeline relating to calculation of metrics from source data.
    Args:
        project_context: Project Context.

    Returns:
        List of nodes
    """

    layer = "master"

    try:
        new_master_file_path = get_catalog_file_path("l6_master", project_context)
        old_master_file_path = get_catalog_file_path("l6_master_old", project_context)

    except AttributeError:
        logger.info("l6_master_old or/and l6_master missing in catalog.")
        return []

    nodes = [
        node(
            get_calculate_qa_metrics_node_wrapper(
                new_master_file_path, layer, old_master_file_path
            ),
            inputs={
                "df": "l6_master",
                "df_old": "l6_master_old",
                "mode": f"params:pipeline",
                "df_sample_msisdn": "l4_qa_sample_unique_msisdn",
                "qa_params": f"params:qa_{layer}",
            },
            outputs={
                "metric_output": f"l4_qa_{layer}_metrics",
                "outlier_output": f"l4_qa_{layer}_outliers",
                "monthly_unique_msisdn_output": f"l4_qa_{layer}_monthly_unique_msisdn",
            },
            tags=["de_qa", f"de_qa_{layer}", f"de_qa_metrics_{layer}"],
        )
    ]

    return nodes


def get_nodes_join(project_context, layer: str):
    """
    Returns a list of nodes to execute in QA pipeline relating to union-ing of dataframes.
    Args:
        project_context: Project Context.
        layer: Layer of nodes

    Returns:
        List of nodes
    """

    name_to_catalog_name_dict = _get_catalog(project_context, layer)

    if len(name_to_catalog_name_dict) == 0:
        return []

    nodes = [
        node(
            func=qa_union,
            inputs={
                table_name: f"l4_qa_{layer}_{table_name}_monthly_unique_msisdn"
                for table_name in name_to_catalog_name_dict.keys()
            },
            outputs=f"l4_qa_{layer}_monthly_unique_msisdn",
            name=f"qa_{layer}_union_monthly_unique_msisdn",
            tags=[
                "de_qa",
                f"de_qa_{layer}",
                "de_qa_unique_msisdn",
                f"de_qa_{layer}_union",
            ],
        ),
        node(
            func=qa_union,
            inputs={
                table_name: f"l4_qa_{layer}_{table_name}_metrics"
                for table_name in name_to_catalog_name_dict.keys()
            },
            outputs=f"l4_qa_{layer}_metrics",
            name=f"qa_{layer}_union_metrics",
            tags=["de_qa", f"de_qa_{layer}", "de_qa_metrics", f"de_qa_{layer}_union"],
        ),
        node(
            func=qa_union,
            inputs={
                table_name: f"l4_qa_{layer}_{table_name}_outliers"
                for table_name in name_to_catalog_name_dict.keys()
            },
            outputs=f"l4_qa_{layer}_outliers",
            name=f"qa_{layer}_union_outliers",
            tags=["de_qa", f"de_qa_{layer}", "de_qa_outliers", f"de_qa_{layer}_union"],
        ),
    ]
    return nodes
