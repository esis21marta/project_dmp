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

from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.dmp._06_qa.catalog_setup.check_catalog_tables_exists import (
    check_catalog_tables_exists_wrapper,
)
from src.dmp.pipelines.dmp._06_qa.data_quality_helper.database_node_generator import (
    get_database_nodes,
)
from src.dmp.pipelines.dmp._06_qa.data_quality_helper.qa_metrics_node_generator import (
    get_master_qa_nodes,
    get_nodes_join,
    get_qa_nodes,
)
from src.dmp.pipelines.dmp._06_qa.data_quality_helper.qa_union import qa_union
from src.dmp.pipelines.dmp._06_qa.data_quality_helper.sample_msisdn import (
    get_sample_msisdn_node,
)
from src.dmp.pipelines.dmp._06_qa.data_quality_helper.spark_df_to_oracle_db import (
    spark_df_to_oracle_db_wrapper,
)
from src.dmp.pipelines.dmp._06_qa.qa_outlet.master_geospatial_node import (
    master_geospatial_node_wrapper,
)
from src.dmp.pipelines.dmp._06_qa.qa_outlet.master_revenue_node import (
    master_revenue_node_wrapper,
)
from utils import get_config_parameters

from .accuracy_completeness.accuracy_completeness_score import (
    get_accuracy_completeness_score_nodes,
)
from .consistency.consistency_score import get_consistency_score_nodes

logger = logging.getLogger(__name__)


def create_pipeline(project_context) -> Pipeline:
    """
    Args:
        project_context: Project Context used in Kedro project.

    Returns:
        QA Pipeline: Pipeline containing nodes to execute Quality Assurance pipeline.
    """

    pipeline_mode = get_config_parameters(project_context, "parameters").get("pipeline")

    if pipeline_mode not in ["training", "scoring"]:
        logger.info(f"QA Pipeline is supported only in 'training' or 'scoring'.")
        return Pipeline([])

    sample_node = get_sample_msisdn_node(pipeline_mode)

    aggregate_nodes = get_qa_nodes(project_context, "aggregation")
    aggregation_nodes_union = get_nodes_join(project_context, "aggregation")
    aggregation_nodes_db = get_database_nodes("aggregation")

    feature_nodes = get_qa_nodes(project_context, "feature")
    feature_nodes_union = get_nodes_join(project_context, "feature")
    feature_nodes_db = get_database_nodes("feature")

    master_nodes = get_master_qa_nodes(project_context)
    master_nodes_db = get_database_nodes("master")

    return Pipeline(
        [
            node(
                func=check_catalog_tables_exists_wrapper(project_context),
                inputs="l6_master",
                outputs="abc",
                name="qa_check_catalog_tables",
                tags=["de_qa"],
            ),
            sample_node,
            *aggregate_nodes,
            *aggregation_nodes_union,
            *aggregation_nodes_db,
            *feature_nodes,
            *feature_nodes_union,
            *feature_nodes_db,
            *master_nodes,
            *master_nodes_db,
            *get_accuracy_completeness_score_nodes(),
            *get_consistency_score_nodes(),
            node(
                func=master_geospatial_node_wrapper(
                    project_context, "l3_outlet_master_static"
                ),
                inputs="l3_outlet_master_static",
                outputs={
                    "accuracy_completeness_metrices": "l4_qa_master_outlet_geo_metrics",
                    "outlier_metrics": "l4_qa_master_outlet_geo_outliers",
                },
                name="de_qa_master_outlet_geospatial",
                tags=["de_qa_outlet", "de_qa_master_outlet"],
            ),
            node(
                func=master_revenue_node_wrapper(
                    project_context,
                    "l3_outlet_master_revenue_daily",
                    "l3_outlet_master_revenue_daily_old",
                ),
                inputs=[
                    "l3_outlet_master_revenue_daily",
                    "l3_outlet_master_revenue_daily_old",
                ],
                outputs={
                    "accuracy_completeness_metrices": "l4_qa_master_outlet_revenue_metrics",
                    "outlier_metrics": "l4_qa_master_outlet_revenue_outliers",
                },
                name="de_qa_master_outlet_revenue",
                tags=["de_qa_outlet", "de_qa_master_outlet"],
            ),
            node(
                func=qa_union,
                inputs={
                    "outlet_geo": "l4_qa_master_outlet_geo_metrics",
                    "outlet_revenue": "l4_qa_master_outlet_revenue_metrics",
                },
                outputs="l4_qa_master_outlet_metrics",
                name="qa_master_outlet_union_metrics",
                tags=["de_qa", "de_qa_master_outlet", "de_qa_master_outlet_union",],
            ),
            node(
                func=qa_union,
                inputs={
                    "outlet_geo": "l4_qa_master_outlet_geo_outliers",
                    "outlet_revenue": "l4_qa_master_outlet_revenue_outliers",
                },
                outputs=f"l4_qa_master_outlet_outliers",
                name=f"qa_master_outlet_union_outliers",
                tags=["de_qa", "de_qa_master_outlet", "de_qa_master_outlet_union",],
            ),
            node(
                func=spark_df_to_oracle_db_wrapper(
                    "l4_qa_outlet_master_metrics_db",
                    upsert=True,
                    primary_keys=["layer", "table_name", "run_time"],
                ),
                inputs="l4_qa_master_outlet_metrics",
                outputs="l4_qa_outlet_master_metrics_db",
                name="qa_master_outlet_metrics_db",
                tags=[
                    "de_qa",
                    "de_qa_to_db",
                    "de_qa_to_db_master_outlet",
                    "de_qa_master_outlet",
                ],
            ),
            node(
                func=spark_df_to_oracle_db_wrapper(
                    "l4_qa_outlet_master_outliers_db",
                    upsert=True,
                    primary_keys=["layer", "table_name", "run_time"],
                ),
                inputs="l4_qa_master_outlet_outliers",
                outputs="l4_qa_outlet_master_outliers_db",
                name="qa_master_outlet_outlets_db",
                tags=[
                    "de_qa",
                    "de_qa_to_db",
                    "de_qa_to_db_master_outlet",
                    "de_qa_master_outlet",
                ],
            ),
        ],
        tags=["de_pipeline", "de_qa"],
    )
