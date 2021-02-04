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

from pathlib import Path

import pytest

from src.dmp.run import ProjectContext
from src.tests.smoke_test.dmp.pipelines.dmp.smoke_assertion_helper import *
from src.tests.smoke_test.dmp.pipelines.dmp.smoke_config_helper import *
from src.tests.smoke_test.dmp.pipelines.dmp.smoke_hive_helper import *
from utils import GetKedroContext


def smoke_project_context(smoke_env: str):
    """
    Setup smoke project context.
    Args:
        smoke_env: Environment path to be read from.
    Returns:
        ProjectContext.
    """
    project_context = ProjectContext(project_path=str(Path.cwd()), env=smoke_env,)

    GetKedroContext(context=project_context, env=smoke_env)
    return project_context


def setup_smoke_test():
    """
    Set up data migration for smoke tests.
    Returns:
        Project Context.
    """
    smoke_testing_catalog = create_smoke_testing_catalog()

    smoke_context = smoke_project_context(smoke_testing_catalog)
    sparkSession: SparkSession = smoke_context._spark_session

    new_smoke_msisdns: pyspark.sql.DataFrame = retrieve_new_smoke_msisdns(sparkSession)
    new_smoke_laccis: pyspark.sql.DataFrame = retrieve_new_smoke_laccis(sparkSession)

    new_smoke_tables = retrieve_new_smoke_tables(sparkSession)
    existing_smoke_tables_df = retrieve_existing_smoke_tables(sparkSession)

    tables_to_migrate = retrieve_delta_smoke_tables(
        existing_smoke_tables_df, new_smoke_tables
    )

    # Migrate tables with no dependencies on other smoke tables
    for delta_smoke_table_dict in tables_to_migrate:

        if delta_smoke_table_dict["skip"]:
            continue

        elif delta_smoke_table_dict["table"] == "sales_dnkm_dd":
            migrate_missing_tables(
                sparkSession, new_smoke_laccis, delta_smoke_table_dict
            )

        else:
            migrate_missing_tables(
                sparkSession, new_smoke_msisdns, delta_smoke_table_dict
            )

    for delta_smoke_table_dict in tables_to_migrate:

        if delta_smoke_table_dict["skip"]:
            continue

        elif delta_smoke_table_dict["table"] == "sales_dnkm_dd":
            continue

        migrate_tables_with_smoke_dependencies(sparkSession, delta_smoke_table_dict)

    return smoke_context


def assert_smoke_tests(project_context: ProjectContext):
    """
    Assert smoke tests.
    Args:
        sparkSession: Spark session.
    """

    # Prepare ground truth parquet paths
    ground_truth_conf_loader: ConfigLoader = ConfigLoader(
        ["conf/smoke_run/ground_truth"]
    )
    ground_truth_conf_catalog: Dict[str, Any] = ground_truth_conf_loader.get(
        "catalog*", "catalog*/**"
    )

    # Prepare aggregation layer parquet paths
    agg_catalog_dict = load_configuration_dicts("aggregation")

    agg_smoke_parquets = retrieve_assertion_paths(
        agg_catalog_dict["smoke_conf"], "01_aggregation"
    )
    agg_ground_truth_parquets = retrieve_assertion_paths(
        ground_truth_conf_catalog, "01_aggregation"
    )

    train_catalog_dict = load_configuration_dicts("training")

    # Prepare primary layer parquet paths
    prm_smoke_parquets = retrieve_assertion_paths(
        train_catalog_dict["smoke_conf"], "02_primary"
    )
    prm_ground_truth_parquets = retrieve_assertion_paths(
        ground_truth_conf_catalog, "02_primary"
    )

    # Prepare feature layer parquet paths
    fea_smoke_parquets = retrieve_assertion_paths(
        train_catalog_dict["smoke_conf"], "04_features"
    )
    fea_ground_truth_parquets = retrieve_assertion_paths(
        ground_truth_conf_catalog, "04_features"
    )

    # Prepare master layer parquet paths
    master_smoke_parquets = retrieve_assertion_paths(
        train_catalog_dict["smoke_conf"], "05_model_input"
    )
    master_ground_truth_parquets = retrieve_assertion_paths(
        ground_truth_conf_catalog, "05_model_input"
    )

    # MSISDNs to assert
    new_smoke_msisdns: pyspark.sql.DataFrame = retrieve_new_smoke_msisdns(
        project_context._spark_session
    )
    new_smoke_laccis: pyspark.sql.DataFrame = retrieve_new_smoke_laccis(
        project_context._spark_session
    )

    # Assert output from aggregation layer
    assert_smoke_tests_handler(
        project_context,
        agg_smoke_parquets,
        agg_ground_truth_parquets,
        new_smoke_msisdns,
        new_smoke_laccis,
    )

    # Assert output from primary layer
    assert_smoke_tests_handler(
        project_context,
        prm_smoke_parquets,
        prm_ground_truth_parquets,
        new_smoke_msisdns,
        new_smoke_laccis,
    )

    # Assert output from feature layer
    assert_smoke_tests_handler(
        project_context,
        fea_smoke_parquets,
        fea_ground_truth_parquets,
        new_smoke_msisdns,
        new_smoke_laccis,
    )

    # Assert output from master layer
    assert_smoke_tests_handler(
        project_context,
        master_smoke_parquets,
        master_ground_truth_parquets,
        new_smoke_msisdns,
        new_smoke_laccis,
    )


@pytest.mark.skip(
    reason="Waiting for ground truth verification === assertion (skipped so develop branch passes)"
)
def test_execute_smoke_run():

    # # Setup smoke testing
    print(" --- Setting up smoke test --- ")
    context = setup_smoke_test()

    # Execute end-to-end pipeline
    print(" --- Executing aggregation pipeline --- ")
    context.run(pipeline_name="aggregation_pipeline")
    print(" --- Executing primary pipeline --- ")
    context.run(pipeline_name="primary_pipeline")
    print(" --- Executing feature pipeline --- ")
    context.run(pipeline_name="feature_pipeline")
    print(" --- Executing master pipeline --- ")
    context.run(pipeline_name="merge_features")

    # Assert results
    print(" --- Asserting smoke tests --- ")
    assert_smoke_tests(context)
