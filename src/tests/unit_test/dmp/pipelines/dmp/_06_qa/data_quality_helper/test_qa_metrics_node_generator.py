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

from unittest import mock

from src.dmp.pipelines.dmp._06_qa.data_quality_helper.qa_metrics_node_generator import (
    get_master_qa_nodes,
    get_qa_nodes,
)

parameters_qa = {
    "qa_aggregation": {
        "catalog": {
            "domain_a": [
                {"aggregation_table_name_a_1": "aggregation_catalog_name_a_1"},
                {"aggregation_table_name_a_2": "aggregation_catalog_name_a_2"},
            ],
            "domain_b": [
                {"aggregation_table_name_b_1": "aggregation_catalog_name_b_1",}
            ],
        }
    },
    "qa_feature": {
        "catalog": {
            "domain_a": [
                {"feature_table_name_a_1": "feature_catalog_name_a_1"},
                {"feature_table_name_a_2": "feature_catalog_name_a_2"},
            ],
            "domain_b": [{"feature_table_name_b_1": "feature_catalog_name_b_1"},],
        }
    },
}


@mock.patch(
    "src.dmp.pipelines.dmp._06_qa.data_quality_helper.qa_metrics_node_generator.get_config_parameters",
    return_value=parameters_qa,
    autospec=True,
)
@mock.patch(
    "src.dmp.pipelines.dmp._06_qa.data_quality_helper.qa_metrics_node_generator.get_catalog_file_path",
    return_value="dummy/file/path",
    autospec=True,
)
@mock.patch(
    "src.dmp.pipelines.dmp._06_qa.data_quality_helper.qa_metrics_node_generator.get_calculate_qa_metrics_node_wrapper"
)
class TestQaNodeGenerator:

    # Test of aggregation QA nodes
    def test_get_qa_nodes_for_aggregation(
        self,
        mock_get_calculate_qa_metrics_node_wrapper,
        mock_get_catalog_file_path,
        mock_get_config_parameters,
        spark_session,
    ):
        aggregation_qa_nodes = get_qa_nodes(project_context=None, layer="aggregation")

        assert len(aggregation_qa_nodes) == 3
        assert aggregation_qa_nodes[0].inputs == [
            "aggregation_catalog_name_a_1_qa_1w",
            "l4_qa_sample_unique_msisdn",
            "params:pipeline",
            "params:qa_aggregation",
        ]
        assert aggregation_qa_nodes[0].outputs == [
            "l4_qa_aggregation_aggregation_table_name_a_1_metrics",
            "l4_qa_aggregation_aggregation_table_name_a_1_monthly_unique_msisdn",
            "l4_qa_aggregation_aggregation_table_name_a_1_outliers",
        ]
        assert aggregation_qa_nodes[0].tags == {
            "de_qa",
            "de_qa_aggregation",
            "de_qa_metrics_aggregation_aggregation_table_name_a_1",
        }

        assert aggregation_qa_nodes[1].inputs == [
            "aggregation_catalog_name_a_2_qa_1w",
            "l4_qa_sample_unique_msisdn",
            "params:pipeline",
            "params:qa_aggregation",
        ]
        assert aggregation_qa_nodes[1].outputs == [
            "l4_qa_aggregation_aggregation_table_name_a_2_metrics",
            "l4_qa_aggregation_aggregation_table_name_a_2_monthly_unique_msisdn",
            "l4_qa_aggregation_aggregation_table_name_a_2_outliers",
        ]
        assert aggregation_qa_nodes[1].tags == {
            "de_qa",
            "de_qa_aggregation",
            "de_qa_metrics_aggregation_aggregation_table_name_a_2",
        }

        assert aggregation_qa_nodes[2].inputs == [
            "aggregation_catalog_name_b_1_qa_1w",
            "l4_qa_sample_unique_msisdn",
            "params:pipeline",
            "params:qa_aggregation",
        ]
        assert aggregation_qa_nodes[2].outputs == [
            "l4_qa_aggregation_aggregation_table_name_b_1_metrics",
            "l4_qa_aggregation_aggregation_table_name_b_1_monthly_unique_msisdn",
            "l4_qa_aggregation_aggregation_table_name_b_1_outliers",
        ]
        assert aggregation_qa_nodes[2].tags == {
            "de_qa",
            "de_qa_aggregation",
            "de_qa_metrics_aggregation_aggregation_table_name_b_1",
        }

        assert mock_get_calculate_qa_metrics_node_wrapper.call_count == 3
        mock_get_calculate_qa_metrics_node_wrapper.assert_any_call(
            "dummy/file/path", "aggregation"
        )

    # Test of feature QA nodes
    def test_get_qa_nodes_for_feature(
        self,
        mock_get_calculate_qa_metrics_node_wrapper,
        mock_get_catalog_file_path,
        mock_get_config_parameters,
        spark_session,
    ):
        feature_qa_nodes = get_qa_nodes(project_context=None, layer="feature")

        assert len(feature_qa_nodes) == 3
        assert feature_qa_nodes[0].inputs == [
            "feature_catalog_name_a_1",
            "l4_qa_sample_unique_msisdn",
            "params:pipeline",
            "params:qa_feature",
        ]
        assert feature_qa_nodes[0].outputs == [
            "l4_qa_feature_feature_table_name_a_1_metrics",
            "l4_qa_feature_feature_table_name_a_1_monthly_unique_msisdn",
            "l4_qa_feature_feature_table_name_a_1_outliers",
        ]
        assert feature_qa_nodes[0].tags == {
            "de_qa",
            "de_qa_feature",
            "de_qa_metrics_feature_feature_table_name_a_1",
        }

        assert feature_qa_nodes[1].inputs == [
            "feature_catalog_name_a_2",
            "l4_qa_sample_unique_msisdn",
            "params:pipeline",
            "params:qa_feature",
        ]
        assert feature_qa_nodes[1].outputs == [
            "l4_qa_feature_feature_table_name_a_2_metrics",
            "l4_qa_feature_feature_table_name_a_2_monthly_unique_msisdn",
            "l4_qa_feature_feature_table_name_a_2_outliers",
        ]
        assert feature_qa_nodes[1].tags == {
            "de_qa",
            "de_qa_feature",
            "de_qa_metrics_feature_feature_table_name_a_2",
        }

        assert feature_qa_nodes[2].inputs == [
            "feature_catalog_name_b_1",
            "l4_qa_sample_unique_msisdn",
            "params:pipeline",
            "params:qa_feature",
        ]
        assert feature_qa_nodes[2].outputs == [
            "l4_qa_feature_feature_table_name_b_1_metrics",
            "l4_qa_feature_feature_table_name_b_1_monthly_unique_msisdn",
            "l4_qa_feature_feature_table_name_b_1_outliers",
        ]
        assert feature_qa_nodes[2].tags == {
            "de_qa",
            "de_qa_feature",
            "de_qa_metrics_feature_feature_table_name_b_1",
        }

        assert mock_get_calculate_qa_metrics_node_wrapper.call_count == 3
        mock_get_calculate_qa_metrics_node_wrapper.assert_any_call(
            "dummy/file/path", "feature"
        )

    # Test of Master QA nodes
    def test_get_master_qa_nodes(
        self,
        mock_get_calculate_qa_metrics_node_wrapper,
        mock_get_catalog_file_path,
        mock_get_config_parameters,
        spark_session,
    ):
        master_qa_nodes = get_master_qa_nodes(project_context=None)

        assert len(master_qa_nodes) == 1
        assert master_qa_nodes[0].inputs == [
            "l4_qa_sample_unique_msisdn",
            "l6_master",
            "l6_master_old",
            "params:pipeline",
            "params:qa_master",
        ]
        assert master_qa_nodes[0].outputs == [
            "l4_qa_master_metrics",
            "l4_qa_master_monthly_unique_msisdn",
            "l4_qa_master_outliers",
        ]
        assert master_qa_nodes[0].tags == {
            "de_qa",
            "de_qa_master",
            "de_qa_metrics_master",
        }

        assert mock_get_calculate_qa_metrics_node_wrapper.call_count == 1
        mock_get_calculate_qa_metrics_node_wrapper.mock_calls
        mock_get_calculate_qa_metrics_node_wrapper.assert_any_call(
            "dummy/file/path", "master", "dummy/file/path"
        )
