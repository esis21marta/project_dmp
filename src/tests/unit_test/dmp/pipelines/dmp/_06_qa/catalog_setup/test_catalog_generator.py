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

from src.dmp.pipelines.dmp._06_qa.catalog_setup.catalog_generator import (
    generate_catalog,
)
from src.tests.unit_test.dmp.pipelines.dmp._06_qa.catalog_setup.catalog_qa_output import (
    scoring_catalog,
    training_catalog,
)


@mock.patch(
    "src.dmp.pipelines.dmp._06_qa.catalog_setup.catalog_generator.get_catalog_file_path",
    return_value="dummy/file/path",
    autospec=True,
)
@mock.patch("builtins.open", new_callable=mock.mock_open)
@mock.patch("yaml.dump")
class TestQaCatalogGenerator:
    aggregation_params = {
        "catalog": {
            "domain_a": [
                {"aggregation_table_name_a_1": "aggregation_catalog_name_a_1"},
                {"aggregation_table_name_a_2": "aggregation_catalog_name_a_2"},
            ],
            "domain_b": [
                {"aggregation_table_name_b_1": "aggregation_catalog_name_b_1",}
            ],
        }
    }
    feature_params = {
        "catalog": {
            "domain_a": [
                {"feature_table_name_a_1": "feature_catalog_name_a_1"},
                {"feature_table_name_a_2": "feature_catalog_name_a_2"},
            ],
            "domain_b": [{"feature_table_name_b_1": "feature_catalog_name_b_1"},],
        }
    }

    # Test of dmp/training
    @mock.patch(
        "src.dmp.pipelines.dmp._06_qa.catalog_setup.catalog_generator.GetKedroContext.get_environment",
        return_value="dmp/training",
        autospec=True,
    )
    def test_catalog_generation_for_training(
        self,
        mock_get_environment,
        mock_yaml_dump,
        mock_open_write,
        mock_get_catalog_file_path,
        spark_session,
    ):
        generate_catalog(
            pipeline="training",
            aggregation_params=self.aggregation_params,
            feature_params=self.feature_params,
        )
        mock_open_write.assert_called_with("conf/dmp/training/catalog_qa.yml", "w+")
        mock_yaml_dump.assert_called_with(
            training_catalog, mock.ANY, default_flow_style=False
        )

    # Test of dmp/scoring
    @mock.patch(
        "src.dmp.pipelines.dmp._06_qa.catalog_setup.catalog_generator.GetKedroContext.get_environment",
        return_value="dmp/scoring",
        autospec=True,
    )
    def test_catalog_generation_for_scoring(
        self,
        mock_get_environment,
        mock_yaml_dump,
        mock_open_write,
        mock_get_catalog_file_path,
        spark_session,
    ):
        generate_catalog(
            pipeline="scoring",
            aggregation_params=self.aggregation_params,
            feature_params=self.feature_params,
        )
        mock_open_write.assert_called_with("conf/dmp/scoring/catalog_qa.yml", "w+")
        mock_yaml_dump.assert_called_with(
            scoring_catalog, mock.ANY, default_flow_style=False
        )

    # Test of dmp_production
    @mock.patch(
        "src.dmp.pipelines.dmp._06_qa.catalog_setup.catalog_generator.GetKedroContext.get_environment",
        return_value="dmp_production",
        autospec=True,
    )
    def test_catalog_generation_for_production(
        self,
        mock_get_environment,
        mock_yaml_dump,
        mock_open_write,
        mock_get_catalog_file_path,
        spark_session,
    ):
        generate_catalog(
            pipeline="scoring",
            aggregation_params=self.aggregation_params,
            feature_params=self.feature_params,
        )
        mock_open_write.assert_called_with("conf/dmp_production/catalog_qa.yml", "w+")
        mock_yaml_dump.assert_called_with(
            scoring_catalog, mock.ANY, default_flow_style=False
        )
