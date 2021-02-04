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

from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.external.binary_classification.nodes import (
    enforce_feature_types,
    train_on_last_weeks,
    validate_out_of_time,
)
from src.dmp.pipelines.external.common.python.utils import add_pai_tag, filter_down


def create_pipeline():
    return Pipeline(
        [
            node(
                add_pai_tag,
                ["params:pai_tags"],  # here it is reading from parameters.yml
                None,
                tags=["pai:binary_classification"],
            ),
            node(filter_down, ["master", "params:common_query"], "after_common_filter"),
            node(
                enforce_feature_types,
                [
                    "after_common_filter",
                    "params:features_used",
                    "params:categorical_features",
                ],
                "with_enforced_types",
            ),
            node(
                validate_out_of_time,
                [
                    "with_enforced_types",
                    "params:features_used",
                    "params:categorical_features",
                    "params:train_query",
                    "params:test_query",
                    "params:weeks_train",
                    "params:weeks_test",
                    "params:classifier_params",
                    "params:weekstart_column_name",
                    "params:train_target_expression_map",
                    "params:test_target_expression_map",
                    "params:weeks_gap",
                    "params:reliability_diagrams_num_bins",
                    "params:gini_params",
                ],
                None,
                tags=["pai:binary_classification"],
            ),
            node(
                train_on_last_weeks,
                [
                    "with_enforced_types",
                    "params:features_used",
                    "params:categorical_features",
                    "params:train_query",
                    "params:weeks_train",
                    "params:classifier_params",
                    "params:weekstart_column_name",
                    "params:train_target_expression_map",
                    "params:extract_shap_values",
                    "params:log_train_data",
                ],
                None,
                tags=["pai:binary_classification"],
            ),
        ]
    )
