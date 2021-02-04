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

from src.dmp.pipelines.external.common.python.utils import add_pai_tag, filter_down
from src.dmp.pipelines.external.income_prediction.nodes import (
    categorical_none_to_nan_string,
    cross_validate_nfolds,
    enforce_feature_types,
    remove_missing_targets,
    replace_large_targets_with_median,
    select_columns_of_interest,
    take_log_of_target,
    train_on_all_data,
)


def create_pipeline():
    return Pipeline(
        [
            node(
                add_pai_tag,
                ["params:pai_tags"],  # here it is reading from parameters.yml
                None,
                tags=["pai:income_prediction"],
            ),
            node(
                filter_down,
                ["master_input_income", "params:filter_query",],
                "after_filter",
            ),
            node(
                remove_missing_targets,
                ["after_filter", "params:target_column_name"],
                "without_missing_targets",
            ),
            node(
                replace_large_targets_with_median,
                [
                    "without_missing_targets",
                    "params:target_column_name",
                    "params:replace_targets_above",
                ],
                "with_replaced_targets",
            ),
            node(
                take_log_of_target,
                ["with_replaced_targets", "params:target_column_name"],
                "with_logs_of_targets",
            ),
            node(
                select_columns_of_interest,
                [
                    "with_logs_of_targets",
                    "params:features_used",
                    "params:target_column_name",
                ],
                "with_columns_of_interest_only",
            ),
            node(
                enforce_feature_types,
                [
                    "with_columns_of_interest_only",
                    "params:features_used",
                    "params:categorical_features",
                ],
                "with_enforced_types",
            ),
            node(
                categorical_none_to_nan_string,
                ["with_enforced_types", "params:categorical_features"],
                "with_categorical_missing_value_handled",
            ),
            node(
                cross_validate_nfolds,
                [
                    "with_categorical_missing_value_handled",
                    "params:features_used",
                    "params:categorical_features",
                    "params:num_folds",
                    "params:catboost_params",
                    "params:target_column_name",
                    "params:num_quantiles",
                ],
                None,
                tags=["pai:income_prediction"],
            ),
            node(
                train_on_all_data,
                [
                    "with_categorical_missing_value_handled",
                    "params:features_used",
                    "params:categorical_features",
                    "params:catboost_params",
                    "params:target_column_name",
                ],
                None,
                tags=["pai:income_prediction"],
            ),
        ]
    )
