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

from src.dmp.pipelines.external.filter_los_brand_and_sampling.nodes import (
    filter_out_low_los,
    filter_out_null_los,
    filter_out_very_high_los,
    filter_territory,
    remove_partner_existing_customers,
    select_right_columns,
)
from src.dmp.pipelines.external.klop.combine_scores_to_hive.nodes import (
    add_features_flags,
    add_quantiles_flag,
    combine_scores_to_hive,
    join_scores,
)


def create_pipeline():
    return Pipeline(
        [
            node(
                func=join_scores,
                inputs=[
                    "default_probability_table",
                    "income_prediction_table",
                    "propensity_table",
                    "params:columns_to_include",
                ],
                outputs="joined_scores",
            ),
            node(
                func=filter_territory,
                inputs=["joined_scores", "territory_table",],
                outputs="joined_with_territory",
            ),
            node(
                func=filter_out_null_los,
                inputs=["joined_with_territory"],
                outputs="scoring_with_no_null_los",
            ),
            node(
                func=filter_out_low_los,
                inputs=["scoring_with_no_null_los", "params:low_los_threshold"],
                outputs="scoring_with_no_low_los",
            ),
            node(
                func=filter_out_very_high_los,
                inputs=["scoring_with_no_low_los", "params:high_los_threshold"],
                outputs="scoring_with_no_high_los",
            ),
            node(
                func=remove_partner_existing_customers,
                inputs=["scoring_with_no_high_los", "kredivo_users"],
                outputs="scoring_without_kredivo",
            ),
            node(  # remove blacklist from CMS
                func=remove_partner_existing_customers,
                inputs=["scoring_without_kredivo", "blacklist_users"],
                outputs="scoring_without_blacklist",
            ),
            node(
                func=combine_scores_to_hive,
                inputs=[
                    "scoring_without_blacklist",
                    "params:default_postgres_columns",
                    "params:columns_to_rename",
                ],
                outputs="combined_scores_table",
            ),
            # Pipelines to add flag of additional features
            node(
                func=add_features_flags,
                inputs=["scoring_without_blacklist", "params:features_flags"],
                outputs="scoring_with_features_flags",
            ),
            node(
                func=add_quantiles_flag,
                inputs=["scoring_with_features_flags"],
                outputs="scoring_with_quantiles",
            ),
            node(
                func=select_right_columns,
                inputs=["combined_scores_table", "params:columns_to_klop_db"],
                outputs="klop_scoring_table",
            ),
            node(
                func=select_right_columns,
                inputs=["combined_scores_table", "params:columns_to_klop_address_db"],
                outputs="klop_address_table",
            ),
        ],
        tags=["combine_scores_to_hive"],
    )
