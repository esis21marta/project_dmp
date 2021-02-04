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
    select_right_columns,
)
from src.dmp.pipelines.external.klop.combine_scores_to_hive.nodes import (
    combine_scores_to_hive,
    join_scores,
)
from src.dmp.pipelines.external.klop.weekly_score_update.nodes import (
    drop_duplicate_msisdns,
    join_to_whitelist,
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
                func=join_to_whitelist,
                inputs=[
                    "klop_master_whitelist",
                    "joined_scores",
                    "params:score_columns",
                ],
                outputs="joined_with_whitelist",
            ),
            node(
                func=combine_scores_to_hive,
                inputs=[
                    "joined_with_whitelist",
                    "params:default_postgres_columns",
                    "params:score_columns",
                ],
                outputs="combined_scores_table",
            ),
            node(
                func=select_right_columns,
                inputs=["combined_scores_table", "params:columns_to_klop_db",],
                outputs="table_with_right_columns",
            ),
            node(
                func=drop_duplicate_msisdns,
                inputs=["table_with_right_columns",],
                outputs="klop_scoring_table",
            ),
        ],
        tags=["weekly_score_update"],
    )
