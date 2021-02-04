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
    add_dummy_lookalike_score,
    filter_out_apple,
    filter_out_low_los,
    filter_out_null_los,
    filter_out_very_high_los,
    merge_tables,
    remove_partner_existing_customers,
    rename_columns_and_coalesce,
    select_random_sample,
    select_right_columns,
    select_specific_brands,
    split_score_table,
)


def create_pipeline():
    return Pipeline(
        [
            node(
                func=add_dummy_lookalike_score,
                inputs=["scoring_table"],
                outputs="scoring_with_lookalike_score",
            ),
            node(
                func=filter_out_null_los,
                inputs=["scoring_with_lookalike_score"],
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
                func=select_specific_brands,
                inputs=["scoring_with_no_high_los", "params:brand_to_include"],
                outputs="scoring_filter_brand",
            ),
            node(
                func=filter_out_apple,
                inputs=[
                    "scoring_filter_brand",
                    "params:apple_manufacturer",
                    "params:apple_devices",
                ],
                outputs="scoring_filter_apple",
            ),
            node(
                func=remove_partner_existing_customers,
                inputs=["scoring_filter_apple", "partner_existing_customers"],
                outputs="without_partner_existing_customers",
            ),
            node(
                func=split_score_table,
                inputs=[
                    "without_partner_existing_customers",
                    "previous_campaign_table",
                ],
                outputs=["previous_campaign_score_table", "remaining_score_table"],
            ),
            node(
                func=select_random_sample,
                inputs=["remaining_score_table", "params:num_random_samples"],
                outputs="remaining_score_table_sampled",
            ),
            node(
                func=merge_tables,
                inputs=[
                    "previous_campaign_score_table",
                    "remaining_score_table_sampled",
                ],
                outputs="score_table_sampled",
            ),
            node(
                func=select_right_columns,
                inputs=["score_table_sampled", "params:columns_to_include"],
                outputs="scoring_filter_columns",
            ),
            node(
                func=rename_columns_and_coalesce,
                inputs=[
                    "scoring_filter_columns",
                    "params:columns_to_rename",
                    "params:final_sample_number_files",
                ],
                outputs="scoring_filtered_sampled",
            ),
        ],
        tags=["filter_los_brand_and_sample"],
    )
