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

# node functions
from .master_table import (
    agg_dynamic_categorical_features_mode,
    agg_dynamic_features_last,
    agg_dynamic_numerical_features_max,
    agg_dynamic_numerical_features_sum_mean,
    build_target_variable,
    create_lagged_features,
    create_network_metric_features,
    filter_consecutive_zero_sales,
    filter_dynamic_master_month,
    filter_outlet_created_at_date,
    join_agg_dynamic_features,
    join_ds_outlets_master_table,
    prepare_non_tsf_outlet_ids,
    prepare_outlets_static_features,
)


def create_pipeline() -> Pipeline:
    """Create reseller analytics data science master table aggregation pipeline

    What does it do:
        - Prepare static and dynamic reseller master tables
        - Builds target variable
        - Filter outlets based on minimum threshold of non-zero sales days
        - Aggregate dynamic (revenue) master table to monthly granularity
        - Join both master tables into a single unified reseller master table

    :return: A Pipeline object
    """
    return Pipeline(
        [
            node(
                func=prepare_non_tsf_outlet_ids,
                inputs=["ra_mck_int_master_dynamic_non_tsf", "params:ra_master_table"],
                outputs="ra_mck_int_master_dynamic_non_tsf_month_outlet_ids",
                name="ra_mck_int_master_dynamic_non_tsf_month_outlet_ids",
            ),
            node(
                func=prepare_outlets_static_features,
                inputs=["ra_mck_int_dmp_static_master_table", "params:ra_master_table"],
                outputs="ra_mck_int_features_dmp_static_prepared",
                name="ra_mck_int_features_dmp_static_prepared",
            ),
            node(
                func=filter_outlet_created_at_date,
                inputs=[
                    "ra_mck_int_dmp_dynamic_master_table",
                    "params:ra_master_table",
                    "ra_mck_int_master_dynamic_non_tsf_month_outlet_ids",
                ],
                outputs=[
                    "ra_mck_int_features_dmp_dynamic_filtered_created_at",
                    "ra_filter_outlet_created_at_date",
                ],
                name="ra_mck_int_features_dmp_dynamic_filtered_created_at",
            ),
            node(
                func=build_target_variable,
                inputs=[
                    "ra_mck_int_features_dmp_dynamic_filtered_created_at",
                    "params:ra_master_table",
                ],
                outputs="ra_mck_int_features_dmp_dynamic_target_variable_created",
                name="ra_mck_int_features_dmp_dynamic_create_target_variable",
            ),
            node(
                func=filter_consecutive_zero_sales,
                inputs=[
                    "ra_mck_int_features_dmp_dynamic_target_variable_created",
                    "params:ra_master_table",
                    "ra_mck_int_master_dynamic_non_tsf_month_outlet_ids",
                ],
                outputs=[
                    "ra_mck_int_features_dmp_dynamic_filtered_consecutive_zero_sales",
                    "ra_filter_seven_consecutive_zero_sales_days",
                ],
                name="ra_mck_int_features_dmp_dynamic_filtered_consecutive_zero_sales",
            ),
            node(
                func=filter_dynamic_master_month,
                inputs=[
                    "ra_mck_int_features_dmp_dynamic_filtered_consecutive_zero_sales",
                    "params:ra_master_table",
                ],
                outputs="ra_mck_int_features_dmp_dynamic_filtered_month",
                name="ra_mck_int_features_dmp_dynamic_filter_month",
            ),
            node(
                func=agg_dynamic_numerical_features_sum_mean,
                inputs=[
                    "ra_mck_int_features_dmp_dynamic_filtered_consecutive_zero_sales",
                    "params:ra_master_table",
                ],
                outputs="ra_mck_int_features_dmp_dynamic_agg_sum_mean",
                name="ra_mck_int_features_dmp_dynamic_agg_sum_mean",
            ),
            node(
                func=create_network_metric_features,
                inputs=[
                    "ra_mck_int_features_dmp_dynamic_agg_sum_mean",
                    "params:ra_master_table",
                ],
                outputs="ra_mck_int_features_dmp_dynamic_agg_network_features",
                name="ra_mck_int_features_dmp_dynamic_agg_network_features",
            ),
            node(
                func=create_lagged_features,
                inputs=[
                    "ra_mck_int_features_dmp_dynamic_agg_sum_mean",
                    "params:ra_master_table",
                ],
                outputs="ra_mck_int_features_dmp_dynamic_agg_sum_mean_with_lagged",
                name="ra_mck_int_features_dmp_dynamic_create_sum_mean_lagged_features",
            ),
            node(
                func=agg_dynamic_categorical_features_mode,
                inputs=[
                    "ra_mck_int_features_dmp_dynamic_filtered_month",
                    "params:ra_master_table",
                ],
                outputs="ra_mck_int_features_dmp_dynamic_agg_mode",
                name="ra_mck_int_features_dmp_dynamic_agg_mode",
            ),
            node(
                func=agg_dynamic_features_last,
                inputs=[
                    "ra_mck_int_features_dmp_dynamic_filtered_month",
                    "params:ra_master_table",
                ],
                outputs="ra_mck_int_features_dmp_dynamic_agg_last",
                name="ra_mck_int_features_dmp_dynamic_agg_last",
            ),
            node(
                func=agg_dynamic_numerical_features_max,
                inputs=[
                    "ra_mck_int_features_dmp_dynamic_filtered_month",
                    "params:ra_master_table",
                ],
                outputs="ra_mck_int_features_dmp_dynamic_agg_max",
                name="ra_mck_int_features_dmp_dynamic_agg_max",
            ),
            node(
                func=join_agg_dynamic_features,
                inputs=[
                    "params:ra_master_table",
                    "ra_mck_int_features_dmp_dynamic_agg_last",
                    "ra_mck_int_features_dmp_dynamic_agg_mode",
                    "ra_mck_int_features_dmp_dynamic_agg_max",
                    "ra_mck_int_features_dmp_dynamic_agg_sum_mean_with_lagged",
                    "ra_mck_int_features_dmp_dynamic_agg_network_features",
                ],
                outputs="ra_mck_int_features_dmp_dynamic_prepared",
                name="ra_mck_int_features_dmp_dynamic_join_agg",
            ),
            node(
                func=join_ds_outlets_master_table,
                inputs=[
                    "params:ra_master_table",
                    "ra_mck_int_features_dmp_dynamic_prepared",
                    "ra_mck_int_features_dmp_static_prepared",
                    "ra_mck_int_master_dynamic_non_tsf_month_outlet_ids",
                ],
                outputs=["ra_master_pandas", "ra_filter_indo_coordinates",],
                name="ra_master_joining",
            ),
        ],
    )
