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

from src.dmp.pipelines.internal_esis._4g_stimulation._0_preprocessing.nodes import (
    add_month_to_master_table,
    concatenate_lac_ci,
    downsampling_master_table,
    join_wrapper,
    select_desired_weekstart,
    select_specific_partner_from_master_table,
    subselect_desired_columns,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    Adding segments and network data to the master table

    :param kwargs: Ignore any additional arguments added in the future.

    :return: A Pipeline object
    """
    return Pipeline(
        [
            node(
                func=select_specific_partner_from_master_table,
                inputs=["dmp_master_table_all_partners", "params:4g_weekstart"],
                outputs="4g_master_table_random_samples",
                name="selecting_random_samples",
            ),
            node(
                func=add_month_to_master_table,
                inputs=["4g_master_table_random_samples"],
                outputs="4g_master_added_month",
                name="adding month (master)",
            ),
            node(
                func=select_desired_weekstart,
                inputs=[
                    "params:4g_weekstart",
                    "4g_master_added_month",
                    "dmp_lacci_level_master_table",
                ],
                outputs=["4g_master_added_month_", "4g_lacci_level_master_table_"],
                name="selecting a specific month from lacci and master tables",
            ),  # in are all tables and out are all tables from a certain month
            node(
                func=subselect_desired_columns,
                inputs=[
                    "lte_map",
                    "params:4g_lte_map_cols",
                    "cb_prepaid_postpaid",
                    "params:4g_cb_prepaid_postpaid_cols",
                    "imei_db",
                    "params:4g_imeidb_cols",
                ],
                outputs=[
                    "lte_map_selected_cols",
                    "cb_prepaid_postpaid_selected_cols",
                    "imei_db_selected_cols",
                ],
                name="subselecting columns and tables",
            ),  # same, in are all non dmp tables
            node(
                func=join_wrapper,
                inputs=["4g_master_added_month_", "lte_map_selected_cols"],
                outputs="4g_master_added_month_1",
                name="joining (lte map)",
            ),
            node(
                func=concatenate_lac_ci,
                inputs="cb_prepaid_postpaid_selected_cols",
                outputs="cb_prepaid_postpaid_selected_cols_",
                name="transforming lac ci to lacci",
            ),
            node(
                func=join_wrapper,
                inputs=[
                    "4g_master_added_month_1",
                    "cb_prepaid_postpaid_selected_cols_",
                ],
                outputs="4g_master_added_month_2",
                name="joining (cb_prepaid - for lacci)",
            ),
            node(
                func=join_wrapper,
                inputs=["4g_master_added_month_2", "imei_db_selected_cols"],
                outputs="4g_master_added_month_3",
                name="joining (imei_db)",
            ),
            node(
                func=lambda x, y: join_wrapper(
                    x, y, cols=["lac_ci", "weekstart"], how="left"
                ),
                inputs=["4g_master_added_month_3", "4g_lacci_level_master_table_"],
                outputs="4g_master_complete",
                name="joining (lacci level table)",
            ),
            node(
                func=downsampling_master_table,
                inputs=["4g_master_complete", "params:4g_downsample_fraction"],
                outputs="4g_master_complete_downsampled",
                name="downsample_master_table",
            ),
        ]
    )
