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

from src.dmp.pipelines.cvm.shared.creating_features_table.features import (
    get_mondays_multidim,
    join_tables,
    prepare_multidim,
    union_tables,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    Create a pipeline for the target variable creation

    :param kwargs: Ignore any additional arguments added in the future.

    :return: A Pipeline object
    """
    return Pipeline(
        [
            node(
                func=get_mondays_multidim,
                inputs=["mck_t_cb_multidim"],
                outputs="sh_mck_int_features_multidim_mondays",
                name="mondays_multidim",
                tags="mondays",
            ),
            node(
                func=prepare_multidim,
                inputs=["sh_mck_int_features_multidim_mondays", "params:multidim"],
                outputs="sh_mck_int_features_multidim_mondays_filtered",
                name="multidim_filter",
                tags=["mondays_filtered", "features_no_mult_mon_run"],
            ),
            node(
                func=union_tables,
                inputs=[
                    "params:mytelkomsel",
                    "dbi_dc_dd_mytsel_user_201906",
                    "dbi_dc_dd_mytsel_user_201907",
                    "dbi_dc_dd_mytsel_user_201908",
                    "dbi_dc_dd_mytsel_user_201909",
                ],
                outputs="mck_int_features_mytsel_unioned",
                name="mytsel_union",
                tags=["basic_features", "features_no_mult_mon_run"],
            ),
            node(
                func=union_tables,
                inputs=[
                    "params:digital",
                    "bcp_digital_segment_201906",
                    "bcp_digital_segment_201907",
                    "bcp_digital_segment_201908",
                    "bcp_digital_segment_201909",
                ],
                outputs="mck_int_features_digital_unioned",
                name="digital_union",
                tags=["basic_features", "features_no_mult_mon_run"],
            ),
            node(
                func=union_tables,
                inputs=[
                    "params:smartphone",
                    "imei_handset_201906",
                    "imei_handset_201907",
                    "imei_handset_201908",
                    "imei_handset_201909",
                ],
                outputs="mck_int_features_smartphone_unioned",
                name="imei_union",
                tags=["basic_features", "features_no_mult_mon_run"],
            ),
            node(
                func=union_tables,
                inputs=[
                    "params:lte",
                    "lte_map_201906",
                    "lte_map_201907",
                    "lte_map_201908",
                    "lte_map_201909",
                ],
                outputs="mck_int_features_lte_unioned",
                name="lte_union",
                tags=["basic_features", "features_no_mult_mon_run"],
            ),
            node(
                func=join_tables,
                inputs=[
                    "sh_mck_int_features_multidim_mondays_filtered",
                    "mck_int_features_mytsel_unioned",
                    "mck_int_features_digital_unioned",
                    "mck_int_features_smartphone_unioned",
                    "mck_int_features_lte_unioned",
                ],
                outputs="sh_mck_int_mck_features_ds",
                name="join_ds_features",
                tags=["basic_features", "features_no_mult_mon_run"],
            ),
        ]
    )
