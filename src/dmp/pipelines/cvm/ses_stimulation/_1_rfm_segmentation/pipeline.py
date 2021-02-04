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

from src.dmp.pipelines.cvm.ses_stimulation._1_rfm_segmentation.nodes import (
    downsampling_master_table,
    join_rfm_dmp,
    rfm_step_1,
    rfm_step_2,
    rfm_step_3,
    rfm_step_4,
    rfm_step_5,
    rfm_step_6,
    rfm_step_7,
    selected_columns,
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
                func=rfm_step_1,
                inputs=["rfm_imei_handset", "rfm_imei_handset_reff"],
                outputs="rfm_pk_imei_handset_reff",
                name="rfm_step1",
            ),
            node(
                func=rfm_step_2,
                inputs=["rfm_pk_imei_handset_reff"],
                outputs="rfm_pk_imei_handset_reff_v2",
                name="rfm_step2",
            ),
            node(
                func=rfm_step_3,
                inputs=[
                    "rfm_cb_prepaid_postpaid_past1m",
                    "rfm_cb_prepaid_postpaid_past2m",
                    "rfm_cb_prepaid_postpaid_past3m",
                ],
                outputs="rfm_bde_baseline_rfm_ver1",
                name="rfm_step3",
            ),
            node(
                func=rfm_step_4,
                inputs=["rfm_bde_baseline_rfm_ver1",],
                outputs="rfm_bde_baseline_rfm_ver2",
                name="rfm_step4",
            ),
            node(
                func=rfm_step_5,
                inputs=["rfm_bde_baseline_rfm_ver2", "rfm_pk_imei_handset_reff_v2",],
                outputs="rfm_bde_abt_rfm",
                name="rfm_step5",
            ),
            node(
                func=rfm_step_6,
                inputs=["rfm_bde_abt_rfm"],
                outputs="rfm_bde_rfm_score",
                name="rfm_step6",
            ),
            node(
                func=rfm_step_7,
                inputs=["rfm_bde_rfm_score"],
                outputs="rfm_bde_ses_segmentation",
                name="rfm_step7",
            ),
            node(
                func=selected_columns,
                inputs=["rfm_bde_ses_segmentation",],
                outputs="rfm_bde_ses_segmentation_selected",
                name="selected_cols",
            ),
            node(
                func=join_rfm_dmp,
                inputs=[
                    "rfm_bde_ses_segmentation_selected",
                    "dmp_master_table_all_partners",
                    "dmp_aggregation",
                    "params:agg_cols_to_select",
                    "params:ses_cols_to_select",
                    "params:ses_segment_low",
                    "params:ses_weekstart",
                ],
                outputs="rfm_bde_ses_segmentation_selected_low",
                name="join_rfm_dmp_low",
            ),
            node(
                func=join_rfm_dmp,
                inputs=[
                    "rfm_bde_ses_segmentation_selected",
                    "dmp_master_table_all_partners",
                    "dmp_aggregation",
                    "params:agg_cols_to_select",
                    "params:ses_cols_to_select",
                    "params:ses_segment_medium",
                    "params:ses_weekstart",
                ],
                outputs="rfm_bde_ses_segmentation_selected_medium",
                name="join_rfm_dmp_medium",
            ),
            node(
                func=join_rfm_dmp,
                inputs=[
                    "rfm_bde_ses_segmentation_selected",
                    "dmp_master_table_all_partners",
                    "dmp_aggregation",
                    "params:agg_cols_to_select",
                    "params:ses_cols_to_select",
                    "params:ses_segment_high",
                    "params:ses_weekstart",
                ],
                outputs="rfm_bde_ses_segmentation_selected_high",
                name="join_rfm_dmp_high",
            ),
            node(
                func=downsampling_master_table,
                inputs=[
                    "rfm_bde_ses_segmentation_selected_low",
                    "params:ses_downsample_fraction",
                ],
                outputs="rfm_bde_ses_segmentation_selected_low_downsampled",
                name="downsample_master_table_low",
            ),
            node(
                func=downsampling_master_table,
                inputs=[
                    "rfm_bde_ses_segmentation_selected_medium",
                    "params:ses_downsample_fraction",
                ],
                outputs="rfm_bde_ses_segmentation_selected_medium_downsampled",
                name="downsample_master_table_medium",
            ),
            node(
                func=downsampling_master_table,
                inputs=[
                    "rfm_bde_ses_segmentation_selected_high",
                    "params:ses_downsample_fraction",
                ],
                outputs="rfm_bde_ses_segmentation_selected_high_downsampled",
                name="downsample_master_table_high",
            ),
        ]
    )
