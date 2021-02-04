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

from kedro.pipeline import Pipeline, node, pipeline

from src.dmp.pipelines.cvm.ses_stimulation._2_sub_segmentation_clustering._1_data_cleaning import (
    concatenate_results_of_clustering,
    concatenate_results_of_rfm_and_clustering,
    recharges_dd,
    scoring_clus,
    summary_step1,
    summary_step2,
    summary_step3,
    summary_step4,
    summary_step5,
    summary_step6,
    to_hdfs,
    to_pandas1,
)


def create_pipeline_scoring():
    n = 100
    base_pipeline = Pipeline(
        [
            node(
                func=lambda spark_table: spark_table.randomSplit([1.0] * n),
                inputs=["rfm_bde_ses_segmentation_selected_low",],
                outputs=[
                    f"rfm_clustering_scoring_small_table_input_low{i}" for i in range(n)
                ],
                name="Splitting_rfm_Data_sets_scoring_low",
            ),
            node(
                func=lambda spark_table: spark_table.randomSplit([1.0] * n),
                inputs=["rfm_bde_ses_segmentation_selected_medium",],
                outputs=[
                    f"rfm_clustering_scoring_small_table_input_medium{i}"
                    for i in range(n)
                ],
                name="Splitting_rfm_Data_sets_scoring_medium",
            ),
            node(
                func=lambda spark_table: spark_table.randomSplit([1.0] * n),
                inputs=["rfm_bde_ses_segmentation_selected_high",],
                outputs=[
                    f"rfm_clustering_scoring_small_table_input_high{i}"
                    for i in range(n)
                ],
                name="Splitting_rfm_Data_sets_scoring_high",
            ),
        ]
    )
    main_scoring_pipeline = Pipeline(
        [
            node(
                func=to_pandas1,
                inputs="input_low",
                outputs=f"ses_local_pandas_master_low_table",
                # stored for recovering MSISDN and assigning them to clusters
                name=f"PysparktoPandaslow",
            ),
            node(
                func=to_pandas1,
                inputs="input_medium",
                outputs=f"ses_local_pandas_master_medium_table",
                # stored for recovering MSISDN and assigning them to clusters
                name=f"PysparktoPandasmed",
            ),
            node(
                func=to_pandas1,
                inputs="input_high",
                outputs=f"ses_local_pandas_master_high_table",
                # stored for recovering MSISDN and assigning them to clusters
                name=f"PysparktoPandashigh",
            ),
            node(
                func=scoring_clus,
                inputs=[
                    "params:ses_cols_to_select_log",
                    "params:ses_cols_to_drop",
                    f"ses_local_pandas_master_low_table",
                    f"ses_local_pandas_master_medium_table",
                    f"ses_local_pandas_master_high_table",
                    f"ses_kmeans_trained_low",
                    f"ses_kmeans_trained_medium",
                    f"ses_kmeans_trained_high",
                ],
                outputs=[f"output_rfm_low", f"output_rfm_medium", f"output_rfm_high"],
                name=f"scoringclus",
            ),
            node(
                func=to_hdfs,
                inputs=f"output_rfm_low",
                outputs=f"output_cluster_low",
                name=f"hdfslow",
            ),
            node(
                func=to_hdfs,
                inputs=f"output_rfm_medium",
                outputs=f"output_cluster_medium",
                name=f"hdfsmedium",
            ),
            node(
                func=to_hdfs,
                inputs=f"output_rfm_high",
                outputs=f"output_cluster_high",
                name=f"hdfshigh",
            ),
        ]
    )

    res = base_pipeline
    for i in range(n):
        res = res + pipeline(
            main_scoring_pipeline,
            inputs={
                "input_low": f"rfm_clustering_scoring_small_table_input_low{i}",
                "input_medium": f"rfm_clustering_scoring_small_table_input_medium{i}",
                "input_high": f"rfm_clustering_scoring_small_table_input_high{i}",
            },
            outputs={
                "output_rfm_low": f"rfm_clustering_scoring_small_table_output_low_table{i}",
                "output_rfm_medium": f"rfm_clustering_scoring_small_table_output_medium_table{i}",
                "output_rfm_high": f"rfm_clustering_scoring_small_table_output_high_table{i}",
                "output_cluster_low": f"rfm_clustering_scoring_output_low{i}",
                "output_cluster_medium": f"rfm_clustering_scoring_output_medium{i}",
                "output_cluster_high": f"rfm_clustering_scoring_output_high{i}",
            },
            parameters={
                "ses_local_pandas_master_low_table": f"ses_local_pandas_master_low_table{i}",
                "ses_local_pandas_master_medium_table": f"ses_local_pandas_master_medium_table{i}",
                "ses_local_pandas_master_high_table": f"ses_local_pandas_master_high_table{i}",
                "params:ses_cols_to_select_log": "params:ses_cols_to_select_log",
                "params:ses_cols_to_drop": "params:ses_cols_to_drop",
                "ses_kmeans_trained_low": "kmeans_low",
                "ses_kmeans_trained_medium": "kmeans_medium",
                "ses_kmeans_trained_high": "kmeans_high",
            },
            namespace=f"portion_{i}_",
        )

    res = res + Pipeline(
        [
            node(
                func=concatenate_results_of_clustering,
                inputs=[f"rfm_clustering_scoring_output_low{i}" for i in range(n)],
                outputs="rfm_mapping_msisdn_segment_scoring_low",
                name="concatenate_results_of_clustering_low",
            ),
            node(
                func=concatenate_results_of_clustering,
                inputs=[f"rfm_clustering_scoring_output_medium{i}" for i in range(n)],
                outputs="rfm_mapping_msisdn_segment_scoring_medium",
                name="concatenate_results_of_clustering_medium",
            ),
            node(
                func=concatenate_results_of_clustering,
                inputs=[f"rfm_clustering_scoring_output_high{i}" for i in range(n)],
                outputs="rfm_mapping_msisdn_segment_scoring_high",
                name="concatenate_results_of_clustering_high",
            ),
            node(
                func=concatenate_results_of_rfm_and_clustering,
                inputs=[
                    "rfm_mapping_msisdn_segment_scoring_low",
                    "rfm_mapping_msisdn_segment_scoring_medium",
                    "rfm_mapping_msisdn_segment_scoring_high",
                ],
                outputs="rfm_clustering_mapping_msisdn_segment_scoring",
                name="concatenate_results_of_rfm_and_clustering",
            ),
            node(
                func=recharges_dd,
                inputs=["smy_v_rech_urp_dd", "smy_v_rech_mkios_dd"],
                outputs="recharge_scoring",
                name="recharges_dd",
            ),
            node(
                func=summary_step1,
                inputs=[
                    "rfm_clustering_mapping_msisdn_segment_scoring",
                    "cb_prepaid_postpaid",
                    "dca_mytsel_all",
                    "uln_bcpgames",
                    "uln_bcpvideo",
                    "bcpusg_appscat_apps",
                    "rfm_bde_abt_rfm",
                ],
                outputs="rfm_clustering_mapping_msisdn_segment_scoring_sum1",
                name="summary_1",
            ),
            node(
                func=summary_step2,
                inputs=[
                    "rfm_clustering_mapping_msisdn_segment_scoring",
                    "multisim_lookalike",
                    "arif_multisim",
                    "uln_digital_scaleup",
                ],
                outputs="rfm_clustering_mapping_msisdn_segment_scoring_sum2",
                name="summary_2",
            ),
            node(
                func=summary_step3,
                inputs=[
                    "rfm_clustering_mapping_msisdn_segment_scoring",
                    "rzl_ottvoice",
                    "uln_bcpsocialnet",
                    "uln_bcpfintech",
                    "uln_bcpbanking",
                ],
                outputs="rfm_clustering_mapping_msisdn_segment_scoring_sum3",
                name="summary_3",
            ),
            node(
                func=summary_step4,
                inputs=[
                    "rfm_clustering_mapping_msisdn_segment_scoring",
                    "recharge_scoring",
                ],
                outputs="rfm_clustering_mapping_msisdn_segment_scoring_sum4",
                name="summary_4",
            ),
            node(
                func=summary_step5,
                inputs=[
                    "rfm_clustering_mapping_msisdn_segment_scoring",
                    "rfm_clustering_mapping_msisdn_segment_scoring_sum1",
                    "rfm_clustering_mapping_msisdn_segment_scoring_sum2",
                    "rfm_clustering_mapping_msisdn_segment_scoring_sum3",
                    "rfm_clustering_mapping_msisdn_segment_scoring_sum4",
                ],
                outputs="rfm_clustering_mapping_msisdn_segment_scoring_sum5",
                name="summary_5",
            ),
            node(
                func=summary_step6,
                inputs=[
                    "rfm_clustering_mapping_msisdn_segment_scoring_sum5",
                    "nwa_sales_dnkm",
                ],
                outputs="rfm_clustering_mapping_msisdn_segment_scoring_sum6",
                name="summary_6",
            ),
        ]
    )
    return res
