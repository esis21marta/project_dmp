from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.internal_esis._ses_stimulation._2_sub_segmentation_clustering.nodes import (
    concatenate_results_of_clustering,
    concatenate_results_of_rfm_and_clustering,
    scoring_clus,
    to_hdfs,
    to_pandas,
)


def create_pipeline_scoring():
    n = 100
    nodes_all = []
    for i in range(n):
        nodes = [
            node(
                func=to_pandas,
                inputs=f"rfm_clustering_scoring_small_table_input_low{i}",
                outputs=f"ses_local_pandas_master_low_table{i}",
                name=f"PysparktoPandaslow{i}",
            ),
            node(
                func=scoring_clus,
                inputs=[
                    "params:ses_cols_to_select_log",
                    "params:ses_cols_to_drop",
                    "params:ses_segment_low",
                    f"ses_local_pandas_master_low_table{i}",
                    f"kmeans_low_score",
                ],
                outputs=f"rfm_clustering_scoring_small_table_output_low_table{i}",
                name=f"scoring_clus_low{i}",
            ),
            node(
                func=to_hdfs,
                inputs=f"rfm_clustering_scoring_small_table_output_low_table{i}",
                outputs=f"rfm_clustering_scoring_output_low{i}",
                name=f"hdfslow{i}",
            ),
            node(
                func=to_pandas,
                inputs=f"rfm_clustering_scoring_small_table_input_medium{i}",
                outputs=f"ses_local_pandas_master_medium_table{i}",
                name=f"PysparktoPandasmed{i}",
            ),
            node(
                func=scoring_clus,
                inputs=[
                    "params:ses_cols_to_select_log",
                    "params:ses_cols_to_drop",
                    "params:ses_segment_medium",
                    f"ses_local_pandas_master_medium_table{i}",
                    f"kmeans_medium_score",
                ],
                outputs=f"rfm_clustering_scoring_small_table_output_medium_table{i}",
                name=f"scoring_clus_medium{i}",
            ),
            node(
                func=to_hdfs,
                inputs=f"rfm_clustering_scoring_small_table_output_medium_table{i}",
                outputs=f"rfm_clustering_scoring_output_medium{i}",
                name=f"hdfsmedium{i}",
            ),
            node(
                func=to_pandas,
                inputs=f"rfm_clustering_scoring_small_table_input_high{i}",
                outputs=f"ses_local_pandas_master_high_table{i}",
                name=f"PysparktoPandashigh{i}",
            ),
            node(
                func=scoring_clus,
                inputs=[
                    "params:ses_cols_to_select_log",
                    "params:ses_cols_to_drop",
                    "params:ses_segment_high",
                    f"ses_local_pandas_master_high_table{i}",
                    f"kmeans_high_score",
                ],
                outputs=f"rfm_clustering_scoring_small_table_output_high_table{i}",
                name=f"scoring_clus_high{i}",
            ),
            node(
                func=to_hdfs,
                inputs=f"rfm_clustering_scoring_small_table_output_high_table{i}",
                outputs=f"rfm_clustering_scoring_output_high{i}",
                name=f"hdfshigh{i}",
            ),
        ]
        nodes_all.extend(nodes)

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
            *nodes_all,
        ]
    )

    res = base_pipeline + Pipeline(
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
            #         node(
            #             func=recharges_dd,
            #             inputs=["smy_v_rech_urp_dd","smy_v_rech_mkios_dd","params:ses_weekstart"],
            #             outputs="recharge_scoring",
            #             name="recharges_dd",
            #         ),
            #         node(
            #             func=summary_step1,
            #             inputs=["rfm_clustering_mapping_msisdn_segment_scoring",
            #                    "dca_mytsel_all",
            #                    "uln_bcpgames",
            #                    "uln_bcpvideo",
            #                    "bcpusg_appscat_apps",
            #                    "params:ses_source_table",
            #                    "params:ses_year"],
            #             outputs="rfm_clustering_mapping_msisdn_segment_scoring_sum1",
            #             name="summary_1",
            #          ),
            #         node(
            #             func=summary_step2,
            #             inputs=["rfm_clustering_mapping_msisdn_segment_scoring",
            #                     "multisim_lookalike",
            #                    "uln_digital_scaleup",
            # #                    "arif_multisim",
            #                    "params:count_table"],
            #             outputs="rfm_clustering_mapping_msisdn_segment_scoring_sum2",
            #             name="summary_2",
            #          ),
            #         node(
            #             func=summary_step3,
            #             inputs=["rfm_clustering_mapping_msisdn_segment_scoring",
            #                     "rzl_ottvoice",
            #                    "uln_bcpsocialnet",
            #                    "uln_bcpfintech",
            #                    "uln_bcpbanking"],
            #             outputs="rfm_clustering_mapping_msisdn_segment_scoring_sum3",
            #             name="summary_3",
            #          ),
            #         node(
            #             func=summary_step4,
            #             inputs=["rfm_clustering_mapping_msisdn_segment_scoring","recharge_scoring"],
            #             outputs="rfm_clustering_mapping_msisdn_segment_scoring_sum4",
            #             name="summary_4",
            #          ),
            #         node(
            #             func=summary_step5,
            #             inputs=["rfm_clustering_mapping_msisdn_segment_scoring",
            #                     "rfm_clustering_mapping_msisdn_segment_scoring_sum1",
            #                     "rfm_clustering_mapping_msisdn_segment_scoring_sum2",
            #                     "rfm_clustering_mapping_msisdn_segment_scoring_sum3",
            #                     "rfm_clustering_mapping_msisdn_segment_scoring_sum4"],
            #             outputs="rfm_clustering_mapping_msisdn_segment_scoring_sum5",
            #             name="summary_5",
            #          ),
            #         node(
            #             func=summary_step6,
            #             inputs=["rfm_clustering_mapping_msisdn_segment_scoring_sum5","nwa_sales_dnkm",
            #                     "params:ses_network_dates"],
            #             outputs="rfm_clustering_mapping_msisdn_segment_scoring_sum6",
            #             name="summary_6",
            #          ),
        ]
    )
    return res
