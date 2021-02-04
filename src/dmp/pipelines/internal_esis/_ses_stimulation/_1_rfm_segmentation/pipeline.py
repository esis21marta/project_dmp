from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.internal_esis._ses_stimulation._1_rfm_segmentation.nodes import (
    check_consistency,
    fitae_baseline_summary,
    join_rfm_dmp,
    kuncie_baseline_summary,
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
                inputs=["rfm_bde_abt_rfm", "params:ses_train_score"],
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
                inputs=["rfm_bde_ses_segmentation", "params:ses_cols_rfm"],
                outputs="rfm_bde_ses_segmentation_selected",
                name="selected_cols",
            ),
            node(
                func=kuncie_baseline_summary,
                inputs=[
                    "rfm_bde_ses_segmentation_selected",
                    "rfm_cb_prepaid_postpaid_past1m",
                    "rfm_v_bcpusg_appscat_apps_comp",
                    "rfm_uln_bcpall",
                    "params:ses_year",
                ],
                outputs=[
                    "rfm_baseline_kuncie",
                    "rfm_summary_kuncie_v1",
                    "rfm_summary_kuncie_v2",
                ],
                name="kuncie_summary",
            ),
            node(
                func=fitae_baseline_summary,
                inputs=[
                    "rfm_bde_ses_segmentation_selected",
                    "rfm_cb_prepaid_postpaid_past1m",
                    "rfm_v_bcpusg_appscat_apps_comp",
                    "rfm_uln_bcpall",
                    "params:ses_year",
                ],
                outputs=[
                    "rfm_baseline_fitae",
                    "rfm_summary_fitae_v1",
                    "rfm_summary_fitae_v2",
                ],
                name="fitae_summary",
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
                    "params:ses_cols_rfm",
                    "params:ses_train_score",
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
                    "params:ses_cols_rfm",
                    "params:ses_train_score",
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
                    "params:ses_cols_rfm",
                    "params:ses_train_score",
                ],
                outputs="rfm_bde_ses_segmentation_selected_high",
                name="join_rfm_dmp_high",
            ),
            node(
                func=check_consistency,
                inputs="rfm_bde_rfm_score",
                outputs=None,
                name="consistency",
            ),
        ]
    )
