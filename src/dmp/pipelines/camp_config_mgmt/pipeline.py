from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.camp_config_mgmt.nodes import (
    extract_campaign_name_family_from_score_table,
    load_validate_config_tables,
    prepare_agg_input,
)


def camp_config_integration_pipeline(**kwargs) -> Pipeline:
    """
    Combine score and configuration tables to form aggregator's input

    :param kwargs: Ignore any additional arguments added in the future.

    :return: A Pipeline object
    """
    return Pipeline(
        [
            node(
                func=extract_campaign_name_family_from_score_table,
                inputs=["highest_score_above_threshold"],
                outputs=["campaign_name_family", "score",],
                name="Camp_Config_Integration_Load_Score_Node",
            ),
            node(
                func=load_validate_config_tables,
                inputs=["campaign_name_family"],
                outputs=[
                    "prioritization_rank",
                    "prioritization_criteria",
                    "campaign_config",
                ],
                name="Camp_Config_Integration_Load_Validate_Node",
            ),
            node(
                func=prepare_agg_input,
                inputs=[
                    "score",
                    "prioritization_rank",
                    "prioritization_criteria",
                    "campaign_config",
                    "params:output_path",
                ],
                outputs=None,
                name="Camp_Config_Integration_Prepare_Agg_Input_Node",
            ),
        ],
        tags=["camp_config_integration"],
    )
