from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.aggregator.nodes import (
    aggregator_event_creation,
    aggregator_event_validation,
    aggregator_input_validation,
    aggregator_inputs,
    aggregator_rank,
    aggregator_split_and_save_ranked_events,
    aggregator_whitelist,
)


def aggregator_pipeline(**kwargs) -> Pipeline:
    """
    Aggregator pipeline

    :param kwargs: Ignore any additional arguments added in the future.

    :return: A Pipeline object
    """
    return Pipeline(
        [
            node(
                func=aggregator_inputs,
                inputs=[
                    "params:input_list_columns",
                    "params:top_rank",
                    "params:limit_push_channel",
                ],
                outputs=["aggregated_dataset", "file_records", "run_id"],
                name="Aggregator_Input_Node",
                tags=["pes", "cms"],
            ),
            node(
                func=aggregator_input_validation,
                inputs=[
                    "aggregated_dataset",
                    "file_records",
                    "run_id",
                    "params:list_push_channel",
                ],
                outputs=[
                    "input_validated",
                    "input_rejected",
                    "total_unique_msisdn_clean",
                ],
                name="Aggregator_Validation_Node",
                tags=["pes", "cms"],
            ),
            node(
                func=aggregator_rank,
                inputs=["input_validated", "file_records", "run_id", "params:top_rank"],
                outputs="ranked",
                name="Aggregator_Rank_Node",
                tags=["pes", "cms"],
            ),
            node(
                func=aggregator_whitelist,
                inputs=[
                    "ranked",
                    "params:pes_whitelist_path",
                    "file_records",
                    "run_id",
                    "params:domains_to_end_at_whitelist",
                    "total_unique_msisdn_clean",
                ],
                outputs=None,
                name="Aggregator_WhiteList_PES_Node",
                tags=["pes"],
            ),
            node(
                func=aggregator_whitelist,
                inputs=[
                    "ranked",
                    "params:cms_whitelist_path",
                    "file_records",
                    "run_id",
                    "params:domains_to_end_at_whitelist",
                    "total_unique_msisdn_clean",
                ],
                outputs=None,
                name="Aggregator_WhiteList_CMS_Node",
                tags=["cms"],
            ),
            node(
                func=aggregator_event_creation,
                inputs=["ranked", "file_records", "run_id"],
                outputs="event_created",
                name="Aggregator_Event_Creation_Node",
                tags=["pes"],
            ),
            node(
                func=aggregator_event_validation,
                inputs=["event_created", "file_records", "run_id"],
                outputs=["event_validated", "event_rejected"],
                name="Aggregator_Event_Validation_Node",
                tags=["pes"],
            ),
            node(
                func=aggregator_split_and_save_ranked_events,
                inputs=[
                    "event_validated",
                    "run_id",
                    "file_records",
                    "params:pes_eventfile_path",
                    "params:top_rank",
                    "params:limit_push_channel",
                ],
                outputs=None,
                name="Aggregator_Split_Save_Ranked_Events_Node",
                tags=["pes"],
            ),
        ],
        tags=["aggregator"],
    )
