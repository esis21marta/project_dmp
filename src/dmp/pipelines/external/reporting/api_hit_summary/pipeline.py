from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.external.reporting.api_hit_summary.nodes import (
    filter_partner_and_period,
    get_clean_response_format,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to send
    the one or more parquet files for reporting purposes with tags to help
    uniquely identify it.

    Returns:
        A Pipeline object containing all the nodes to execute reporting pipeline.
    """
    return Pipeline(
        [
            node(
                func=filter_partner_and_period,
                inputs=["api_requests", "params:partner_id"],
                outputs="filtered_api_requests",
                name="filter_partner_and_period",
            ),
            node(
                func=get_clean_response_format,
                inputs=["filtered_api_requests", "params:columns_to_include"],
                outputs="partner_api_hit_report",
                name="get_clean_response_format",
            ),
        ]
    )
