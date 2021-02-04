from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.external.reporting.nodes import send_reporting_via_email


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
                func=send_reporting_via_email,
                inputs=["params:reporting_email_setup"],
                outputs=None,
                name="send_external_reporting_via_email",
            ),
        ]
    )
