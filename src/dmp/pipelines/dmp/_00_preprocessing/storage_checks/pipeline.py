from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.dmp._00_preprocessing.storage_checks.storage_checks import (
    storage_checks,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    Storage Checks Pipeline.

    Args:
        kwargs: Ignore any additional arguments added in the future.

    Returns:
        A Pipeline object containing all Storage Capacity.
    """
    return Pipeline(
        [
            node(
                func=storage_checks,
                inputs="params:email_notif_setup",
                outputs=None,
                name="dmp_storage_checks",
            ),
        ],
        tags=[
            "preprocess_scoring_pipeline",
            "scoring_pipeline",
            "dmp_storage_checks",
            "de_preprocess_pipeline",
            "de_pipeline",
        ],
    )
