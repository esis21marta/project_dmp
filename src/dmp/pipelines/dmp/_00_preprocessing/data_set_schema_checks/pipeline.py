from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.dmp._00_preprocessing.data_set_schema_checks.check_data_set_schema import (
    check_data_set_schema,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    Preprocess the LOS Pipeline.

    Args:
        kwargs: Ignore any additional arguments added in the future.

    Returns:
        A Pipeline object containing all the LOS Preprocessing Nodes.
    """
    return Pipeline(
        [
            node(
                func=check_data_set_schema,
                inputs=["params:schema", "params:email_notif_setup"],
                outputs=None,
                name="check_data_set_schema",
            ),
        ],
        tags=[
            "preprocess_customer_profile",
            "preprocess_customer_los",
            "preprocess_handset",
            "preprocess_handset_internal",
            "preprocess_internet_app_usage",
            "preprocess_internet_usage",
            "preprocess_network",
            "preprocess_outlets",
            "preprocess_payu_usage",
            "preprocess_product",
            "preprocess_recharge",
            "preprocess_revenue",
            "preprocess_text_messaging",
            "preprocess_voice_calls",
            "preprocess_data_set_schema_checks",
            "de_preprocess_pipeline",
            "de_pipeline",
            # "preprocess_scoring_pipeline",
            # "scoring_pipeline",
        ],
    )
