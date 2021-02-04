from kedro.pipeline import Pipeline, node

from .fea_mytsel import fea_mytsel


def create_pipeline(**kwargs) -> Pipeline:
    """ Create the PAYU Usage Feature Pipeline.

    :return: A Pipeline object containing all the PAYU Usage Feature Nodes.
    """

    return Pipeline(
        [
            node(
                func=fea_mytsel,
                inputs=[
                    "l3_mytsel_weekly_final",
                    "params:config_feature.mytsel",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l4_fea_mytsel",
                name="fea_mytsel",
            ),
        ],
        tags=["fea_mytsel", "mytsel_pipeline", "de_feature_pipeline", "de_pipeline",],
    )
