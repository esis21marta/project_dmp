from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.model_scoring.nodes import (
    clean_temp,
    compute_correlations,
    make_mlflow_models_from_pai_runs,
    pivot_predicted_scores,
    plot_correlation_matrix,
    return_above_threshold,
    score,
    select_highest_score_campaign,
)


def model_scoring_pipeline():
    return Pipeline(
        [
            node(
                func=make_mlflow_models_from_pai_runs,
                inputs=["params:pai_run_ids", "params:pai_campaign_name"],
                outputs="models",
                tags=["pai:no_logging"],
            ),
            node(
                func=score,
                inputs=[
                    "to_score",
                    "models",
                    "params:msisdn_col",
                    "params:campaign_name_col",
                ],
                outputs="predicted_scores",
            ),
            node(
                func=pivot_predicted_scores,
                inputs=[
                    "predicted_scores",
                    "params:msisdn_col",
                    "params:campaign_name_col",
                ],
                outputs="pivoted_predicted_scores",
            ),
            node(
                func=compute_correlations,
                inputs=["pivoted_predicted_scores", "params:msisdn_col",],
                outputs="correlation_matrix",
            ),
            node(
                func=plot_correlation_matrix,
                inputs=["correlation_matrix",],
                outputs=None,
                tags=["pai:score_correlation_matrix"],
            ),
            node(
                func=select_highest_score_campaign,
                inputs=[
                    "predicted_scores",
                    "params:msisdn_col",
                    "params:campaign_name_col",
                ],
                outputs="highest_scores",
            ),
            node(
                func=return_above_threshold,
                inputs=["highest_scores", "params:score_threshold"],
                outputs="highest_score_above_threshold",
            ),
            # predicted_scores is needed as input to control Kedro order of execution
            node(func=clean_temp, inputs=["models", "predicted_scores"], outputs=None,),
        ]
    )
