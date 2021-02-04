from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.feature_selection.nodes import (
    aggregate_feature_importance,
    get_feature_importance,
    top_n_features,
)


def feature_selection_pipeline():
    return Pipeline(
        [
            node(
                func=get_feature_importance,
                inputs=["params:pai_run_ids", "params:pai_runs_load"],
                outputs="importance_dfs",
                tags=["pai:feature_selection"],
            ),
            node(
                func=aggregate_feature_importance,
                inputs="importance_dfs",
                outputs="agg_importance_df",
            ),
            node(
                func=top_n_features,
                inputs=["agg_importance_df", "params:top_n"],
                outputs="top_importance_df",
                tags=["pai:feature_selection"],
            ),
        ]
    )
