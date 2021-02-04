"""
The model factory
-----------------

This pipeline will create a single instance of a model-factory model. It
can be used in the related model factories to build up  larger pipelines
that can be used for scoring of uplift models or sending churn campaigns
or whatever.

To run a single model, use this command:


Propensity pipeline has following structure:
- preprocessing pipeline - everything related to data preparation
 - clean the data
 - featurize - add needed features, including target column
 - limit the dataset - run column, row selection and sampling
- eda pipeline - plot different statistics on curated data sets
- modelling pipeline - model are here
"""

from kedro.framework.context import KedroContext
from kedro.pipeline import Pipeline, node
from pyspark import sql

from src.dmp.pipelines.model_factory import (
    eda_nodes,
    preprocessing_nodes,
    propensity_model_nodes,
)
from src.dmp.pipelines.model_factory.shared_pipeline import config_checks


def create_preprocessing_pipeline(**kwargs):
    """
    Preprocessing pipeline - execute everything related to data preparation
     - clean the data
     - featurize - add needed features, including target column
     - limit the dataset - run column, row selection and sampling

    In the end everything is converted to pandas and checked.

    :return: kedro Pipeline
    """
    project_context: KedroContext = kwargs["project_context"]
    config_checks(project_context)

    def topd(frame: sql.DataFrame):
        return frame.limit(100000).toPandas()

    return Pipeline(
        [
            # Cleaning nodes
            node(
                func=preprocessing_nodes.remove_duplicate_msisdn_weekstart,
                inputs=["master", "params:msisdn_column", "params:weekstart_column"],
                outputs="cleaned",
                tags=["pai:model_factory"],
            ),
            # Featurize nodes
            node(
                func=preprocessing_nodes.add_target_source_col,
                inputs=["cleaned", "params:target_source"],
                outputs="with_target_source",
            ),
            node(
                func=preprocessing_nodes.add_churn_propensity_target,
                inputs=[
                    "with_target_source",
                    "params:weekstart_column",
                    "params:msisdn_column",
                    "params:target_window_weeks",
                    "params:train_offset_weeks",
                ],
                outputs="features_with_target",
                tags=["pai:model_factory"],
            ),
            # Limit the dataset nodes
            node(
                func=preprocessing_nodes.drop_kartuhalo,
                inputs=["features_with_target"],
                outputs="features_with_target_no_kartuhalo",
                tags=["pai:model_factory"],
            ),
            node(
                func=preprocessing_nodes.apply_generic_filter,
                inputs=["features_with_target", "params:extra_filter"],
                outputs="features_with_target_filtered",
                tags=["pai:model_factory"],
            ),
            node(
                func=preprocessing_nodes.filter_dates,
                inputs=[
                    "features_with_target_filtered",
                    "params:weekstart_column",
                    "params:training_date",
                    "params:training_date",
                ],
                outputs="on_training_week",
                tags=["pai:model_factory"],
            ),
            node(
                func=preprocessing_nodes.column_selection,
                inputs=[
                    "on_training_week",
                    "params:feature_column_prefix",
                    "params:column_selection",
                    "params:msisdn_column",
                ],
                outputs="relevant_columns_only",
                tags=["pai:model_factory"],
            ),
            node(
                func=preprocessing_nodes.sample_rows_and_features,
                inputs=[
                    "relevant_columns_only",
                    "params:column_selection",
                    "params:n_rows_to_sample",
                    "params:n_features_to_sample",
                    "params:feature_column_prefix",
                ],
                outputs="sampled_rows_and_features",
                tags=["pai:model_factory"],
            ),
            node(
                func=topd,
                inputs=["sampled_rows_and_features"],
                outputs="pd_features_with_target",
                tags=["pai:model_factory"],
            ),
            # Convert and check nodes
            node(
                func=preprocessing_nodes.convert_to_pandas,
                inputs="sampled_rows_and_features",
                outputs="pandas_df_unchecked",
                tags=["pai:model_factory"],
            ),
            node(
                func=preprocessing_nodes.dataset_checks,
                inputs=["pandas_df_unchecked", "params:msisdn_column"],
                outputs="pandas_df",
                tags=["pai:model_factory"],
            ),
        ]
    )


def create_eda_pipeline(**kwargs):
    project_context: KedroContext = kwargs["project_context"]
    config_checks(project_context)
    return Pipeline(
        [
            node(
                func=eda_nodes.count_targets,
                inputs=["pandas_df", "params:msisdn_column"],
                outputs=None,
                tags=["pai:model_factory"],
            ),
            node(
                func=eda_nodes.plot_histogram_for_churn_split,
                inputs=["pandas_df", "params:plot_churn_comparison_features"],
                outputs=None,
                tags=["pai:model_factory"],
            ),
        ]
    )


def create_model_pipeline(**kwargs):
    project_context: KedroContext = kwargs["project_context"]
    config_checks(project_context)
    return Pipeline(
        [
            node(
                func=propensity_model_nodes.train_test_split,
                inputs=["pandas_df", "params:propensity_training_ratio"],
                outputs=["propensity_training", "propensity_test"],
                tags=["pai:model_factory"],
            ),
            node(
                func=propensity_model_nodes.preprocess_training_data,
                inputs=[
                    "propensity_training",
                    "params:propensity_preprocessing_blacklisted_columns",
                ],
                outputs=[
                    "propensity_training_preprocessed",
                    "target_training",
                    "propensity_preprocessing_pipeline",
                    "features_used",
                ],
                tags=["pai:model_factory"],
            ),
            node(
                func=propensity_model_nodes.preprocess_scoring_data,
                inputs=["propensity_test", "propensity_preprocessing_pipeline"],
                outputs=["propensity_test_preprocessed", "target_test"],
                tags=["pai:model_factory"],
            ),
            node(
                func=propensity_model_nodes.train_catboost_model,
                inputs=["propensity_training_preprocessed", "target_training"],
                outputs="model_fitted",
                tags=["pai:model_factory"],
            ),
            node(
                func=propensity_model_nodes.score_catboost_model,
                inputs=["propensity_test_preprocessed", "model_fitted"],
                outputs="prediction_scores",
                tags=["pai:model_factory"],
            ),
            node(
                func=propensity_model_nodes.validate_model,
                inputs=["target_test", "prediction_scores"],
                outputs=None,
                tags=["pai:model_factory"],
            ),
            node(
                func=propensity_model_nodes.save_feature_importance_catboost,
                inputs=["model_fitted", "features_used"],
                outputs=None,
                tags=["pai:model_factory"],
            ),
            node(
                func=propensity_model_nodes.create_shap_summary_plot,
                inputs=[
                    "model_fitted",
                    "propensity_training_preprocessed",
                    "features_used",
                ],
                outputs=None,
                tags=["pai:model_factory"],
            ),
        ]
    )


def create_scoring_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=preprocessing_nodes.remove_duplicate_msisdn_weekstart,
                inputs=[
                    "scoring_master",
                    "params:msisdn_column",
                    "params:weekstart_column",
                ],
                outputs="cleaned",
                tags=["pai:model_factory"],
            ),
            node(
                func=preprocessing_nodes.drop_kartuhalo,
                inputs=["cleaned"],
                outputs="features_with_target_no_kartuhalo",
                tags=["pai:model_factory"],
            ),
            node(
                func=preprocessing_nodes.filter_dates,
                inputs=[
                    "features_with_target_no_kartuhalo",
                    "params:weekstart_column",
                    "params:scoring_date",
                    "params:scoring_date",
                ],
                outputs="on_scoring_week",
                tags=["pai:model_factory"],
            ),
            node(
                func=propensity_model_nodes.pick_features_from_pai,
                inputs=[
                    "on_scoring_week",
                    "params:pai_scoring_run_id",
                    "params:msisdn_column",
                    "params:weekstart_column",
                ],
                outputs="columns_filtered",
                tags=["pai:model_factory"],
            ),
            node(
                func=preprocessing_nodes.convert_to_pandas,
                inputs="columns_filtered",
                outputs="pandas_df",
                tags=["pai:model_factory"],
            ),
            node(
                func=lambda df_score, run_id: propensity_model_nodes.preprocess_scoring_data(
                    df_score, None, run_id
                ),
                inputs=["pandas_df", "params:pai_scoring_run_id"],
                outputs=["scoring_preprocessed", "scoring_target"],
                tags=["pai:model_factory"],
            ),
            node(
                func=lambda X_score, run_id: propensity_model_nodes.score_catboost_model(
                    X_score, None, run_id
                ),
                inputs=["scoring_preprocessed", "params:pai_scoring_run_id"],
                outputs="prediction_scores",
                tags=["pai:model_factory"],
            ),
            node(
                func=propensity_model_nodes.join_scores_with_msisdns,
                inputs=["pandas_df", "prediction_scores", "params:msisdn_column"],
                outputs="msisdn_scores",
                tags=["pai:model_factory"],
            ),
        ]
    )
