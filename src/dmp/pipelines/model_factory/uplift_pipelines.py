"""
The model factory
-----------------

This pipeline will create a single instance of a model-factory model. It
can be used in the related model factories to build up  larger pipelines
that can be used for scoring of uplift models or sending churn campaigns
or whatever.
"""
from kedro.framework.context import KedroContext
from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.model_factory import (
    consts,
    eda_nodes,
    model_nodes,
    preprocessing_nodes,
    shared_pipeline,
)
from utils import get_config_parameters


def create_preprocessing_pipeline(**kwargs):

    entries = get_config_parameters(config="catalog")
    tables = [e for e in entries if e.startswith("master")]

    return Pipeline(
        [
            node(
                func=eda_nodes.add_pai_tag,
                inputs="params:pai_tags",
                outputs=None,
                tags=["pai:model_factory"],
            ),
            node(
                func=preprocessing_nodes.pai_log_campaign_name,
                inputs="params:pai_campaign_name",
                outputs=None,
                tags=["pai:model_factory"],
            ),
            node(
                func=preprocessing_nodes.pai_log_use_case,
                inputs="params:use_case",
                outputs=None,
                tags=["pai:model_factory"],
            ),
            node(
                func=preprocessing_nodes.select_campaign,
                inputs=["params:campaign_name_column", "params:campaign_names"]
                + tables,
                outputs="campaign_only",
            ),
            node(
                func=preprocessing_nodes.remove_duplicate_msisdn_weekstart,
                inputs=[
                    "campaign_only",
                    "params:msisdn_column",
                    "params:weekstart_column",
                ],
                outputs="no_duplicates",
            ),
            node(
                func=preprocessing_nodes.add_is_treatment_column,
                inputs=["no_duplicates", "params:is_control_column"],
                outputs="with_treatment",
            ),
            node(
                func=preprocessing_nodes.get_important_dates,
                inputs=[
                    "with_treatment",
                    "params:campaign_start_date_column",
                    "params:campaign_end_date_column",
                    "params:target_window_weeks",
                ],
                outputs=[
                    "observation_start",
                    "campaign_start_monday",
                    "observation_end",
                ],
                tags=["pai:model_factory"],
            ),
            node(
                func=preprocessing_nodes.add_target_source_col,
                inputs=["with_treatment", "params:target_source"],
                outputs="with_target_source",
            ),
            # Note - this is an EDA node,  but it also uses data that is
            # in a pyspark table.  Because we have so far kept all spark
            # in the pre-processing step,  I think it's better to have a
            # small amount of EDA in the preprocessing, rather than have
            # large amount of preprocessing in the EDA pipeline.
            node(
                func=eda_nodes.plot_target_source,
                inputs=[
                    "with_target_source",
                    "params:weekstart_column",
                    "params:is_taker_column",
                    "params:is_control_column",
                    "params:plot_labels",
                ],
                outputs=None,
                name="plot_target_source",
                tags=["pai:model_factory"],
            ),
            node(
                func=preprocessing_nodes.filter_dates,
                inputs=[
                    "with_target_source",
                    "params:weekstart_column",
                    "observation_start",
                    "observation_end",
                ],
                outputs="time_filtered_subset",
                tags=["pai:model_factory"],
            ),
            node(
                func=preprocessing_nodes.add_target,
                inputs=[
                    "time_filtered_subset",
                    "params:msisdn_column",
                    "params:use_case",
                    "campaign_start_monday",
                    "params:weekstart_column",
                ],
                outputs="with_target",
            ),
            node(
                func=preprocessing_nodes.select_training_day,
                inputs=[
                    "with_target",
                    "params:weekstart_column",
                    "campaign_start_monday",
                    "params:train_offset_weeks",
                ],
                outputs="on_training_week",
                tags=["pai:model_factory"],
            ),
            node(
                func=lambda: consts.IS_TREATMENT_COL,
                inputs=None,
                outputs="is_treatment_column",
            ),
            node(
                func=preprocessing_nodes.column_selection,
                inputs=[
                    "on_training_week",
                    "params:feature_column_prefix",
                    "params:column_selection",
                    "params:is_taker_column",
                    "params:msisdn_column",
                    "is_treatment_column",
                ],
                outputs="relevant_columns_only",
                tags=["pai:model_factory"],
            ),
            node(
                func=preprocessing_nodes.feature_selection,
                inputs=[
                    "relevant_columns_only",
                    "params:selected_features_run_id",
                    "params:is_taker_column",
                    "params:msisdn_column",
                    "is_treatment_column",
                ],
                outputs="selected_columns_only",
                tags=["pai:model_factory"],
            ),
            node(
                func=preprocessing_nodes.sample_rows_and_features,
                inputs=[
                    "selected_columns_only",
                    "params:column_selection",
                    "params:n_rows_to_sample",
                    "params:n_features_to_sample",
                    "params:feature_column_prefix",
                ],
                outputs="sampled_rows_and_features",
            ),
            node(
                func=preprocessing_nodes.convert_to_pandas,
                inputs="sampled_rows_and_features",
                outputs="pandas_df_unchecked",
            ),
            node(
                func=preprocessing_nodes.dataset_checks,
                inputs=[
                    "pandas_df_unchecked",
                    "params:msisdn_column",
                    "is_treatment_column",
                ],
                outputs="pandas_df",
            ),
        ]
    )


def create_eda_pipeline(**kwargs):
    project_context: KedroContext = kwargs["project_context"]
    shared_pipeline.config_checks(project_context)
    return Pipeline(
        [
            node(
                func=eda_nodes.plot_target,
                inputs=["pandas_df", "params:is_taker_column"],
                outputs=None,
                name="plot_target",
                tags=["pai:model_factory"],
            ),
            node(
                func=eda_nodes.count_populations,
                inputs=["pandas_df", "params:is_taker_column"],
                outputs=None,
                name="count_populations",
                tags=["pai:model_factory"],
            ),
            node(
                func=eda_nodes.target_mean_by_group,
                inputs=["pandas_df"],
                outputs=None,
                name="conversion_rates",
                tags=["pai:model_factory"],
            ),
            node(
                func=eda_nodes.learn_treatment_vs_control,
                inputs=[
                    "pandas_df",
                    "params:is_taker_column",
                    "params:msisdn_column",
                    "params:shap_treatment_vs_control",
                ],
                outputs=None,
                name="treatment_vs_control_model",
                tags=["pai:model_factory"],
            ),
            node(
                func=eda_nodes.learn_taker_vs_nontaker,
                inputs=[
                    "pandas_df",
                    "params:is_taker_column",
                    "params:msisdn_column",
                    "params:shap_taker_vs_nontaker",
                ],
                outputs=None,
                name="taker_vs_nontaker_model",
                tags=["pai:model_factory"],
            ),
        ]
    )


def create_model_pipeline(**kwargs):
    project_context: KedroContext = kwargs["project_context"]
    shared_pipeline.config_checks(project_context)

    return Pipeline(
        [
            node(
                func=model_nodes.create_uplift_model,
                inputs=[
                    "params:use_case",
                    "params:modeling_approach",
                    "params:uplift_iterations",
                ],
                outputs="uplift_model",
            ),
            node(
                func=model_nodes.drop_msisdns,
                inputs=["pandas_df", "params:msisdn_column"],
                outputs="pandas_df_no_msisdns",
            ),
            node(
                func=model_nodes.model_uplift_all_features,
                inputs=[
                    "uplift_model",
                    "pandas_df_no_msisdns",
                    "params:is_taker_column",
                    "params:uplift_folds",
                    "params:uplift_train",
                    "params:wrapper_resolution",
                    "params:n_features_to_select",
                    "params:expose_wrapped_model",
                ],
                outputs="best_features_df",
                tags=["pai:model_factory"],
            ),
            node(
                func=model_nodes.model_uplift_best_features,
                inputs=[
                    "uplift_model",
                    "best_features_df",
                    "params:is_taker_column",
                    "params:uplift_folds",
                    "params:uplift_train",
                    "params:wrapper_resolution",
                    "params:expose_wrapped_model",
                ],
                outputs=["x_data", "calibrated_model"],
                tags=["pai:model_factory"],
            ),
            node(
                func=model_nodes.log_calibrated_model_to_pai,
                inputs=["calibrated_model"],
                outputs=None,
                tags=["pai:model_factory"],
            ),
            node(
                func=model_nodes.log_shap_best_features,
                inputs=["x_data", "calibrated_model"],
                outputs=None,
                tags=["pai:model_factory"],
            ),
        ]
    )
