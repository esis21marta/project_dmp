from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.internal_esis._4g_stimulation._2_dim_reduction_and_clustering._1_data_cleaning import (
    cast_decimals_to_float,
    pick_a_segment_and_month,
    remove_rows_with_positive_4g_usage,
    remove_unit_of_analysis,
    specific_transformations,
    subset_features,
    to_pandas,
    transforming_columns,
)
from src.dmp.pipelines.internal_esis._4g_stimulation._2_dim_reduction_and_clustering._2_dim_reduction import (
    transform_dimension_reduction,
)
from src.dmp.pipelines.internal_esis._4g_stimulation._2_dim_reduction_and_clustering._3_clustering import (
    concatenate_results_of_clustering,
    joining_kmeans_result_and_original_table,
    kmeans_clustering_predict,
    to_hdfs,
)


def create_pipeline_scoring():
    n = 80
    nodes_all = []
    for i in range(n):
        nodes = [
            node(
                func=to_pandas,
                inputs=f"4g_clustering_scoring_small_table_input{i}",
                outputs=f"4g_local_pandas_master_table{i}",
                name=f"portion_{i}_.PysparktoPandas",
            ),
            node(
                func=remove_rows_with_positive_4g_usage,
                inputs=f"4g_local_pandas_master_table{i}",
                outputs=f"4g_local_pandas_master_table__{i}",
                name=f"portion_{i}_.remove_rows",
            ),
            node(
                func=remove_unit_of_analysis,
                inputs=[
                    f"4g_local_pandas_master_table__{i}",
                    "params:4g_clustering_unit_of_analysis",
                ],
                outputs=f"4g_local_pandas_master_table_no_unit_of_analysis{i}",
                name=f"portion_{i}_.remove_unit_of_analysis",
            ),
            node(
                func=specific_transformations,
                inputs=f"4g_local_pandas_master_table_no_unit_of_analysis{i}",
                outputs=f"4g_local_pandas_master_table_{i}",
                name=f"portion_{i}_.specific_transf",
            ),
            node(
                func=transforming_columns,
                inputs=[
                    f"4g_local_pandas_master_table_{i}",
                    "params:4g_clustering_cols_to_log",
                    "params:4g_clustering_imputation_method",
                    "params:4g_clustering_limit_manuf",
                    "params:4g_score_tag",
                ],
                outputs=f"4g_table_cleaned{i}",
                name=f"portion_{i}_.Transforming columns",
            ),
            node(
                func=transform_dimension_reduction,
                inputs=[
                    f"4g_table_cleaned{i}",
                    "4g_num_cols_score",
                    "4g_pca_score",
                    "4g_clustering_scaler_score",
                    "params:4g_score_tag",
                ],
                outputs=f"4g_table_pca_components{i}",
                name=f"portion_{i}_.Tranform_dimension",
            ),
            node(
                func=kmeans_clustering_predict,
                inputs=[f"4g_table_pca_components{i}", "4g_kmeans_trained_score"],
                outputs=f"4g_pca_table_and_clusters{i}",
                name=f"portion_{i}_.Kmeans Clustering predict",
            ),
            node(
                func=joining_kmeans_result_and_original_table,
                inputs=[
                    f"4g_pca_table_and_clusters{i}",
                    f"4g_local_pandas_master_table__{i}",
                ],
                outputs=f"4g_kmeans_result_and_original_table{i}",
                name=f"portion_{i}_.joining_kmeans_result_and_original_table",
            ),
            node(
                func=to_hdfs,
                inputs=f"4g_kmeans_result_and_original_table{i}",
                outputs=f"4g_clustering_scoring_small_table_output{i}",
                name=f"portion_{i}_.output_",
            ),
        ]
        nodes_all.extend(nodes)

    base_pipeline = Pipeline(
        [
            node(
                func=pick_a_segment_and_month,
                inputs=[
                    "4g_master_complete",
                    "params:4g_segment",
                    "params:4g_weekstart",
                ],
                outputs="4g_master_table_segment___",
                name="picking a segment (scoring)",
            ),
            node(
                func=cast_decimals_to_float,
                inputs=[
                    "4g_master_table_segment___",
                    "params:4g_score_tag",
                    "4g_cols_under_null",
                ],
                outputs="4g_master_table_casted__",
                name="casting cols with decimals to float (scoring)",
            ),
            node(
                func=subset_features,
                inputs=[
                    "4g_master_table_casted__",
                    "params:4g_clustering_features_to_select",
                ],
                outputs="4g_table_feature_subset___",
                name="Select features to use (scoring)",
            ),
            node(
                func=lambda spark_table: spark_table.randomSplit([1.0] * n),
                inputs=["4g_table_feature_subset___",],
                outputs=[
                    f"4g_clustering_scoring_small_table_input{i}" for i in range(n)
                ],
                name="Splitting Data sets (scoring)",
            ),
            *nodes_all,
        ]
    )

    res = base_pipeline + Pipeline(
        [
            node(
                func=concatenate_results_of_clustering,
                inputs=[
                    f"4g_clustering_scoring_small_table_output{i}" for i in range(n)
                ],
                outputs="4g_mapping_msisdn_segment_scoring",
                name="concatenate_results_of_clustering",
            )
        ]
    )
    return res
