# Copyright 2018-present QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.internal_esis._4g_stimulation._2_dim_reduction_and_clustering._1_data_cleaning import (
    cast_decimals_to_float,
    count_nans,
    pick_a_segment_and_month,
    plot_histogram,
    remove_rows_with_positive_4g_usage,
    remove_unit_of_analysis,
    select_df_to_downsampled_or_train,
    specific_transformations,
    subset_features,
    to_pandas,
    transforming_columns,
)
from src.dmp.pipelines.internal_esis._4g_stimulation._2_dim_reduction_and_clustering._2_dim_reduction import (
    apply_pca,
    numerical_categorical,
    transform_dimension_reduction,
)
from src.dmp.pipelines.internal_esis._4g_stimulation._2_dim_reduction_and_clustering._3_clustering import (
    downsample,
    joining_kmeans_result_and_original_table,
    joining_with_complete_master_table,
    kmeans_clustering_fit,
    kmeans_clustering_predict,
    to_hdfs,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    Adding segments and network data to the master table

    :param kwargs: Ignore any additional arguments added in the future.

    :return: A Pipeline object
    """

    preprocessing_pipeline = Pipeline(
        [
            node(
                select_df_to_downsampled_or_train,
                inputs=[
                    "4g_master_complete_downsampled",
                    "4g_master_complete_downsampled",
                    "lte_map",
                    "params:4g_weekstart",
                    "params:4g_downsampled_or_train_tag",
                ],
                outputs="4g_master_complete_selected",
                name="start_complete",
            ),
            node(
                func=pick_a_segment_and_month,
                inputs=[
                    "4g_master_complete_selected",
                    "params:4g_segment",
                    "params:4g_weekstart",
                ],
                outputs="4g_master_table_segment",
                name="picking a segment",
            ),
            node(
                func=count_nans,
                inputs=[
                    "4g_master_table_segment",
                    "params:4g_method",
                    "params:4g_threshold",
                ],
                outputs=[
                    "4g_cols_under_null",
                    "4g_cols_under_zeros",
                    "4g_cols_under_minus1",
                ],
                name="count_missing",
            ),
            node(
                func=cast_decimals_to_float,
                inputs=[
                    "4g_master_table_segment",
                    "params:4g_train_tag",
                    "4g_cols_under_null",
                ],
                outputs="4g_master_table_casted",
                name="casting cols with decimals to float",
            ),
            node(
                func=subset_features,
                inputs=[
                    "4g_master_table_casted",
                    "params:4g_clustering_features_to_select",
                ],
                outputs="4g_table_feature_subset",
            ),
            node(
                func=specific_transformations,
                inputs="4g_table_feature_subset",
                outputs="4g_table_feature_subset_",
                name="specific_transformations",
            ),
            node(
                func=to_pandas,
                inputs="4g_table_feature_subset_",
                outputs="4g_local_pandas_master_table",
                name="PysparktoPandas",
            ),
            node(
                func=remove_rows_with_positive_4g_usage,
                inputs="4g_local_pandas_master_table",
                outputs="4g_local_pandas_master_table__",
                name="remove_positive_4g",
            ),
            node(
                func=remove_unit_of_analysis,
                inputs=[
                    "4g_local_pandas_master_table__",
                    "params:4g_clustering_unit_of_analysis",
                ],
                outputs="4g_local_pandas_master_table_no_unit_of_analysis",
                name="remove_unit_of_analysis",
            ),
            node(
                func=transforming_columns,
                inputs=[
                    "4g_local_pandas_master_table_no_unit_of_analysis",
                    "params:4g_clustering_cols_to_log",
                    "params:4g_clustering_imputation_method",
                    "params:4g_clustering_limit_manuf",
                    "params:4g_score_tag",
                ],
                outputs="4g_table_cleaned",
                name="Transforming",
            ),
            node(
                func=plot_histogram,
                inputs=["4g_table_cleaned", "params:4g_path_to_save_hist"],
                outputs=None,
                name="plot_histogram",
            ),
        ]
    )

    dim_reduction_pipeline = Pipeline(
        [
            node(
                func=numerical_categorical,
                inputs="4g_table_cleaned",
                outputs=[
                    "4g_table_numerical",
                    "4g_table_categorical",
                    "4g_num_cols_train",
                    "4g_cat_cols_train",
                ],
                name="separate_numerical_categorical",
            ),
            node(
                func=apply_pca,
                inputs=[
                    "4g_table_numerical",
                    "params:4G_dimension_reduction_parameter",
                    "params:4g_path_to_save_corr_matrix_plot_pca",
                ],
                outputs=[
                    "4g_pca_train",
                    "4g_pca_correlations",
                    "4g_pca_explained_inertia",
                    "4g_clustering_scaler_pca_train",
                ],
                name="pca_dimension_reduction",
            ),
            node(
                func=transform_dimension_reduction,
                inputs=[
                    "4g_table_cleaned",
                    "4g_num_cols_train",
                    "4g_pca_train",
                    "4g_clustering_scaler_pca_train",
                    "params:4g_train_tag",
                ],
                outputs="4g_table_pca_components",
                name="Tranform_dimension",
            ),
        ],
        tags=["4g_pca_kmeans"],
    )

    clustering_pipeline = Pipeline(
        [
            node(
                func=kmeans_clustering_fit,
                inputs=["4g_table_pca_components", "params:4G_clustering_parameter"],
                outputs="4g_kmeans_trained_pca_train",
                name="Kmeans_Clustering_fit",
            ),
            node(
                func=kmeans_clustering_predict,
                inputs=["4g_table_pca_components", "4g_kmeans_trained_pca_train"],
                outputs="4g_pca_table_and_clusters",
                name="Kmeans_Clustering_predict_pca",
                tags=["kmeans_predict"],
            ),
        ],
        tags=["4g_kmeans"],
    )

    reporting_pipeline = Pipeline(
        [
            # pca
            node(
                func=joining_kmeans_result_and_original_table,
                inputs=["4g_pca_table_and_clusters", "4g_local_pandas_master_table__"],
                outputs="4g_local_pandas_original_table_pca_components_and_kmeans_components",
                name="reporting1",
            ),
            node(
                func=to_hdfs,
                inputs="4g_local_pandas_original_table_pca_components_and_kmeans_components",
                outputs="4g_original_table_pca_components_and_kmeans_components_hdfs",
                name="reporting2",
            ),
            node(
                func=joining_with_complete_master_table,
                inputs=[
                    "4g_original_table_pca_components_and_kmeans_components_hdfs",
                    "4g_master_table_casted",
                ],
                outputs="4g_master_table_pca_components_and_kmeans_components",
                name="reporting3",
            ),
            node(
                func=downsample,
                inputs=[
                    "4g_master_table_pca_components_and_kmeans_components",
                    "params:4g_sample_size_to_compute_stats",
                ],
                outputs="4g_master_table_pca_components_and_kmeans_components_downsampled",
                name="reporting4",
            ),
            node(
                func=to_pandas,
                inputs="4g_master_table_pca_components_and_kmeans_components_downsampled",
                outputs="4g_master_table_pca_components_and_kmeans_components_downsampled_pandas",
                name="reporting5",
            ),
            #         node(
            #             func=run_statistics_profile,
            #             inputs=["4g_master_table_pca_components_and_kmeans_components_downsampled_pandas","params:4g_path_to_save_summary_pca"],
            #             outputs="4g_statistics_profile_pca",
            #             tags=["4g_reporting_pca"]
            #         ),
        ]
    )

    return (
        preprocessing_pipeline
        + dim_reduction_pipeline
        + clustering_pipeline
        + reporting_pipeline
    )
