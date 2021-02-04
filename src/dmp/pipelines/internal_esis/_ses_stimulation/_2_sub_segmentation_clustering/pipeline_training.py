from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.internal_esis._ses_stimulation._2_sub_segmentation_clustering.nodes import (
    kmeans_clustering_fit,
    select_cols_topandas,
    silhoutte_score_result,
)


def create_pipeline_training(**kwargs) -> Pipeline:

    preprocessing_pipeline = Pipeline(
        [
            node(
                func=select_cols_topandas,
                inputs=[
                    "rfm_bde_ses_segmentation_selected_low",
                    "params:ses_cols_to_select_log",
                ],
                outputs=[
                    "rfm_bde_ses_segmentation_selected_low_downsampled_pandas",
                    "ses_low_num_cols_train",
                ],
                name="select_features_pandas_low",
            ),
            node(
                func=select_cols_topandas,
                inputs=[
                    "rfm_bde_ses_segmentation_selected_medium",
                    "params:ses_cols_to_select_log",
                ],
                outputs=[
                    "rfm_bde_ses_segmentation_selected_medium_downsampled_pandas",
                    "ses_medium_num_cols_train",
                ],
                name="select_features_pandas_medium",
            ),
            node(
                func=select_cols_topandas,
                inputs=[
                    "rfm_bde_ses_segmentation_selected_high",
                    "params:ses_cols_to_select_log",
                ],
                outputs=[
                    "rfm_bde_ses_segmentation_selected_high_downsampled_pandas",
                    "ses_high_num_cols_train",
                ],
                name="select_features_pandas_high",
            ),
        ]
    )

    clustering_pipeline = Pipeline(
        [
            node(
                func=silhoutte_score_result,
                inputs=[
                    "rfm_bde_ses_segmentation_selected_low_downsampled_pandas",
                    "params:ses_low_path_to_save_hist",
                    "params:ses_segment_low",
                ],
                outputs=None,
                name="silhoute_analysis_low",
            ),
            node(
                func=kmeans_clustering_fit,
                inputs=[
                    "rfm_bde_ses_segmentation_selected_low_downsampled_pandas",
                    "params:ses_clustering_parameter_low",
                    "params:ses_cols_to_select_log",
                ],
                outputs="kmeans_low_train",
                name="Kmeans_Clustering_fit_low",
            ),
            node(
                func=silhoutte_score_result,
                inputs=[
                    "rfm_bde_ses_segmentation_selected_medium_downsampled_pandas",
                    "params:ses_medium_path_to_save_hist",
                    "params:ses_segment_medium",
                ],
                outputs=None,
                name="silhoute_analysis_medium",
            ),
            node(
                func=kmeans_clustering_fit,
                inputs=[
                    "rfm_bde_ses_segmentation_selected_medium_downsampled_pandas",
                    "params:ses_clustering_parameter_medium",
                    "params:ses_cols_to_select_log",
                ],
                outputs="kmeans_medium_train",
                name="Kmeans_Clustering_fit_medium",
            ),
            node(
                func=silhoutte_score_result,
                inputs=[
                    "rfm_bde_ses_segmentation_selected_high_downsampled_pandas",
                    "params:ses_high_path_to_save_hist",
                    "params:ses_segment_high",
                ],
                outputs=None,
                name="silhoute_analysis_high",
            ),
            node(
                func=kmeans_clustering_fit,
                inputs=[
                    "rfm_bde_ses_segmentation_selected_high_downsampled_pandas",
                    "params:ses_clustering_parameter_high",
                    "params:ses_cols_to_select_log",
                ],
                outputs="kmeans_high_train",
                name="Kmeans_Clustering_fit_high",
            ),
        ]
    )

    return preprocessing_pipeline + clustering_pipeline
