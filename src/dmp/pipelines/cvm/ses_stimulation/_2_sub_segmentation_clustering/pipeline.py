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

from src.dmp.pipelines.cvm.ses_stimulation._2_sub_segmentation_clustering._1_data_cleaning import (
    kmeans_clustering_fit,
    kmeans_clustering_predict,
    select_df_to_score_or_train,
    to_pandas,
)


def create_pipeline(**kwargs) -> Pipeline:

    preprocessing_pipeline = Pipeline(
        [
            node(
                select_df_to_score_or_train,
                inputs=[
                    "rfm_bde_ses_segmentation_selected_low",
                    "rfm_bde_ses_segmentation_selected_low_downsampled",
                    "params:ses_score_or_train_tag",
                ],
                outputs="ses_master_complete_selected_low",
            ),
            node(
                select_df_to_score_or_train,
                inputs=[
                    "rfm_bde_ses_segmentation_selected_medium",
                    "rfm_bde_ses_segmentation_selected_medium_downsampled",
                    "params:ses_score_or_train_tag",
                ],
                outputs="ses_master_complete_selected_medium",
            ),
            node(
                select_df_to_score_or_train,
                inputs=[
                    "rfm_bde_ses_segmentation_selected_high",
                    "rfm_bde_ses_segmentation_selected_high_downsampled",
                    "params:ses_score_or_train_tag",
                ],
                outputs="ses_master_complete_selected_high",
            ),
            node(
                func=to_pandas,
                inputs=[
                    "ses_master_complete_selected_low",
                    "ses_master_complete_selected_medium",
                    "ses_master_complete_selected_high",
                ],
                outputs=[
                    "ses_local_pandas_master_table_low",
                    "ses_local_pandas_master_table_medium",
                    "ses_local_pandas_master_table_high",
                ],
                # stored for recovering MSISDN and assigning them to clusters
                name=f"PysparktoPandas",
            ),
        ]
    )

    clustering_pipeline = Pipeline(
        [
            node(
                func=kmeans_clustering_fit,
                inputs=[
                    "params:ses_clustering_parameter",
                    "params:ses_cols_to_select_log",
                    "ses_local_pandas_master_table_low",
                    "ses_local_pandas_master_table_medium",
                    "ses_local_pandas_master_table_high",
                    "params:ses_max_rows",
                ],
                outputs=["kmeans_low", "kmeans_medium", "kmeans_high",],
                name="Kmeans_Clustering_fit",
            ),
            node(
                func=kmeans_clustering_predict,
                inputs=[
                    "ses_local_pandas_master_table_low",
                    "kmeans_low",
                    "ses_local_pandas_master_table_medium",
                    "kmeans_medium",
                    "ses_local_pandas_master_table_high",
                    "kmeans_high",
                    "params:ses_cols_to_select_log",
                ],
                outputs=[
                    "ses_clusters_low",
                    "ses_clusters_medium",
                    "ses_clusters_high",
                ],
                name="Kmeans_Clustering_predict",
            ),
        ]
    )

    return preprocessing_pipeline + clustering_pipeline
