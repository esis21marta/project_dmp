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

# node functions
from ..utils import (
    master_remove_duplicates,
    master_sample,
    obtain_features_target,
    obtain_outlet_performance_percentile,
    plot_overall_performance_dist,
)
from .single import (
    bin_target_variable,
    build_training_indices_matrix,
    create_categorical_encoder,
    create_numerical_scaler,
    encode_categorical_features,
    evaluate_predictions,
    extract_feature_importance,
    impute_nulls,
    predict_outlet_target_var,
    retrieve_similar_outlets_ground_truths,
    scale_numerical_features,
    train_model,
    train_test_dataset_split,
    typecast_columns,
)


def create_pipeline() -> Pipeline:
    """Create reseller analytics data science single model pipeline

    What does it do:
        - Efficiently evaluate performance on model with:
            - New features
            - Different model parameters
            - Obtain performance percentiles for a few specified outlets

    :return: A Pipeline object
    """
    return Pipeline(
        [
            node(
                func=master_remove_duplicates,
                inputs=[
                    "ra_master_pandas",
                    "params:ra_numerical_features",
                    "params:ra_categorical_features",
                    "params:ra_model",
                ],
                outputs="ra_master_deduplicated",
                name="ra_mck_int_single_model_remove_duplicates",
            ),
            node(
                func=master_sample,
                inputs=["ra_master_deduplicated", "params:ra_model"],
                outputs="ra_master_sample",
                name="ra_mck_int_single_model_sample_master",
            ),
            node(
                func=train_test_dataset_split,
                inputs=["ra_master_sample", "params:ra_model"],
                outputs=[
                    "ra_mck_int_single_model_training_set",
                    "ra_mck_int_single_model_test_set",
                ],
                name="ra_mck_int_single_model_dataset_split",
            ),
            node(
                func=bin_target_variable,
                inputs=[
                    "params:ra_model",
                    "ra_mck_int_single_model_training_set",
                    "ra_mck_int_single_model_test_set",
                ],
                outputs=[
                    "ra_mck_int_single_model_training_set_bins",
                    "ra_mck_int_single_model_training_set_bin_target",
                    "ra_mck_int_single_model_test_set_bin_target",
                ],
                name="ra_mck_int_single_model_bin_targets",
            ),
            node(
                func=impute_nulls,
                inputs=[
                    "params:ra_categorical_features",
                    "params:ra_numerical_features",
                    "params:ra_impute_values",
                    "ra_mck_int_single_model_training_set_bin_target",
                    "ra_mck_int_single_model_test_set_bin_target",
                ],
                outputs=[
                    "ra_mck_int_single_model_training_set_imputed",
                    "ra_mck_int_single_model_test_set_imputed",
                ],
                name="ra_mck_int_single_model_impute_nulls",
            ),
            node(
                func=typecast_columns,
                inputs=[
                    "params:ra_categorical_features",
                    "params:ra_numerical_features",
                    "ra_mck_int_single_model_training_set_imputed",
                    "ra_mck_int_single_model_test_set_imputed",
                ],
                outputs=[
                    "ra_mck_int_single_model_training_set_cleaned",
                    "ra_mck_int_single_model_test_set_cleaned",
                ],
                name="ra_mck_int_single_model_typecast_columns",
            ),
            node(
                func=create_categorical_encoder,
                inputs=[
                    "params:ra_categorical_features",
                    "params:ra_model",
                    "ra_mck_int_single_model_training_set_cleaned",
                ],
                outputs="ra_mck_int_single_model_categorical_encoder",
                name="ra_mck_int_single_model_create_categorical_encoder",
            ),
            node(
                func=encode_categorical_features,
                inputs=[
                    "ra_mck_int_single_model_categorical_encoder",
                    "params:ra_categorical_features",
                    "params:ra_model",
                    "ra_mck_int_single_model_training_set_cleaned",
                    "ra_mck_int_single_model_test_set_cleaned",
                ],
                outputs=[
                    "ra_mck_int_single_model_training_set_encoded",
                    "ra_mck_int_single_model_test_set_encoded",
                ],
                name="ra_mck_int_single_model_encode_categorical_features",
            ),
            node(
                func=create_numerical_scaler,
                inputs=[
                    "params:ra_numerical_features",
                    "params:ra_model",
                    "ra_mck_int_single_model_training_set_encoded",
                ],
                outputs="ra_mck_int_single_model_numerical_scaler",
                name="ra_mck_int_single_model_create_numerical_scaler",
            ),
            node(
                func=scale_numerical_features,
                inputs=[
                    "params:ra_numerical_features",
                    "ra_mck_int_single_model_numerical_scaler",
                    "params:ra_model",
                    "ra_mck_int_single_model_training_set_encoded",
                    "ra_mck_int_single_model_test_set_encoded",
                ],
                outputs=[
                    "ra_mck_int_single_model_training_set_prepared",
                    "ra_mck_int_single_model_test_set_prepared",
                ],
            ),
            node(
                func=obtain_features_target,
                inputs=[
                    "params:ra_model",
                    "params:ra_categorical_features",
                    "params:ra_numerical_features",
                    "ra_mck_int_single_model_training_set_prepared",
                    "ra_mck_int_single_model_test_set_prepared",
                ],
                outputs=[
                    "ra_mck_int_single_model_training_set_features",
                    "ra_mck_int_single_model_training_set_target",
                    "ra_mck_int_single_model_test_set_features",
                    "ra_mck_int_single_model_test_set_target",
                ],
                name="ra_mck_int_single_model_obtain_features_target",
            ),
            node(
                func=train_model,
                inputs=[
                    "ra_mck_int_single_model_training_set_features",
                    "ra_mck_int_single_model_training_set_target",
                    "params:ra_model",
                ],
                outputs=[
                    "ra_mck_int_single_model_trained_model",
                    "ra_mck_int_single_model_features_list",
                    "ra_mck_int_single_model_max_leaves",
                    "ra_mck_int_single_model_trees_bootstrap_indices",
                ],
                name="ra_mck_int_single_model_train_model",
            ),
            node(
                func=extract_feature_importance,
                inputs=[
                    "ra_mck_int_single_model_trained_model",
                    "ra_mck_int_single_model_features_list",
                ],
                outputs="ra_mck_int_single_model_feature_importances",
                name="ra_mck_int_single_model_feature_importances",
            ),
            node(
                func=predict_outlet_target_var,
                inputs=[
                    "ra_mck_int_single_model_trained_model",
                    "params:ra_model",
                    "ra_mck_int_single_model_training_set_features",
                    "ra_mck_int_single_model_training_set_prepared",
                    "ra_mck_int_single_model_test_set_features",
                    "ra_mck_int_single_model_test_set_prepared",
                ],
                outputs=[
                    "ra_mck_int_single_model_training_set_predictions",
                    "ra_mck_int_single_model_training_set_model_leaf_indices",
                    "ra_mck_int_single_model_test_set_predictions",
                    "ra_mck_int_single_model_test_set_model_leaf_indices",
                ],
                name="ra_mck_int_single_model_predictions_and_leaf_indices",
            ),
            node(
                func=evaluate_predictions,
                inputs=[
                    "params:ra_model",
                    "ra_mck_int_single_model_features_list",
                    "ra_mck_int_single_model_training_set_predictions",
                    "ra_mck_int_single_model_test_set_predictions",
                ],
                outputs="ra_mck_int_single_model_evaluation_metrics",
                name="ra_mck_int_single_model_evaluate_model",
            ),
            node(
                func=build_training_indices_matrix,
                inputs=[
                    "ra_mck_int_single_model_trained_model",
                    "ra_mck_int_single_model_max_leaves",
                    "ra_mck_int_single_model_trees_bootstrap_indices",
                    "ra_mck_int_single_model_training_set_features",
                ],
                outputs="ra_mck_int_single_model_training_indices_matrix",
                name="ra_mck_int_single_model_build_training_indices_matrix",
            ),
            node(
                func=retrieve_similar_outlets_ground_truths,
                inputs=[
                    "ra_mck_int_single_model_test_set_model_leaf_indices",
                    "ra_mck_int_single_model_training_indices_matrix",
                    "ra_mck_int_single_model_training_set_prepared",
                    "ra_mck_int_single_model_test_set_prepared",
                    "ra_mck_int_single_model_test_set_predictions",
                    "params:ra_model",
                ],
                outputs="ra_mck_int_single_model_similar_outlets_data",
                name="ra_mck_int_single_model_retrieve_similar_outlets_ground_truths",
            ),
            node(
                func=obtain_outlet_performance_percentile,
                inputs="ra_mck_int_single_model_similar_outlets_data",
                outputs="ra_mck_int_single_model_performance_percentiles",
                name="ra_mck_int_single_model_performance_percentiles",
            ),
            node(
                func=plot_overall_performance_dist,
                inputs=[
                    "ra_mck_int_single_model_performance_percentiles",
                    "params:ra_overall_performance_scatter",
                ],
                outputs="ra_mck_int_single_model_overall_performance_scatter",
                name="ra_mck_int_single_model_overall_performance_scatter",
            ),
        ],
    )
