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
    obtain_outlet_performance_percentile,
    prepare_dashboard_input_table,
    prepare_dashboard_performance_plot_data,
)
from .kfold import (
    kfold_bin_target,
    kfold_build_training_indices_matrix,
    kfold_encode_categorical,
    kfold_evaluate,
    kfold_impute_nulls,
    kfold_master_split,
    kfold_obtain_features_target,
    kfold_predict,
    kfold_retrieve_similar_outlets_ground_truths,
    kfold_scale_numerical,
    kfold_train_models,
    kfold_typecast_columns,
)


def create_pipeline() -> Pipeline:
    """Create reseller analytics data science k-fold modeling pipeline

    What does it do:
        - Trains k models and obtain performance percentile values for all outlets in the dataset
        - Node functions are wrapper functions of single model node function, expanded to run k times
        - Saves performance percentile outputs to two separate hive tables for Tableau dashboard to utilise as inputs

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
                name="ra_mck_int_kfold_deduplication",
            ),
            node(
                func=master_sample,
                inputs=[
                    "ra_master_deduplicated",
                    "params:ra_model",
                    "params:ra_return_threshold_filtered_outlets",
                ],
                outputs=[
                    "ra_master_sample",
                    "ra_filter_bottom_removed_outlets",
                    "ra_filter_top_removed_outlets",
                ],
                name="ra_mck_int_kfold_sample_master",
            ),
            node(
                func=kfold_master_split,
                inputs=["ra_master_sample", "params:ra_model"],
                outputs="ra_mck_int_kfold_train_test_sets_dict",
                name="ra_mck_int_kfold_master_split",
            ),
            node(
                func=kfold_bin_target,
                inputs=["ra_mck_int_kfold_train_test_sets_dict", "params:ra_model"],
                outputs="ra_mck_int_kfold_train_test_sets_dict_binned_target",
                name="ra_mck_int_kfold_bin_target",
            ),
            node(
                func=kfold_impute_nulls,
                inputs=[
                    "ra_mck_int_kfold_train_test_sets_dict_binned_target",
                    "params:ra_categorical_features",
                    "params:ra_numerical_features",
                    "params:ra_impute_values",
                ],
                outputs="ra_mck_int_kfold_train_test_sets_dict_imputed",
                name="ra_mck_int_kfold_imputation",
            ),
            node(
                func=kfold_typecast_columns,
                inputs=[
                    "ra_mck_int_kfold_train_test_sets_dict_imputed",
                    "params:ra_categorical_features",
                    "params:ra_numerical_features",
                ],
                outputs="ra_mck_int_kfold_train_test_sets_dict_typecasted",
                name="ra_mck_int_kfold_typecasting",
            ),
            node(
                func=kfold_encode_categorical,
                inputs=[
                    "ra_mck_int_kfold_train_test_sets_dict_typecasted",
                    "params:ra_categorical_features",
                    "params:ra_model",
                ],
                outputs="ra_mck_int_kfold_train_test_sets_dict_encoded",
                name="ra_mck_int_kfold_encoding",
            ),
            node(
                func=kfold_scale_numerical,
                inputs=[
                    "ra_mck_int_kfold_train_test_sets_dict_encoded",
                    "params:ra_numerical_features",
                    "params:ra_model",
                ],
                outputs="ra_mck_int_kfold_train_test_sets_dict_prepared",
                name="ra_mck_int_kfold_scaling",
            ),
            node(
                func=kfold_obtain_features_target,
                inputs=[
                    "ra_mck_int_kfold_train_test_sets_dict_prepared",
                    "params:ra_categorical_features",
                    "params:ra_numerical_features",
                    "params:ra_model",
                ],
                outputs="ra_mck_int_kfold_train_test_sets_features_target_dict",
                name="ra_mck_int_kfold_obtain_features_target",
            ),
            node(
                func=kfold_train_models,
                inputs=[
                    "ra_mck_int_kfold_train_test_sets_features_target_dict",
                    "params:ra_model",
                ],
                outputs=[
                    "ra_mck_int_kfold_models",
                    "ra_mck_int_kfold_features_columns",
                    "ra_mck_int_kfold_training_matrix_info_dict",
                ],
                name="ra_mck_int_kfold_model_training",
            ),
            node(
                func=kfold_predict,
                inputs=[
                    "ra_mck_int_kfold_train_test_sets_features_target_dict",
                    "ra_mck_int_kfold_train_test_sets_dict_prepared",
                    "ra_mck_int_kfold_models",
                    "params:ra_model",
                ],
                outputs=[
                    "ra_mck_int_kfold_train_test_sets_prediction_dict",
                    "ra_mck_int_kfold_leaf_indices_sets_dict",
                ],
                name="ra_mck_int_kfold_prediction",
            ),
            node(
                func=kfold_evaluate,
                inputs=[
                    "ra_mck_int_kfold_train_test_sets_prediction_dict",
                    "params:ra_model",
                    "ra_mck_int_kfold_features_columns",
                ],
                outputs="ra_mck_int_kfold_evaluation_metrics",
                name="ra_mck_int_kfold_evaluation",
            ),
            node(
                func=kfold_build_training_indices_matrix,
                inputs=[
                    "ra_mck_int_kfold_models",
                    "ra_mck_int_kfold_training_matrix_info_dict",
                    "ra_mck_int_kfold_train_test_sets_features_target_dict",
                ],
                outputs="ra_mck_int_kfold_training_indices_matrices_dict",
                name="ra_mck_int_kfold_build_training_indices_matrix",
            ),
            node(
                func=kfold_retrieve_similar_outlets_ground_truths,
                inputs=[
                    "ra_mck_int_kfold_leaf_indices_sets_dict",
                    "ra_mck_int_kfold_training_indices_matrices_dict",
                    "ra_mck_int_kfold_train_test_sets_dict_prepared",
                    "ra_mck_int_kfold_train_test_sets_prediction_dict",
                    "params:ra_model",
                ],
                outputs="ra_mck_int_kfold_similar_outlets_data",
                name="ra_mck_int_kfold_retrieve_similar_outlets_ground_truths",
            ),
            node(
                func=obtain_outlet_performance_percentile,
                inputs="ra_mck_int_kfold_similar_outlets_data",
                outputs="ra_mck_int_kfold_performance_percentiles",
                name="ra_mck_int_kfold_performance_percentiles",
            ),
            node(
                func=prepare_dashboard_input_table,
                inputs=[
                    "ra_master_pandas",
                    "ra_mck_int_kfold_performance_percentiles",
                    "params:ra_dashboard_main_input_master_table_cols",
                    "params:ra_dashboard_performance_percentile_table_cols",
                    "params:ra_dashboard_main_input_typecast",
                ],
                outputs="ra_mck_int_dashboard_main_input",
                name="ra_mck_int_kfold_prepare_dashboard_input_table",
            ),
            node(
                func=prepare_dashboard_performance_plot_data,
                inputs=[
                    "ra_master_pandas",
                    "ra_mck_int_kfold_performance_percentiles",
                    "params:ra_dashboard_performance_plot_master_table_cols",
                    "params:ra_dashboard_performance_percentile_table_cols",
                    "params:ra_dashboard_performance_plot_typecast",
                    "params:ra_dashboard_performance_plot_melt_id_vars",
                ],
                outputs="ra_mck_int_dashboard_performance_plot",
                name="ra_mck_int_kfold_prepare_dashboard_performance_plot_data",
            ),
        ],
    )
