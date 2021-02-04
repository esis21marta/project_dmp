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

from ..single_model.single import (
    bin_target_variable,
    create_categorical_encoder,
    create_numerical_scaler,
    encode_categorical_features,
    impute_nulls,
    scale_numerical_features,
    typecast_columns,
)
from ..utils import master_remove_duplicates, master_sample, obtain_features_target
from .gridsearchcv import instantiate_gridsearchcv


def create_pipeline() -> Pipeline:
    """Create reseller analytics data science hyperparameter search pipeline

    What does it do:
        - Retrieve best hyperparameter combination
        - Based on parameter grid defined in parameters.yml under ra_gridsearch_cv
        - Utilise node functions from single model pipeline to preprocess data before initiating grid search

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
                    "params:ra_gridsearch_cv",
                ],
                outputs="ra_mck_int_gridsearchcv_master_deduplicated",
                name="ra_mck_int_gridsearchcv_remove_duplicates",
            ),
            node(
                func=master_sample,
                inputs=[
                    "ra_mck_int_gridsearchcv_master_deduplicated",
                    "params:ra_gridsearch_cv",
                ],
                outputs="ra_mck_int_gridsearchcv_master_sample",
                name="ra_mck_int_gridsearchcv_master_sample",
            ),
            node(
                func=bin_target_variable,
                inputs=[
                    "params:ra_gridsearch_cv",
                    "ra_mck_int_gridsearchcv_master_sample",
                ],
                outputs=[
                    "ra_mck_int_gridsearchcv_bins",
                    "ra_mck_int_gridsearchcv_master_bin_target",
                ],
                name="ra_mck_int_single_model_bin_target",
            ),
            node(
                func=impute_nulls,
                inputs=[
                    "params:ra_categorical_features",
                    "params:ra_numerical_features",
                    "params:ra_impute_values",
                    "ra_mck_int_gridsearchcv_master_bin_target",
                ],
                outputs="ra_mck_int_gridsearchcv_master_imputed",
                name="ra_mck_int_gridsearchcv_impute_nulls",
            ),
            node(
                func=typecast_columns,
                inputs=[
                    "params:ra_categorical_features",
                    "params:ra_numerical_features",
                    "ra_mck_int_gridsearchcv_master_imputed",
                ],
                outputs="ra_mck_int_gridsearchcv_master_typecasted",
                name="ra_mck_int_gridsearchcv_typecast",
            ),
            node(
                func=create_categorical_encoder,
                inputs=[
                    "params:ra_categorical_features",
                    "params:ra_gridsearch_cv",
                    "ra_mck_int_gridsearchcv_master_typecasted",
                ],
                outputs="ra_mck_int_gridsearchcv_categorical_encoder",
                name="ra_mck_int_gridsearchcv_create_categorical_encoder",
            ),
            node(
                func=encode_categorical_features,
                inputs=[
                    "ra_mck_int_gridsearchcv_categorical_encoder",
                    "params:ra_categorical_features",
                    "params:ra_gridsearch_cv",
                    "ra_mck_int_gridsearchcv_master_typecasted",
                ],
                outputs="ra_mck_int_gridsearchcv_master_encoded",
                name="ra_mck_int_gridsearchcv_encode_categorical_features",
            ),
            node(
                func=create_numerical_scaler,
                inputs=[
                    "params:ra_numerical_features",
                    "params:ra_gridsearch_cv",
                    "ra_mck_int_gridsearchcv_master_encoded",
                ],
                outputs="ra_mck_int_gridsearchcv_numerical_scaler",
                name="ra_mck_int_gridsearchcv_create_numerical_scaler",
            ),
            node(
                func=scale_numerical_features,
                inputs=[
                    "params:ra_numerical_features",
                    "ra_mck_int_gridsearchcv_numerical_scaler",
                    "params:ra_gridsearch_cv",
                    "ra_mck_int_gridsearchcv_master_encoded",
                ],
                outputs="ra_mck_int_gridsearchcv_master_prepared",
                name="ra_mck_int_gridsearchcv_scale_numerical_features",
            ),
            node(
                func=obtain_features_target,
                inputs=[
                    "params:ra_model",
                    "params:ra_categorical_features",
                    "params:ra_numerical_features",
                    "ra_mck_int_gridsearchcv_master_prepared",
                ],
                outputs=[
                    "ra_mck_int_gridsearchcv_features",
                    "ra_mck_int_gridsearchcv_target",
                ],
                name="ra_mck_int_gridsearchcv_obtain_features_target",
            ),
            node(
                func=instantiate_gridsearchcv,
                inputs=[
                    "ra_mck_int_gridsearchcv_features",
                    "ra_mck_int_gridsearchcv_target",
                    "params:ra_gridsearch_cv",
                ],
                outputs=[
                    "ra_mck_int_gridsearchcv_results",
                    "ra_mck_int_gridsearchcv_params",
                    "ra_mck_int_gridsearchcv_feature_importances",
                ],
                name="ra_mck_int_gridsearchcv_execute_gridsearch",
            ),
        ],
    )
