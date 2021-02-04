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

# in-built libraries
from typing import Any, Dict, List, Tuple, Union

# third-party libraries
import pandas
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.model_selection import KFold

# internal code
from ..single_model.single import (
    bin_target_variable,
    build_training_indices_matrix,
    create_categorical_encoder,
    create_numerical_scaler,
    encode_categorical_features,
    evaluate_predictions,
    impute_nulls,
    predict_outlet_target_var,
    retrieve_similar_outlets_ground_truths,
    scale_numerical_features,
    train_model,
)
from ..utils import obtain_features_target


def kfold_master_split(
    ra_master: pandas.DataFrame, model_params: Dict[str, Any]
) -> Dict[str, Dict[str, pandas.DataFrame]]:
    """Split master table into k separate pairs of datasets (train and test) based on outlet_id

    Args:
        ra_master: Reseller analytics joined master table on <outlet_id, month> granularity
        model_params: Model parameters which contains kfold parameters

    Returns:
        Dictionary of k dataset pairs, stored as index: {'train_set': train_set, 'test_set': test_set}
        (0-indexed, keys range from [0, k-1])
    """

    kfold = KFold(**model_params["kfold"])

    # get distinct outlet_ids
    distinct_outlet_ids = ra_master["outlet_id"].unique()

    # get split of outlet_ids into train and test sets for each fold
    kfold_train_test_sets_dict = {
        idx: {
            "train_set": ra_master[
                ra_master["outlet_id"].isin(distinct_outlet_ids[train_indices])
            ].reset_index(drop=True),
            "test_set": ra_master[
                ra_master["outlet_id"].isin(distinct_outlet_ids[test_indices])
            ].reset_index(drop=True),
        }
        for idx, (train_indices, test_indices) in enumerate(
            kfold.split(distinct_outlet_ids)
        )
    }

    return kfold_train_test_sets_dict


def kfold_bin_target(
    kfold_train_test_sets_dict: Dict[str, Dict[str, pandas.DataFrame]],
    model_params: Dict[str, Any],
) -> Dict[str, Dict[str, pandas.DataFrame]]:
    """Convert target variable into binned equal-size classes for each train-test dataset pair

    Args:
        kfold_train_test_sets_dict: K-fold dictionary of dataset pairs (train-test)
        model_params: Contains parameters such as number of classes, target variable name, etc.

    Returns:
        K-fold dictionary of dataset pairs (train-test) with target binned into equal-size classes
    """

    for key, dataset_pair in kfold_train_test_sets_dict.items():
        _, train_set_target_binned, test_set_target_binned = bin_target_variable(
            model_params, dataset_pair["train_set"], dataset_pair["test_set"]
        )

        # replace dataset pair in overall dictionary with binned target datasets
        kfold_train_test_sets_dict[key] = {
            "train_set": train_set_target_binned,
            "test_set": test_set_target_binned,
        }

    return kfold_train_test_sets_dict


def kfold_impute_nulls(
    kfold_train_test_sets_dict: Dict[str, Dict[str, pandas.DataFrame]],
    categorical_columns: List[str],
    numerical_columns: List[str],
    impute_params: Dict[str, Any],
) -> Dict[str, Dict[str, pandas.DataFrame]]:
    """Perform null imputation for categorical and numerical features for datasets

    Args:
        kfold_train_test_sets_dict: K-fold dictionary of dataset pairs (train-test)
        categorical_columns: Categorical columns to impute nulls for
        numerical_columns: Numerical columns to impute nulls for
        impute_params: Contains null replacement values

    Returns:
        K-fold dictionary of dataset pairs (train-test) with nulls imputed
    """

    for key, dataset_pair in kfold_train_test_sets_dict.items():
        train_set_imputed, test_set_imputed = impute_nulls(
            categorical_columns,
            numerical_columns,
            impute_params,
            dataset_pair["train_set"],
            dataset_pair["test_set"],
        )

        # replace dataset pair in overall dictionary with binned target datasets
        kfold_train_test_sets_dict[key] = {
            "train_set": train_set_imputed,
            "test_set": test_set_imputed,
        }

    return kfold_train_test_sets_dict


def kfold_typecast_columns(
    kfold_train_test_sets_dict: Dict[str, Dict[str, pandas.DataFrame]],
    categorical_columns: List[str],
    numerical_columns: List[str],
) -> Dict[str, Dict[str, pandas.DataFrame]]:
    """Perform typecasting for datasets

    Args:
        kfold_train_test_sets_dict: K-fold dictionary of dataset pairs (train-test)
        categorical_columns: Categorical columns to typecast to str
        numerical_columns: Numerical columns to typecast to float

    Returns:
        K-fold dictionary of dataset pairs (train-test) with typecasting performed on columns
    """

    typecast_dict = {
        **{categorical_column: str for categorical_column in categorical_columns},
        **{numerical_column: float for numerical_column in numerical_columns},
    }

    for key, dataset_pair in kfold_train_test_sets_dict.items():

        # replace dataset pair in overall dictionary with typecasted datasets
        kfold_train_test_sets_dict[key] = {
            "train_set": dataset_pair["train_set"].astype(typecast_dict),
            "test_set": dataset_pair["test_set"].astype(typecast_dict),
        }

    return kfold_train_test_sets_dict


def kfold_encode_categorical(
    kfold_train_test_sets_dict: Dict[str, Dict[str, pandas.DataFrame]],
    columns_to_encode: List[str],
    model_params: Dict[str, Any],
) -> Dict[str, Dict[str, pandas.DataFrame]]:
    """Encode categorical features for k pairs of datasets, using training sets to fit encoder

    Args:
        kfold_train_test_sets_dict: K-fold dictionary of dataset pairs (train-test)
        columns_to_encode: Categorical columns to encode
        model_params: Model parameter which contain boolean on whether to perform encoding

    Returns:
        K-fold dictionary of dataset pairs (train-test) with encoding performed on columns depending on boolean variable
    """

    if model_params["encode_features"]:

        for key, dataset_pair in kfold_train_test_sets_dict.items():

            # create encoder dict
            encoder_dict = create_categorical_encoder(
                columns_to_encode, model_params, dataset_pair["train_set"]
            )
            train_set_encoded, test_set_encoded = encode_categorical_features(
                encoder_dict,
                columns_to_encode,
                model_params,
                dataset_pair["train_set"],
                dataset_pair["test_set"],
            )

            # replace dataset pair in overall dictionary with encoded datasets
            kfold_train_test_sets_dict[key] = {
                "train_set": train_set_encoded,
                "test_set": test_set_encoded,
            }

    return kfold_train_test_sets_dict


def kfold_scale_numerical(
    kfold_train_test_sets_dict: Dict[str, Dict[str, pandas.DataFrame]],
    columns_to_scale: List[str],
    model_params: Dict[str, Any],
) -> Dict[str, Dict[str, pandas.DataFrame]]:
    """Scale numerical features for k pairs of datasets, using training sets to fit scaler

    Args:
        kfold_train_test_sets_dict: K-fold dictionary of dataset pairs (train-test)
        columns_to_scale: Numerical columns to scale
        model_params: Model parameter which contain boolean on whether to perform scaling

    Returns:
        K-fold dictionary of dataset pairs (train-test) with scaling performed on columns depending on boolean variable
    """

    if model_params["scale_features"]:

        for key, dataset_pair in kfold_train_test_sets_dict.items():

            # create scaler
            scaler = create_numerical_scaler(
                columns_to_scale, model_params, dataset_pair["train_set"]
            )
            train_set_scaled, test_set_scaled = scale_numerical_features(
                columns_to_scale,
                scaler,
                model_params,
                dataset_pair["train_set"],
                dataset_pair["test_set"],
            )

            # replace dataset pair in overall dictionary with scaled datasets
            kfold_train_test_sets_dict[key] = {
                "train_set": train_set_scaled,
                "test_set": test_set_scaled,
            }

    return kfold_train_test_sets_dict


def kfold_obtain_features_target(
    kfold_train_test_sets_dict: Dict[str, Dict[str, pandas.DataFrame]],
    columns_to_encode: List[str],
    columns_to_scale: List[str],
    model_params: Dict[str, Any],
) -> Dict[str, Dict[str, Dict[str, Union[pandas.DataFrame, pandas.Series]]]]:
    """Obtain features and target variable from each dataset for model input for training, prediction and evaluation

    Args:
        kfold_train_test_sets_dict: K-fold dictionary of dataset pairs (train-test)
        columns_to_encode: Categorical columns to encode
        columns_to_scale: Numerical columns to scale
        model_params: Contains model arguments and list of features and target variable names

    Returns:
        K-fold dictionary of dataset pairs (train-test), with each dataset being a dictionary of (features and target)
    """

    for key, dataset_pair in kfold_train_test_sets_dict.items():

        (
            train_set_features,
            train_set_target,
            test_set_features,
            test_set_target,
        ) = obtain_features_target(
            model_params,
            columns_to_encode,
            columns_to_scale,
            dataset_pair["train_set"],
            dataset_pair["test_set"],
        )

        # replace dataset pair in overall dictionary with dictionary of features and target
        kfold_train_test_sets_dict[key] = {
            "train_set": {"features": train_set_features, "target": train_set_target},
            "test_set": {"features": test_set_features, "target": test_set_target},
        }

    return kfold_train_test_sets_dict


def kfold_train_models(
    kfold_train_test_sets_features_target_dict: Dict[
        str, Dict[str, Dict[str, Union[pandas.DataFrame, pandas.Series]]]
    ],
    model_params: Dict[str, Any],
) -> Tuple[
    Dict[str, Union[RandomForestClassifier, RandomForestRegressor]],
    List[str],
    Dict[str, Dict[str, Union[int, List[int]]]],
]:
    """Train k models with each training set

    Args:
        kfold_train_test_sets_features_target_dict: K-fold dictionary of dataset pairs (train-test) with features and
            target stored as a nested dictionary
        model_params: Contains parameters (model type, booleans for encoding/scaling, columns to drop, target variable)

    Returns:
        Dictionary with k models, list of feature names and dictionary with info to build training indices matrices
    """

    kfold_models, training_matrix_info_dict = {}, {}
    feature_columns = kfold_train_test_sets_features_target_dict[0]["train_set"][
        "features"
    ].columns

    for key, dataset_pair in kfold_train_test_sets_features_target_dict.items():

        # train model and extract required information
        fitted_model, _, max_leaf_size, trees_bootstrap_indices = train_model(
            dataset_pair["train_set"]["features"],
            dataset_pair["train_set"]["target"],
            model_params,
        )

        # assign model objects and info to build training matrix to dictionaries
        kfold_models[key] = fitted_model
        training_matrix_info_dict[key] = {
            "max_leaf_size": max_leaf_size,
            "trees_bootstrap_indices": trees_bootstrap_indices,
        }

    return kfold_models, feature_columns, training_matrix_info_dict


def kfold_predict(
    kfold_train_test_sets_features_target_dict: Dict[
        str, Dict[str, Dict[str, Union[pandas.DataFrame, pandas.Series]]]
    ],
    kfold_train_test_sets_dict: Dict[str, Dict[str, pandas.DataFrame]],
    kfold_models: Dict[str, Union[RandomForestClassifier, RandomForestRegressor]],
    model_params: Dict[str, Any],
) -> Tuple[
    Dict[str, Dict[str, pandas.DataFrame]], Dict[str, Dict[str, pandas.DataFrame]]
]:
    """Obtain prediction and leaf node indices for datasets passed into the function using the same fold model

    Args:
        kfold_train_test_sets_features_target_dict: K-fold dictionary of dataset pairs (train-test) with features and
            target stored as a nested dictionary
        kfold_train_test_sets_dict: K-fold dictionary of dataset pairs (train-test)
        kfold_models: Dictionary with k models
        model_params: Contains model parameter which determine either classification or regression modeling

    Returns:
        Dictionary of model prediction for datasets and dictionary of leaf node indices for datasets
    """

    kfold_train_test_sets_prediction_dict = {}
    kfold_leaf_indices_sets_dict = {}

    for key, dataset_pair in kfold_train_test_sets_features_target_dict.items():

        (
            train_set_pred,
            train_set_leaf_indices,
            test_set_pred,
            test_set_leaf_indices,
        ) = predict_outlet_target_var(
            kfold_models[key],
            model_params,
            dataset_pair["train_set"]["features"],
            kfold_train_test_sets_dict[key]["train_set"],
            dataset_pair["test_set"]["features"],
            kfold_train_test_sets_dict[key]["test_set"],
        )

        # assign to output dictionaries
        kfold_train_test_sets_prediction_dict[key] = {
            "train_set": train_set_pred,
            "test_set": test_set_pred,
        }
        kfold_leaf_indices_sets_dict[key] = {
            "train_set": train_set_leaf_indices,
            "test_set": test_set_leaf_indices,
        }

    return kfold_train_test_sets_prediction_dict, kfold_leaf_indices_sets_dict


def kfold_evaluate(
    kfold_train_test_sets_prediction_dict: Dict[str, Dict[str, pandas.DataFrame]],
    model_params: Dict[str, Any],
    features_columns: List[str],
) -> pandas.DataFrame:
    """Evaluate k-fold models performance

    Args:
        kfold_train_test_sets_prediction_dict: K-fold dictionary of model prediction for datasets
        model_params: Contains model type and target variable name
        features_columns: Names of features used to train model

    Returns:
        Evaluation metrics of datasets aggregated across folds (mean, median, min, max, std)
    """

    test_sets_preds = [
        dataset_pair["test_set"]
        for dataset_pair in kfold_train_test_sets_prediction_dict.values()
    ]

    # aggregate metrics from training and test sets
    agg_fn_names = ["mean", "std", "median", "min", "max"]
    test_set_metrics = evaluate_predictions(
        model_params, features_columns, *test_sets_preds
    )
    metrics = [col for col in test_set_metrics.columns if "dataset_num" not in col]
    test_set_metrics_agg = test_set_metrics[metrics].agg(agg_fn_names)

    # replace indices of aggregated DataFrames before outputting
    test_indices = [f"{col}_test_set" for col in test_set_metrics_agg.index.tolist()]
    test_set_metrics_agg.index = test_indices

    # rename index before saving
    kfold_metrics_dataframe = test_set_metrics_agg
    kfold_metrics_dataframe.index.name = "aggregate_function"

    return kfold_metrics_dataframe.reset_index()


def kfold_build_training_indices_matrix(
    kfold_models: Dict[str, Union[RandomForestClassifier, RandomForestRegressor]],
    training_matrix_info_dict: Dict[str, Dict[str, Union[int, List[int]]]],
    kfold_train_test_sets_features_target_dict: Dict[
        str, Dict[str, Dict[str, Union[pandas.DataFrame, pandas.Series]]]
    ],
) -> Dict[str, pandas.DataFrame]:
    """Build training set indices matrices, for quick retrieval of similar outlets that reside in the same leaf node

    Args:
        kfold_models: Dictionary with k models
        training_matrix_info_dict: Dictionary with info to build training indices matrices
        kfold_train_test_sets_features_target_dict: K-fold dictionary of dataset pairs (train-test) with features and
            target stored as a nested dictionary

    Returns:
        Dictionary of training indices matrices
    """

    kfold_training_indices_matrices = {}

    for key, dataset_pair in kfold_train_test_sets_features_target_dict.items():

        training_indices_matrix = build_training_indices_matrix(
            kfold_models[key],
            training_matrix_info_dict[key]["max_leaf_size"],
            training_matrix_info_dict[key]["trees_bootstrap_indices"],
            dataset_pair["train_set"]["features"],
        )

        # assign to output dictionary
        kfold_training_indices_matrices[key] = training_indices_matrix

    return kfold_training_indices_matrices


def kfold_retrieve_similar_outlets_ground_truths(
    kfold_leaf_indices_sets_dict: Dict[str, Dict[str, pandas.DataFrame]],
    kfold_training_indices_matrices: Dict[str, pandas.DataFrame],
    kfold_train_test_sets_dict: Dict[str, Dict[str, pandas.DataFrame]],
    kfold_train_test_sets_prediction_dict: Dict[str, Dict[str, pandas.DataFrame]],
    model_params: Dict[str, Any],
) -> pandas.DataFrame:
    """Retrieve all similar outlets cashflows for outlets that fall in the test sets for each fold

    Args:
        kfold_leaf_indices_sets_dict: Dictionary of leaf node indices for datasets
        kfold_training_indices_matrices: Dictionary of training indices matrices, also known as the bootstrapped
            datasets indices for each leaf node of each tree
        kfold_train_test_sets_dict: K-fold dictionary of dataset pairs (train-test)
        kfold_train_test_sets_prediction_dict: Dictionary of model prediction for datasets
        model_params: Contains target variable name and predicted model output column name

    Returns:
        Single DataFrame with all outlets in the dataset and their similar outlets cashflows
    """

    similar_outlets_cashflow_df = pandas.DataFrame()

    for key, leaf_indices_pair in kfold_leaf_indices_sets_dict.items():

        # prepare function input arguments
        predictions_leaf_indices = leaf_indices_pair["test_set"]
        training_indices_matrix = kfold_training_indices_matrices[key]
        training_set = kfold_train_test_sets_dict[key]["train_set"]
        prepared_dataset_of_predictions = kfold_train_test_sets_dict[key]["test_set"]
        predictions = kfold_train_test_sets_prediction_dict[key]["test_set"]

        similar_outlets_cashflow_for_fold = retrieve_similar_outlets_ground_truths(
            predictions_leaf_indices,
            training_indices_matrix,
            training_set,
            prepared_dataset_of_predictions,
            predictions,
            model_params,
        )

        if not key:
            similar_outlets_cashflow_df = similar_outlets_cashflow_for_fold
        else:
            similar_outlets_cashflow_df = similar_outlets_cashflow_df.append(
                similar_outlets_cashflow_for_fold, ignore_index=True
            )

    return similar_outlets_cashflow_df
