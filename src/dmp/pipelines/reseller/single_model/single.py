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
import logging
from typing import Any, Dict, List, Tuple, Union

# third-party libraries
import numpy
import pandas
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import confusion_matrix, max_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OrdinalEncoder, RobustScaler

# internal code
from ..misc.outlet_id_filters import include_outlet_ids_in_test_set
from ..utils import auc_average, rmse, rrse, rse, unpack_if_single_element

logger = logging.getLogger(__name__)


def train_test_dataset_split(
    ra_master: pandas.DataFrame, model_params: Dict[str, Any]
) -> Tuple[pandas.DataFrame, ...]:
    """Retrieve training set from joined outlet master table by training set ratio of outlet_id

    Args:
        ra_master: Joined outlet master table
        model_params: Contains training and test set ratios and random seed

    Returns:
        Training and test sets
    """

    # obtain distinct outlets
    distinct_outlets = pandas.Series(ra_master["outlet_id"].unique())

    # train-test split on distinct outlet identifiers, step 1: split into training and test sets
    train_outlets, test_outlets = train_test_split(
        distinct_outlets,
        random_state=model_params["random_state"],
        train_size=model_params["split_ratio"]["train"],
    )

    # obtain outlet_ids to include in test set if boolean parameter is true
    include_in_test_set_outlet_ids = (
        include_outlet_ids_in_test_set()
        if model_params["include_specific_outlet_ids_in_test_set"]
        else None
    )

    if include_in_test_set_outlet_ids:
        test_not_including_specified_outlets = test_outlets[
            ~test_outlets.isin(include_in_test_set_outlet_ids)
        ]
        train_including_specified_outlets = train_outlets[
            train_outlets.isin(include_in_test_set_outlet_ids)
        ]
        train_not_including_specified_outlets = train_outlets[
            ~train_outlets.isin(include_in_test_set_outlet_ids)
        ]

        train_outlets = train_not_including_specified_outlets.append(
            test_not_including_specified_outlets.iloc[
                : len(train_including_specified_outlets)
            ],
            ignore_index=True,
        )
        test_outlets = test_outlets[~test_outlets.isin(train_outlets)].append(
            train_including_specified_outlets, ignore_index=True
        )

    # filter outlets master table to only outlets from training and test sets
    train_set = ra_master.loc[
        ra_master["outlet_id"].isin(train_outlets), :
    ].reset_index(drop=True)
    test_set = ra_master.loc[ra_master["outlet_id"].isin(test_outlets), :].reset_index(
        drop=True
    )

    return train_set, test_set


def bin_target_variable(
    model_params: Dict[str, Any], *datasets: pandas.DataFrame
) -> Tuple[Any, ...]:
    """Convert target variable into equal-size binned classes

    Args:
        model_params: Contains parameters such as number of classes, target variable name, etc.
        *datasets: Data sets to perform target variable binning on

    Returns:
        Data sets with target variable binned into classes
    """

    bins = numpy.ndarray([])

    if model_params["model_type"] == "classification":
        num_classes = model_params["classification_num_target_classes"]
        target_variable = model_params["target_variable"]
        target_variable_binned = f"{target_variable}_binned"
        for idx, dataset in enumerate(datasets):
            if not idx:
                dataset.loc[:, target_variable_binned], bins = pandas.qcut(
                    dataset[target_variable],
                    num_classes,
                    labels=range(num_classes),
                    retbins=True,
                )
                bins[0] = 0.0
                logger.info(
                    f"\nBins edges of target variable (classification):\n{bins}\n"
                )
            else:
                if dataset[target_variable].max() > bins[-1]:
                    bins[-1] = dataset[target_variable].max()
                    logger.info(
                        f"\nUpdated bins edges of target variable (classification):\n{bins}\n"
                    )
                dataset.loc[:, target_variable_binned] = pandas.cut(
                    dataset[target_variable], bins=bins, labels=False
                )

    output_tuple = (bins, *datasets)

    return output_tuple


def impute_nulls(
    categorical_columns: List[str],
    numerical_columns: List[str],
    impute_params: Dict[str, Any],
    *datasets: pandas.DataFrame,
) -> Union[pandas.DataFrame, List[pandas.DataFrame]]:
    """Impute nulls with specified values

    Args:
        categorical_columns: Categorical features to impute nulls
        numerical_columns: Numerical features to impute nulls
        impute_params: Contains parameters on value to impute for categorical and numerical features
        *datasets: Data sets to perform null imputation on

    Returns:
        Data sets with nulls imputed with specified values for categorical and numerical features
    """

    imputed_datasets = []

    for dataset in datasets:
        impute_dataset = dataset.copy()

        impute_dataset.loc[:, categorical_columns] = impute_dataset.loc[
            :, categorical_columns
        ].fillna(value=impute_params["categorical"])

        impute_dataset.loc[:, numerical_columns] = impute_dataset.loc[
            :, numerical_columns
        ].fillna(value=float(impute_params["numerical"]))
        imputed_datasets.append(impute_dataset)

    return unpack_if_single_element(imputed_datasets)


def typecast_columns(
    categorical_columns: List[str],
    numerical_columns: List[str],
    *datasets: pandas.DataFrame,
) -> Union[pandas.DataFrame, List[pandas.DataFrame]]:
    """Convert categorical and numerical features from data sets into strings and floats respectively

    Args:
        categorical_columns: Categorical features to typecast to strings
        numerical_columns: Numerical features to typecast to floats
        *datasets: Data sets to perform typecasting on

    Returns:
        Data sets with categorical and numerical features typecasted to strings and floats respectively
    """

    typecast_dict = {
        **{categorical_column: str for categorical_column in categorical_columns},
        **{numerical_column: float for numerical_column in numerical_columns},
    }

    typecasted_datasets = [dataset.astype(typecast_dict) for dataset in datasets]

    return unpack_if_single_element(typecasted_datasets)


def create_categorical_encoder(
    columns_to_encode: List[str],
    model_params: Dict[str, Any],
    training_set: pandas.DataFrame,
) -> Dict[str, Any]:
    """Create categorical feature encoder based on training set

    Args:
        columns_to_encode: Categorical features to encode
        model_params: Contains boolean parameter on whether to perform encoding
        training_set: Training set to create and fit categorical feature encoder on

    Returns:
        Dict with encoder and dict of categorical features to their unique set of values
    """

    encoder_dict = {}

    if model_params["encode_features"]:

        unique_categorical_values_dict = {
            column: set(training_set[column].unique()) for column in columns_to_encode
        }

        ord_encoder = OrdinalEncoder().fit(training_set[columns_to_encode])

        ord_encoder.categories_ = [
            numpy.append(category_array, "UNKNOWN")
            for category_array in ord_encoder.categories_
        ]

        encoder_dict = {
            "categories_dict": unique_categorical_values_dict,
            "ord_encoder": ord_encoder,
        }

    return encoder_dict


def encode_categorical_features(
    encoder_dict: Dict[str, Any],
    columns_to_encode: List[str],
    model_params: Dict[str, Any],
    *datasets: pandas.DataFrame,
) -> Union[pandas.DataFrame, List[pandas.DataFrame]]:
    """Encodes categorical features and add them to the data sets as additional columns

    Args:
        encoder_dict: Dict with encoder and dict of categorical features to their unique set of values
        columns_to_encode: Categorical features to encode
        model_params: Contains boolean parameter on whether to perform encoding
        *datasets: Data sets to perform encoding on

    Returns:
        Data sets with encoded categorical features added
    """

    if model_params["encode_features"]:

        transformed_datasets = []
        encoded_columns = [f"{column}_encoded" for column in columns_to_encode]

        for dataset in datasets:

            # replace values
            for categorical_column, unique_values_set in encoder_dict[
                "categories_dict"
            ].items():
                dataset[categorical_column] = numpy.where(
                    dataset[categorical_column].isin(unique_values_set),
                    dataset[categorical_column],
                    "UNKNOWN",
                )

            dataset[encoded_columns] = pandas.DataFrame(
                encoder_dict["ord_encoder"].transform(dataset[columns_to_encode]),
                columns=encoded_columns,
            )

            transformed_datasets.append(dataset)
    else:
        transformed_datasets = datasets

    return unpack_if_single_element(transformed_datasets)


def create_numerical_scaler(
    columns_to_scale: List[str],
    model_params: Dict[str, Any],
    training_set: pandas.DataFrame,
) -> Union[RobustScaler, dict]:
    """Create numerical feature scaler based on training set

    Args:
        columns_to_scale: Numerical features to scale
        model_params: Contains boolean parameter on whether to perform scaling
        training_set: Training set to create and fit numerical feature scaler on

    Returns:
        Fitted RobustScaler object or empty dict
    """

    scaler = {}

    if model_params["scale_features"]:

        scaler = RobustScaler()

        scaler.fit(training_set[columns_to_scale])

    return scaler


def scale_numerical_features(
    columns_to_scale: List[str],
    scaler: Union[RobustScaler, None],
    model_params: Dict[str, Any],
    *datasets: pandas.DataFrame,
) -> Union[pandas.DataFrame, List[pandas.DataFrame]]:
    """Scales numerical features and add them to the data sets as additional columns

    Args:
        columns_to_scale: Numerical features to scale
        scaler: Fitted RobustScaler object or None
        model_params: Contains boolean parameter on whether to perform scaling
        *datasets: Data sets to perform scaling on

    Returns:
        Data sets with scaled numerical features added
    """

    if model_params["scale_features"]:

        scaled_column_names = [f"{col}_scaled" for col in columns_to_scale]

        for dataset in datasets:
            dataset[scaled_column_names] = pandas.DataFrame(
                scaler.transform(dataset[columns_to_scale])
            )

    return unpack_if_single_element(datasets)


def train_model(
    train_set_features: pandas.DataFrame,
    train_set_target: pandas.Series,
    model_params: Dict[str, Any],
) -> Tuple[
    Union[RandomForestRegressor, RandomForestClassifier],
    List[str],
    numpy.ndarray,
    List[numpy.ndarray],
]:
    """Instantiate model and fit training dataset to model object

    Args:
        train_set_features: Training set features
        train_set_target: Training set target variable
        model_params: Contains model arguments and list of features and target variable names

    Returns:
        - Model fitted with training set
        - List of features from training set
        - Maximum number of leaves out of all trees in ensemble model
        - Indices of training data set which form bootstrap data set for each tree
    """

    model = None
    trees_bootstrap_indices = []

    # instantiate and fit model with training data given model
    if model_params["model_type"] == "regression":
        model = RandomForestRegressor(criterion="mae", **model_params["sklearn_params"])
        logger.info(
            f"\nTraining regression model with {train_set_features.shape[0]} observations"
            f"\nMinimum target: {train_set_target.min()}"
            f"\nMaximum target: {train_set_target.max()}\n"
        )
    elif model_params["model_type"] == "classification":
        model = RandomForestClassifier(
            criterion="gini", **model_params["sklearn_params"]
        )
        logger.info(
            f"\nTraining classification model with {train_set_features.shape[0]} observations"
            f"\nNumber of classes: {train_set_target.nunique()}\n"
        )
    fitted_model = model.fit(train_set_features, train_set_target)
    logger.info("\nModel training complete!\n")

    # log decision trees info
    for num, decision_tree in enumerate(fitted_model.estimators_, 1):
        logger.info(
            f"Decision Tree {num} - Depth: {decision_tree.get_depth()}, "
            f"Number of leaves: {decision_tree.get_n_leaves()}"
        )

        # store bootstrap indices for quick access
        trees_bootstrap_indices.append(decision_tree.bootstrap_indices)

    leaf_indices = fitted_model.apply(train_set_features)
    max_leaf_size = leaf_indices.max() + 1

    # log out of bag score
    if model_params["sklearn_params"]["oob_score"]:
        logger.info(
            f"Out-of-bag score for fitted random forest model: {fitted_model.oob_score_}"
        )

    return (
        fitted_model,
        train_set_features.columns,
        max_leaf_size,
        trees_bootstrap_indices,
    )


def extract_feature_importance(
    fitted_ensemble_model: Union[RandomForestRegressor, RandomForestClassifier],
    train_set_columns: List[str],
) -> pandas.DataFrame:
    """Extract feature importance from fitted tree ensemble model

    Args:
        fitted_ensemble_model: Tree ensemble model fitted with training data
        train_set_columns: List of features to extract importance from

    Returns:
        Feature importance sorted in descending order
    """

    feature_importances_df = pandas.DataFrame(
        {
            "feature": train_set_columns,
            "importance": fitted_ensemble_model.feature_importances_,
        }
    ).sort_values(by="importance", axis=0, ascending=False)

    return feature_importances_df


def predict_outlet_target_var(
    fitted_model: Union[RandomForestRegressor, RandomForestClassifier],
    model_params: Dict[str, Any],
    *features_original_datasets: Union[pandas.DataFrame, pandas.DataFrame],
) -> List[pandas.DataFrame]:
    """Obtain target variable predictions for data sets using fitted model

    Args:
        fitted_model: Model fitted with training set data
        model_params: Model parameters which contain target variable name
        *features_original_datasets:
            - Features-only data sets to pass into the model for prediction (first half)
            - Features + Target + outlet identifier + etc. data sets to merge with output data sets (second half)

    Returns:
        Features with prediction column DataFrames and leaf node indices of predictions DataFrames
    """

    output_df_list = []

    target_variable = (
        f'{model_params["target_variable"]}_binned'
        if model_params["model_type"] == "classification"
        else model_params["target_variable"]
    )

    for idx in range(0, len(features_original_datasets), 2):

        # get features dataset for current loop iteration
        features_dataset = features_original_datasets[idx]

        # create copy of dataframe to append model predictions to for output
        prepared_dataset = features_original_datasets[idx + 1].copy()

        # obtain predictions and assign to new column
        predictions = fitted_model.predict(features_dataset)
        prepared_dataset[f"{target_variable}_prediction"] = predictions

        # get leaf nodes indices
        leaf_node_indices = pandas.DataFrame(fitted_model.apply(features_dataset))

        if model_params["model_type"] == "classification":
            predictions_probas = fitted_model.predict_proba(features_dataset)
            probas_columns = [
                f"{target_variable}_prediction_probas_class_{class_num}"
                for class_num in range(predictions_probas.shape[1])
            ]
            predictions_probas_df = pandas.DataFrame(
                predictions_probas, columns=probas_columns, index=prepared_dataset.index
            )

            prepared_dataset = pandas.concat(
                [prepared_dataset, predictions_probas_df], axis=1
            )

        # extend to output list
        output_df_list.extend([prepared_dataset, leaf_node_indices])

    return output_df_list


def evaluate_predictions(
    model_params: Dict[str, Any],
    features_columns: List[str],
    *predictions_datasets: pandas.DataFrame,
) -> pandas.DataFrame:
    """Obtain evaluation metrics based on target variable predictions and ground truth

    Args:
        model_params: Model parameters which contain target variable name
        features_columns: Feature columns to obtain number of features
        *predictions_datasets: Data sets with prediction values; output of predict_outlet_target_var

    Returns:
        Tabular data of evaluation metrics, number of rows depending on len(predictions_datasets)
    """

    # obtain ground truth and prediction columns
    ground_truth_col = (
        f'{model_params["target_variable"]}_binned'
        if model_params["model_type"] == "classification"
        else model_params["target_variable"]
    )
    prediction_col = f"{ground_truth_col}_prediction"

    # create list of metric output dataframes
    metrics_dataframes = []

    # get number of features (k)
    k = len(features_columns)

    metrics_functions_dict = {}
    auc_functions_dict = {}
    keys = []

    # dictionary of metric functions
    if model_params["model_type"] == "regression":
        metrics_functions_dict = {
            "r-squared": r2_score,
            "rmse": rmse,
            "mae": mean_absolute_error,
            "max_error": max_error,
            "rse": rse,
            "rrse": rrse,
        }
        keys = list(metrics_functions_dict.keys())

    elif model_params["model_type"] == "classification":
        # metrics key: value pair can be added to this dictionary but unit testcase should be modified to account it
        metrics_functions_dict = {}
        auc_functions_dict = {"auc_weighted": auc_average("weighted")}
        keys = list(metrics_functions_dict.keys()) + list(auc_functions_dict.keys())

    for idx, predictions_dataframe in enumerate(predictions_datasets, start=1):

        # obtain ground truths and prediction values
        ground_truths = predictions_dataframe[ground_truth_col]
        predictions = predictions_dataframe[prediction_col]

        # generate metric values based on ground truths and predictions
        metric_values_dict = {
            metric_name: [metric_function(ground_truths, predictions)]
            if "rse" not in metric_name
            else [metric_function(ground_truths, predictions, k=k)]
            for metric_name, metric_function in metrics_functions_dict.items()
        }

        if model_params["model_type"] == "classification":
            probas_columns = [
                f"{prediction_col}_probas_class_{class_num}"
                for class_num in range(
                    model_params["classification_num_target_classes"]
                )
            ]
            predictions_probas = predictions_dataframe[probas_columns]

            auc_values_dict = {
                metric_name: [metric_function(ground_truths, predictions_probas)]
                for metric_name, metric_function in auc_functions_dict.items()
            }

            metric_values_dict = {**metric_values_dict, **auc_values_dict}

            logger.info(
                f"Confusion matrix for dataset number {idx}; row (actual), column (predicted):"
                f"\n{confusion_matrix(ground_truths, predictions)}"
                f'\n{confusion_matrix(ground_truths, predictions, normalize="all")}'
            )

        # metrics dataframe
        metrics_dataframe = pandas.DataFrame(metric_values_dict)
        metrics_dataframe["dataset_num"] = idx

        # append to list of metrics output dataframes
        metrics_dataframes.append(metrics_dataframe)

    concatenated_metrics_dataframe = pandas.concat(
        metrics_dataframes, ignore_index=True
    )

    # arrange output dataframe columns
    concatenated_metrics_dataframe = concatenated_metrics_dataframe[
        ["dataset_num"] + keys
    ]

    return concatenated_metrics_dataframe


def build_training_indices_matrix(
    fitted_model: Union[RandomForestRegressor, RandomForestClassifier],
    max_leaf_size: int,
    trees_bootstrap_indices: List[numpy.ndarray],
    train_set_features: pandas.DataFrame,
) -> pandas.DataFrame:
    """Build training set indices matrix, for quick retrieval of similar outlets that reside in the same leaf node

    Example of usage for output (training indices matrix):
        To access similar outlets training set indices for given leaf index and tree index
        Inputs:
            - Leaf index: 20
            - Tree index: 100
        Output:
            - training_indices_matrix.loc[20, 100] -> similar outlets training set indices

    Args:
        fitted_model: Model fitted with training set
        max_leaf_size: Maximum leaf index out of all trees from fitted ensemble model
        trees_bootstrap_indices: Training set indices of bootstrapped data sets used to build each tree
        train_set_features: Training set features, to be indexed by bootstrapped data set indices

    Returns:
        Pandas DataFrame on [leaf index, tree index] which contains list of training observations indices that fall into
        the same leaf node for a particular tree in the ensemble
    """

    # create training indices matrix (pandas DataFrame [number of trees * max number of leaves in forest])
    # element contains array of training set observation indices that fall into a particular leaf node of the tree
    training_indices_matrix = pandas.DataFrame(index=range(max_leaf_size))

    # loop through trees
    for idx, decision_tree in enumerate(fitted_model.estimators_):

        # obtain bootstrap dataset by indexing trees_bootstrap_indices and indexing train dataset features with indices
        bootstrap_features = train_set_features.loc[trees_bootstrap_indices[idx], :]

        # use tree to get leaf node indices using tree apply and assign to column
        bootstrap_leaf_indices = pandas.DataFrame(
            {
                "bootstrap_index": bootstrap_features.index,
                "leaf_index": decision_tree.apply(bootstrap_features),
            }
        )

        # group by leaf node indices and collect list of bootstrap_indices
        grouped_leaf_indices = (
            bootstrap_leaf_indices.groupby("leaf_index")
            .bootstrap_index.apply(list)
            .sort_index(ascending=True)
        ).rename(idx)

        # assign to training_indices_matrix at indices [tree_index, :num of leaf nodes for tree]
        training_indices_matrix = training_indices_matrix.join(
            grouped_leaf_indices, how="left"
        )

    return training_indices_matrix


def retrieve_similar_outlets_ground_truths(
    predictions_leaf_indices: pandas.DataFrame,
    training_indices_matrix: pandas.DataFrame,
    training_set: pandas.DataFrame,
    prepared_dataset_of_predictions: pandas.DataFrame,
    predictions: pandas.DataFrame,
    model_params: Dict[str, Any],
) -> pandas.DataFrame:
    """Obtain target variable values of bootstrapped training set instances that fall into same leaf node as predictions

    Args:
        predictions_leaf_indices: Leaf indices of predictions, [prediction row index, tree index] format
        training_indices_matrix:
            2D numpy array on [leaf index, tree index] which contains list of training observations
            indices that fall into the same leaf node for a particular tree in the ensemble
        training_set: Training data set which contains ground truths of the target variable for training observations
        prepared_dataset_of_predictions: Contains ground truths of the target variable for prediction observations
        predictions: Contains prediction values
        model_params: Model parameters which contain target variable name

    Returns:
        DataFrame with ground truth cashflow, prediction, list of similar outlets cashflow on outlet_id level
    """

    # obtain corresponding list of training set indices with same leaf node indices
    def retrieve_bootstrap_indices(column_index: int) -> pandas.Series:
        bootstrap_indices = training_indices_matrix.iloc[
            predictions_leaf_indices[column_index].values, column_index
        ]
        return bootstrap_indices.rename(column_index).reset_index(drop=True)

    similar_outlets_bootstrap_indices = pandas.concat(
        map(retrieve_bootstrap_indices, training_indices_matrix.columns), axis=1
    )

    # fill numpy.nans to allow concatenation of lists
    similar_outlets_bootstrap_indices_filled_sum = similar_outlets_bootstrap_indices.applymap(
        lambda cell_value: cell_value if isinstance(cell_value, list) else []
    ).sum(
        axis=1
    )

    # perform unique on retrieved bootstrap indices and retrieve cashflows from indices
    def retrieve_bootstrap_cashflows(
        similar_outlets_bootstrap_indices_list: List[int],
    ) -> List[float]:
        cashflows = training_set.loc[
            set(similar_outlets_bootstrap_indices_list), model_params["target_variable"]
        ]
        sorted_cashflows_list = sorted(cashflows.tolist())
        # trim top 5 percentile of similar outlets cashflows (outliers removal)
        percentile_95th_index = int(len(sorted_cashflows_list) * 0.95)
        return sorted_cashflows_list[:percentile_95th_index]

    bootstrap_cashflows = similar_outlets_bootstrap_indices_filled_sum.map(
        retrieve_bootstrap_cashflows
    )

    # obtain ground truth values of prediction outlets
    ground_truths = prepared_dataset_of_predictions.loc[
        :, ["outlet_id", model_params["target_variable"]]
    ]

    # obtain prediction values of prediction outlets
    predicted_col = (
        f'{model_params["target_variable"]}_binned_prediction'
        if model_params["model_type"] == "classification"
        else f'{model_params["target_variable"]}_prediction'
    )
    predictions = predictions.loc[:, predicted_col]

    # prepare output DataFrame
    output_df = pandas.concat([ground_truths, predictions, bootstrap_cashflows], axis=1)
    output_df.columns = [
        "outlet_id",
        "ground_truth",
        "prediction",
        "training_ground_truths_list",
    ]

    return output_df
