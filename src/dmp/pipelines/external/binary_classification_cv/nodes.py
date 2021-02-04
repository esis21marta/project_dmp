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

from typing import Dict, List, Tuple, Union

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pai
import pandas as pd
import shap
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import KFold, train_test_split

from src.dmp.pipelines.external.common.python.ml_facade import ClassifierFacade
from src.dmp.pipelines.external.common.python.utils import adjust_raw_probability


def enforce_feature_types(
    df: pd.DataFrame, features_used: List[str], categorical_features: List[str]
) -> pd.DataFrame:
    """
    Casts categorical_features to string, casts other features to float.

    :param df: Input DataFrame
    :param features_used: All features used in this run
    :param categorical_features: Columns containing the categorical features
    :return: DataFrame containing a subset of columns
    """
    for col in features_used:
        if col in categorical_features:
            df[col] = df[col].astype(str)
        else:
            df[col] = df[col].astype(float)
    return df


def _gini(
    y_true: pd.Series,
    y_score: pd.Series,
    adjustment_type: str,
    num_bins: int,
    bin_type: str,
) -> float:
    """
    Returns the (adjusted) gini score for two array-like objects.

    :param y_true: Array-like object containing true labels
    :param y_score: Array-like object containing the scores
    :param adjustment_type: Type of adjustment ('raw_proba' or 'avg_raw_proba' or 'woe')
    :param num_bins: Number of bins of scores
    :param bin_type: Type of bins to use ('quantile' or 'equal_size')
    :return: (adjusted) Gini score
    """
    adjusted_y_score = adjust_raw_probability(
        y_true, y_score, adjustment_type, num_bins, bin_type
    )
    return 2 * roc_auc_score(y_true, adjusted_y_score) - 1


def _log_plot_ginis(ginis: List[float]) -> None:
    """
    Creates and logs a plot of gini for every train-test pair.

    :param ginis: Gini values for every train-test pair
    :param time_folds: Start and end labels for every train-test pair
    """
    gini_out_of_time = plt.figure(figsize=(16, 9))
    x = range(len(ginis))
    plt.bar(x=x, height=ginis)
    # plt.xticks(x, time_folds, rotation=90, fontsize=12)
    plt.yticks(fontsize=14)
    plt.ylabel("GINI", fontsize=16)
    plt.tight_layout()

    pai.log_artifacts({"gini CV plot": gini_out_of_time})


def _preprocess_X_y(
    df: pd.DataFrame,
    filter_query: str,
    features_used: List[str],
    target_expression_map: Dict[int, str],
) -> Tuple[pd.DataFrame, pd.Series, pd.Series]:
    """
    Selects rows from the DataFrame using filter_query and computes targets using the target_expression_map, then splits into features, targets and weeks.

    :param df: Input DataFrame
    :param filter_query: Query used to filter down the DataFrame
    :param features_used: All features used in this run
    :param target_expression_map: Mapping of target values to the expressions used to compute them
    """
    X = df[features_used]

    positive = df.eval(target_expression_map[0])
    negative = df.eval(target_expression_map[1])
    if not np.all(positive == ~negative):
        raise AssertionError(
            "Target 0 and target 1 must form a partition of the target column."
        )
    y = negative.map(int)

    return X, y


def _auc(y_true: pd.Series, y_score: pd.Series,) -> float:
    """
    Returns the AUC score

    :param y_true: Array-like object containing true labels
    :param y_score: Array-like object containing the scores
    :return: AUC score
    """
    return roc_auc_score(y_true, y_score)


def train_and_test_split(
    df: pd.DataFrame,
    features_used: List[str],
    categorical_features: List[str],
    train_query: str,
    classifier_params: Dict[str, Union[str, float]],
    target_expression_map: Dict[int, str],
    test_data_ratio: float,
    random_state: int,
) -> None:
    """
    Perform evaluation on training and test data, split by test_data_ration

    :param df: Input DataFrame
    :param features_used: All features used in this run
    :param categorical_features: Columns containing the categorical features
    :param train_query: Query used to filter down the training set
    :param classifier_params: Classifier parameters
    :param target_expression_map: Mapping of target values to the expressions used to compute them for train
    :param test_data_ratio: Ratio of test set
    :param random_state: A random state to make sure that the training and test data split is consistent
    """

    X, y = _preprocess_X_y(df, train_query, features_used, target_expression_map,)

    if len(y.unique()) != 2:
        return

    train_x, test_x, train_y, test_y = train_test_split(
        X, y, test_size=test_data_ratio, random_state=random_state, stratify=y
    )

    model = ClassifierFacade(classifier_params)
    model.fit(train_x, train_y, categorical_features)

    artifacts = {}

    y_score_test = model.predict_proba(test_x)[:, 1]
    auc_score_test = _auc(test_y.values, y_score_test)
    artifacts["auc_score_test"] = auc_score_test

    y_score_train = model.predict_proba(train_x)[:, 1]
    auc_score_train = _auc(train_y.values, y_score_train)
    artifacts["auc_score_train"] = auc_score_train

    pai.log_artifacts(artifacts)

    pai.log_metrics({"AUC Train": auc_score_train, "AUC Test": auc_score_test})


def cross_validate_nfolds(
    df: pd.DataFrame,
    features_used: List[str],
    categorical_features: List[str],
    train_query: str,
    classifier_params: Dict[str, Union[str, float]],
    target_expression_map: str,
    num_folds: int,
    random_state: int,
    gini_params: Dict[str, str],
) -> None:
    """
    Performs a cross-validation on the DataFrame and logs the training and test parameters and results.

    :param df: Input DataFrame
    :param features_used: All features used in this run
    :param categorical_features: Columns containing the categorical features
    :param num_folds: Number of folds in cross-validation
    :param classifier_params: parameters of the model
    :param target_column_name: Column containing the target
    :param num_quantiles: Number of quantiles to plot in confusion matrix
    """

    pai.log_params({"Classifier parameters": classifier_params})
    pai.log_params({"Number of folds": num_folds})

    test_auc_scores = []
    train_auc_scores = []
    test_ginis = []
    train_ginis = []

    X, y = _preprocess_X_y(df, train_query, features_used, target_expression_map,)
    kf = KFold(n_splits=num_folds)

    fold_id = 0

    cross_validation_dataset_basic_stats = []
    stats_columns = ["num_rows", "num_cols"] + ["label_0", "label_1"]

    for in_train, in_test in kf.split(X.values):
        fold_id += 1

        X_train = X.loc[in_train, :]
        y_train = y.loc[in_train]

        X_test = X.loc[in_test, :]
        y_test = y.loc[in_test]

        model = ClassifierFacade(classifier_params)
        model.fit(X_train, y_train, categorical_features)

        # Calculating the performance metrics for the train and test set
        test_auc_scores.append(_auc(y_test.values, model.predict_proba(X_test)[:, 1]))
        train_auc_scores.append(
            _auc(y_train.values, model.predict_proba(X_train)[:, 1])
        )
        train_ginis.append(
            _gini(
                y_train,
                pd.Series(model.predict_proba(X_train)[:, 1], index=y_train.index),
                gini_params["adjustment_type"],
                gini_params["num_bins"],
                gini_params["bin_type"],
            )
        )
        test_ginis.append(
            _gini(
                y_test,
                pd.Series(model.predict_proba(X_test)[:, 1], index=y_test.index),
                gini_params["adjustment_type"],
                gini_params["num_bins"],
                gini_params["bin_type"],
            )
        )

        cross_validation_dataset_basic_stats.extend(
            [
                get_dataset_basic_stats_df(
                    X_train, y_train, stats_columns, "train", f"_fold_{fold_id}",
                ),
                get_dataset_basic_stats_df(
                    X_test, y_test, stats_columns, "test", f"_fold_{fold_id}"
                ),
                # Just to prettify the display
                pd.DataFrame(
                    data=[[""] * len(stats_columns)], columns=stats_columns, index=[""]
                ),
            ]
        )

    if len(test_ginis) > 0:
        pai.log_metrics(
            {
                "Train CV Mean AUC": np.round(np.mean(train_auc_scores), 4),
                "Test CV Mean AUC": np.round(np.mean(test_auc_scores), 4),
                "Train Std CV AUC": np.round(np.std(train_auc_scores), 4),
                "Test Std CV AUC": np.round(np.std(test_auc_scores), 4),
                "Mean GINI": np.mean(test_ginis),
                "SD GINI": np.std(test_ginis),
                "Min GINI": np.min(test_ginis),
                "Max GINI": np.max(test_ginis),
                "Train Mean GINI": np.mean(train_ginis),
            }
        )

        _log_plot_ginis(test_ginis)

        pai.log_artifacts(
            {
                f"auc_of_cross_validations": _plot_metric_cross_validation(
                    test_auc_scores,
                    [f"Fold #{1 + fold_id}" for fold_id in range(num_folds)],
                    "AUC",
                ),
                "basic stats - cross validation dataset": pd.concat(
                    cross_validation_dataset_basic_stats
                ),
            }
        )


def train_on_all(
    df: pd.DataFrame,
    features_used: List[str],
    categorical_features: List[str],
    train_query: str,
    classifier_params: Dict[str, Union[str, float]],
    target_expression_map: Dict[int, str],
    extract_shap_values: bool,
) -> None:
    """
    Perform evaluation on training and test data, split by test_data_ration

    :param df: Input DataFrame
    :param features_used: All features used in this run
    :param categorical_features: Columns containing the categorical features
    :param train_query: Query used to filter down the training set
    :param classifier_params: Classifier parameters
    :param target_expression_map: Mapping of target values to the expressions used to compute them for train
    :param extract_shap_values: To extract or not the Shapley value of features
    """

    X, y = _preprocess_X_y(df, train_query, features_used, target_expression_map,)

    if len(y.unique()) != 2:
        return

    model = ClassifierFacade(classifier_params)
    model.fit(X, y, categorical_features)

    pai.log_model(model)

    importances = model.get_feature_importance()

    pai.log_features(
        features=importances["Feature Id"], importance=importances["Importances"]
    )

    feature_sorted_by_importance_desc_list = sorted(
        zip(importances["Feature Id"], importances["Importances"]),
        key=lambda x: x[1],
        reverse=True,
    )
    feature_sorted_by_importance_desc_list = [
        feature for feature, _ in feature_sorted_by_importance_desc_list
    ]

    artifacts = {
        "feature_column_name_list": features_used,
        "feature_sorted_by_importance_desc_list": feature_sorted_by_importance_desc_list,
    }

    if extract_shap_values:
        shap_values = model.get_shap(X, y, categorical_features)
        if shap_values is not None:
            feature_effects = plt.figure()
            shap.summary_plot(shap_values, X, show=False, max_display=len(X.columns))
            plt.tight_layout()
            artifacts["feature_effects"] = feature_effects

    pai.log_artifacts(artifacts)

    # test on the test set
    pai.log_metrics(
        {"Train Set AUC": np.round(_auc(y.values, model.predict_proba(X)[:, 1]), 2)}
    )


def get_dataset_basic_stats_df(
    X: pd.DataFrame,
    y: pd.Series,
    stats_columns: List[str],
    prefix_tag: str,
    suffix_tag: str,
):
    """
    Returns s DataFrame containing the basic statistics about the dataset

    :param X: DataFrame representing a dataset
    :param y: Pandas Series containing the targets
    :param bins: number of bins to use for the targets (continuous) values
    :param stats_columns: List of column names
    :param prefix_tag: Prefix tag  to use
    :param suffix_tag: Suffix tag  to use
    :return:
        A DataFrame containing the basic stats about the dataset
    """
    y_stats = y.value_counts()
    stats_df = pd.DataFrame(
        data=[
            ["", ""] + list(y_stats.index),
            list(X.shape) + [v for v in y_stats.values],
        ],
        columns=stats_columns,
        index=[
            f"{prefix_tag}_target_segments{suffix_tag}",
            f"{prefix_tag}_statistics{suffix_tag}",
        ],
    )

    return stats_df


def _plot_metric_cross_validation(
    metric_values: List[float], fold_ids: List[str], metric_name: str
) -> mpl.figure.Figure:
    """
    Creates and returns a plot of metric value for every train-test pair.

    :param metric_values: Metric values for every train-test pair (in the cross-validation)
    :param fold_ids: List of fold_ids
    :param metric_name: metric name
    """
    rmse_of_cross_validations = plt.figure(figsize=(12, 7.5))
    x = range(len(metric_values))
    plt.scatter(x=x, y=metric_values)
    plt.xticks(x, fold_ids, rotation="vertical", fontsize=8)
    plt.yticks(fontsize=10)
    plt.ylabel(metric_name, fontsize=15)
    plt.tight_layout()

    return rmse_of_cross_validations
