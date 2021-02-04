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

from math import ceil, sqrt
from typing import Dict, List, Union

import catboost
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pai
import pandas as pd
import seaborn as sns
import shap
from sklearn.metrics import confusion_matrix, mean_squared_error, r2_score
from sklearn.model_selection import KFold


def remove_missing_targets(df: pd.DataFrame, target_column_name: str) -> pd.DataFrame:
    """
    Removes the rows where the value of the target column is missing or contains +/- infinity

    :param df: Input DataFrame
    :param target_column_name: Column containing the target
    :return: DataFrame without rows with +/- infinity or missing target column values
    """
    df[target_column_name] = df[target_column_name].replace([np.inf, -np.inf], np.nan)
    return df[df[target_column_name].notnull()]


def replace_large_targets_with_median(
    df: pd.DataFrame, target_column_name: str, replace_targets_above: float
) -> pd.DataFrame:
    """
    Replaces targets greater than or equal to replace_targets_above by their median.

    :param df: Input DataFrame
    :param target_column_name: Column containing the target
    :param replace_targets_above: Cutoff value above or at which the targets are replaced
    :return: DataFrame with targets greater than or equal to replace_targets_above replaced by their median
    """
    large_targets = df[target_column_name] >= replace_targets_above
    df.loc[large_targets, target_column_name] = df.loc[
        large_targets, target_column_name
    ].median()
    return df


def take_log_of_target(df: pd.DataFrame, target_column_name: str) -> pd.DataFrame:
    """
    Takes the log of the target column.

    :param df: Input DataFrame
    :param target_column_name: Column containing the target
    :return: DataFrame with the log target
    """

    df.loc[(df[target_column_name] <= 1), target_column_name] = 1

    df[target_column_name] = (
        df[target_column_name].apply(np.log).replace([np.inf, -np.inf], np.nan)
    )
    return df[df[target_column_name].notnull()]


def select_columns_of_interest(
    df: pd.DataFrame, features_used: List[str], target_column_name: str
) -> pd.DataFrame:
    """
    Keeps only target and features_used columns.

    :param df: Input DataFrame
    :param features_used: All features used in this run
    :param target_column_name: Column containing the target
    :return: DataFrame containing a subset of columns
    """
    to_keep = features_used + [target_column_name]
    return df[to_keep]


def enforce_feature_types(
    df: pd.DataFrame, features_used: List[str], categorical_features: List[str]
) -> pd.DataFrame:
    """
    Casts categorical_features to string, casts other features to float.

    :param df: Input DataFrame
    :param features_used: All input features used in this run
    :param categorical_features: Columns containing the categorical features
    :return: DataFrame containing a subset of columns
    """
    for col in features_used:
        if col in categorical_features:
            df[col] = df[col].astype(str)
        else:
            df[col] = df[col].astype(float)
    return df


def categorical_none_to_nan_string(
    df: pd.DataFrame, categorical_features: List[str]
) -> pd.DataFrame:
    """
    Returns a DataFrame in which categorical missing values have been handled.

    :param df: Input DataFrame
    :param categorical_features: Columns containing the categorical features
    :return: DataFrame in which categorical missing values have been handled.
    """
    df[categorical_features] = df[categorical_features].fillna("nan")
    return df


def cross_validate_nfolds(
    df: pd.DataFrame,
    features_used: List[str],
    categorical_features: List[str],
    num_folds: int,
    catboost_params: Dict[str, Union[str, float]],
    target_column_name: str,
    num_quantiles: int = -1,
) -> None:
    """
    Performs a cross-validation on the DataFrame and logs the training and test parameters and results.

    :param df: Input DataFrame
    :param features_used: All features used in this run
    :param categorical_features: Columns containing the categorical features
    :param num_folds: Number of folds in cross-validation
    :param catboost_params: parameters of catboost
    :param target_column_name: Column containing the target
    :param num_quantiles: Number of quantiles to plot in confusion matrix
    """

    pai.log_params({"CatBoost parameters": catboost_params})
    pai.log_params({"Number of folds": num_folds})

    test_rmse_scores = []
    test_r2_scores = []
    test_mape_scores = []
    train_r2_scores = []

    df = df.reset_index(drop=True)
    X = df[features_used]
    y = df[target_column_name]

    kf = KFold(n_splits=num_folds)

    fold_id = 0
    cv_y_test_true = {}
    cv_y_test_pred = {}

    cross_validation_dataset_basic_stats = []
    bins = 10
    stats_columns = ["num_rows", "num_cols"] + [
        f"target_bin_{i}" for i in range(1, bins + 1)
    ]

    for in_train, in_test in kf.split(X.values):
        fold_id += 1

        X_train = X.loc[in_train, :]
        y_train = y.loc[in_train]

        X_test = X.loc[in_test, :]
        y_test = y.loc[in_test]

        model = _train_model(X_train, y_train, categorical_features, catboost_params)

        # Calculating the performance metrics for the train set
        y_train_exp = _apply_exp(y_train)
        train_r2_scores.append(
            r2_score(_apply_exp(y_train), _apply_exp(_get_predictions(X_train, model)))
        )

        # Applying the exp to transform both the predicted and actual target values
        y_test_exp = _apply_exp(y_test)
        y_test_pred_exp = _apply_exp(_get_predictions(X_test, model))

        # Calculating the performance metrics for the test set
        test_rmse_scores.append(_rmse(y_test_exp, y_test_pred_exp))
        test_r2_scores.append(r2_score(y_test_exp, y_test_pred_exp))
        test_mape_scores.append(_mape(y_test_exp, y_test_pred_exp))

        cv_y_test_true[fold_id] = y_test_exp
        cv_y_test_pred[fold_id] = y_test_pred_exp

        cross_validation_dataset_basic_stats.extend(
            [
                get_dataset_basic_stats_df(
                    X_train,
                    y_train_exp,
                    bins,
                    stats_columns,
                    "train",
                    f"_fold_{fold_id}",
                ),
                get_dataset_basic_stats_df(
                    X_test, y_test_exp, bins, stats_columns, "test", f"_fold_{fold_id}"
                ),
                # Just to prettify the display
                pd.DataFrame(
                    data=[[""] * len(stats_columns)], columns=stats_columns, index=[""]
                ),
            ]
        )

    pai.log_metrics(
        {
            "Mean MAPE": np.round(np.mean(test_mape_scores), 1),
            "Mean RMSE": np.round(np.mean(test_rmse_scores), 1),
            "STD RMSE": np.round(np.std(test_rmse_scores), 1),
            "Mean R2": np.mean(test_r2_scores),
            "STD R2": np.std(test_r2_scores),
            "Train Mean R2": np.round(np.mean(train_r2_scores), 1),
        }
    )

    pai.log_artifacts(
        {
            "confusion matrix (quantiles)": _get_confusion_matrix(
                cv_y_test_true, cv_y_test_pred, quantile=num_quantiles
            ),
            "actual - prediction distribution": _dist_err_plot(
                cv_y_test_true, cv_y_test_pred
            ),
            "residual plot": _residual_plot(cv_y_test_true, cv_y_test_pred),
            "actual - prediction scatter plot": _actual_pred_scatter_plot(
                cv_y_test_true, cv_y_test_pred
            ),
            "actual - prediction box plot": _actual_pred_box_plot(
                cv_y_test_true, cv_y_test_pred, num_quantiles=num_quantiles
            ),
            f"r2_of_cross_validations": _plot_metric_cross_validation(
                test_r2_scores,
                [f"Fold #{1 + fold_id}" for fold_id in range(num_folds)],
                "R2",
            ),
            "basic stats - cross validation dataset": pd.concat(
                cross_validation_dataset_basic_stats
            ),
        }
    )


def train_on_all_data(
    df: pd.DataFrame,
    features_used: List[str],
    categorical_features: List[str],
    catboost_params: Dict[str, Union[str, float]],
    target_column_name: str,
) -> None:
    """
    Trains the model on all the dataset, logs feature importances and shap summary plot.

    :param df: Input DataFrame
    :param features_used: All features used in this run
    :param categorical_features: Columns containing the categorical features
    :param catboost_params: parameters of catboost
    :param target_column_name: Column containing the target
    """

    X = df[features_used]
    y = df[target_column_name]

    model = _train_model(X, y, categorical_features, catboost_params)

    pai.log_model(model)

    pool = catboost.Pool(data=X, label=y, cat_features=categorical_features)

    importances = model.get_feature_importance(pool, prettified=True)

    pai.log_features(
        features=importances["Feature Id"], importance=importances["Importances"]
    )

    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(pool)

    feature_effects = plt.figure()
    shap.summary_plot(shap_values, X, show=False, max_display=len(X.columns))
    plt.tight_layout()

    bins = 10
    stats_columns = ["num_rows", "num_cols"] + [
        f"target_bin_{i}" for i in range(1, bins + 1)
    ]

    pai.log_artifacts(
        {
            "feature_effects": feature_effects,
            "feature_column_name_list": features_used,
            "basic stats - final training set": get_dataset_basic_stats_df(
                X, _apply_exp(y), bins, stats_columns, "train", ""
            ),
        }
    )


def _train_model(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    categorical_features: List[str],
    catboost_params: Dict[str, Union[str, float]],
) -> catboost.CatBoostRegressor:
    """
    Trains a catboost Regressor model.

    :param X_train: Features
    :param y_train: Targets
    :param categorical_features: Columns containing the categorical features
    :param catboost_params: dictionary of catboost params
    :return: Trained Regressor model
    """
    training_pool = catboost.Pool(
        data=X_train, label=y_train, cat_features=categorical_features
    )

    params = {
        **{"loss_function": "RMSE", "verbose": False, "allow_writing_files": False},
        **catboost_params,
    }

    model = catboost.CatBoostRegressor(**params)
    model.fit(training_pool)
    return model


def _get_predictions(
    X_test: pd.DataFrame, model: catboost.CatBoostRegressor
) -> pd.Series:
    """
    Returns the predictions for a given model and DataFrame
    :param X_test: Features
    :param model: Model
    :return: Pandas Series containing the predictions
    """
    predictions = pd.Series(model.predict(X_test), index=X_test.index)
    return predictions


def _apply_exp(y_vals: pd.Series) -> pd.Series:
    """
    Applies the exponential to the Series of values
    :param y_vals: Input Series of floats
    :return: Input Series of floats (after applying the exponential)
    """
    return y_vals.apply(np.exp)


def _rmse(y_true: pd.Series, y_pred: pd.Series) -> float:
    """
    Returns the Root Mean Square Error

    :param y_true: Series of floats (Actual value)
    :param y_pred: Series of floats (Predicted values)
    :return: Root Mean Square Error
    """
    return np.sqrt(mean_squared_error(y_true, y_pred))


def _mape(y_true: pd.Series, y_pred: pd.Series) -> float:
    """
       Returns the Mean Absolute Percentage Error

       :param y_true: Series of floats (Actual value)
       :param y_pred: Series of floats (Predicted values)
       :return: Mean Absolute Percentage Error
       """
    return np.mean(np.abs((y_true.values - y_pred.values) / y_true.values)) * 100


def _get_quantile_index(elt: float, quantile_values: List[float]) -> int:
    """
    Returns the quantile index of an element given the list of the quantile values

    :param elt: element for which the quantile index will be calculated
    :param quantile_values: List of the quantile values
    :return: Quantile index
    """
    num_quantile_values = len(quantile_values)
    for i in range(num_quantile_values):
        if elt <= quantile_values[i]:
            return i
    return num_quantile_values


def _get_quantile_label(quantile_index: int, quantile_values: List[float]) -> str:
    """
    Returns a Quantile label associated to the quantile index, given the list of the quantile values

    :param quantile_index: Quantile index
    :param quantile_values: List of the quantile values
    :return: Quantile label
    """
    quantile_values_in_millions = ["%.1fe6" % (e / 1_000_000) for e in quantile_values]

    if quantile_index == 0:
        return "< %s" % quantile_values_in_millions[0]

    if quantile_index == len(quantile_values):
        return "%s+" % quantile_values_in_millions[-1]

    return "%s - %s" % (
        quantile_values_in_millions[quantile_index - 1],
        quantile_values_in_millions[quantile_index],
    )


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


def _get_confusion_matrix(
    cv_y_true: Dict[int, pd.Series], cv_y_pred: Dict[int, pd.Series], quantile: int = -1
) -> mpl.figure.Figure:

    """
    Returns the plotted confusion matrix

    :param cv_y_true: Series of floats (Actual value) for each fold of cross-validation
    :param cv_y_pred: Series of floats (Predicted values) for each fold of cross-validation
    :param quantile: Number of quantiles to plot (-1 means no quantile)
    :return: Figure object of the confusion matrix plotted
    """

    fold_ids = list(cv_y_true.keys())
    n_folds = len(fold_ids)
    ncols = ceil(sqrt(n_folds))
    nrows = ncols

    fig, axes = plt.subplots(
        nrows=nrows,
        ncols=ncols,
        figsize=(16, 9),
        constrained_layout=True,
        squeeze=False,
    )
    fig.suptitle("Confusion Matrix")

    fold_id_index = -1
    for row_id in range(nrows):
        for col_id in range(ncols):
            fold_id_index += 1
            if fold_id_index >= n_folds:
                break

            fold_id = fold_ids[fold_id_index]
            y_true = cv_y_true[fold_id]
            y_pred = cv_y_pred[fold_id]

            title = f"Fold #{fold_id}"
            if quantile <= 1:
                y_true_cleaned = y_true.apply(np.floor)
                y_pred_cleaned = y_pred.apply(np.floor)

                index_labels = sorted(
                    list(
                        set(
                            y_true_cleaned.unique().tolist()
                            + y_pred_cleaned.unique().tolist()
                        )
                    )
                )
                labels = [str(i) for i in index_labels]
            else:
                all_y_vals = y_true.tolist() + y_pred.tolist()

                quantile_values = np.quantile(
                    all_y_vals, [e / quantile for e in range(1, quantile)]
                )

                y_true_cleaned = y_true.apply(
                    _get_quantile_index, args=(quantile_values,)
                )
                y_pred_cleaned = y_pred.apply(
                    _get_quantile_index, args=(quantile_values,)
                )

                index_labels = range(quantile)
                labels = [_get_quantile_label(i, quantile_values) for i in index_labels]
                title = f"{title} (Quantile)"

            matrix = confusion_matrix(
                y_true_cleaned, y_pred_cleaned, labels=index_labels, normalize="true"
            )

            sns.heatmap(
                pd.DataFrame(np.round(matrix, 2), index=labels, columns=labels),
                annot=True,
                fmt="g",
                ax=axes[row_id][col_id],
            )
            axes[row_id][col_id].set_title(title)
            axes[row_id][col_id].set_xlabel("Prediction")
            axes[row_id][col_id].set_ylabel("True Value")
            axes[row_id][col_id].invert_yaxis()

    return fig


def _dist_err_plot(
    cv_y_true: Dict[int, pd.Series], cv_y_pred: Dict[int, pd.Series], kde: bool = True
) -> mpl.figure.Figure:
    """
    Returns the plotted graph of the evolution of y_true and y_pred for each fold of cross-validation

    :param cv_y_true: Series of floats (actual values) for each fold of cross-validation
    :param cv_y_pred: Series of floats (predicted values) for each fold of cross-validation
    :param kde: Boolean for plotting the Kernel Distribution Estimates
    :return: Figure object of the graph plotted
    """

    fold_ids = list(cv_y_true.keys())
    n_folds = len(fold_ids)
    ncols = ceil(sqrt(n_folds))
    nrows = ncols

    fig, axes = plt.subplots(
        nrows=nrows,
        ncols=ncols,
        figsize=(16, 9),
        constrained_layout=True,
        squeeze=False,
    )
    fig.suptitle("Actual - Prediction Distribution")

    fold_id_index = -1
    for row_id in range(nrows):
        for col_id in range(ncols):
            fold_id_index += 1
            if fold_id_index >= n_folds:
                break

            fold_id = fold_ids[fold_id_index]
            y_true = cv_y_true[fold_id]
            y_pred = cv_y_pred[fold_id]

            sns.distplot(y_true, label="Actual", kde=kde, ax=axes[row_id][col_id])
            sns.distplot(y_pred, label="Predicted", kde=kde, ax=axes[row_id][col_id])
            axes[row_id][col_id].set_title(f"Fold #{fold_id}")
            axes[row_id][col_id].legend()

    return fig


def _residual_plot(
    cv_y_true: Dict[int, pd.Series], cv_y_pred: Dict[int, pd.Series]
) -> mpl.figure.Figure:
    """
    Returns the residual plot of y_true and y_pred for each fold of cross-validation

    :param cv_y_true: Series of floats (actual values) for each fold of cross-validation
    :param cv_y_pred: Series of floats (predicted values) for each fold of cross-validation
    :return: Figure object of the graph plotted
    """
    fold_ids = list(cv_y_true.keys())
    n_folds = len(fold_ids)
    ncols = ceil(sqrt(n_folds))
    nrows = ncols

    fig, axes = plt.subplots(
        nrows=nrows,
        ncols=ncols,
        figsize=(16, 9),
        constrained_layout=True,
        squeeze=False,
    )
    fig.suptitle("Residual Plot")

    fold_id_index = -1
    for row_id in range(nrows):
        for col_id in range(ncols):
            fold_id_index += 1
            if fold_id_index >= n_folds:
                break

            fold_id = fold_ids[fold_id_index]
            y_true = cv_y_true[fold_id]
            y_pred = cv_y_pred[fold_id]

            y_true_values = y_true.values
            residuals = y_true_values - y_pred.values

            axes[row_id][col_id].scatter(y_true_values, residuals, alpha=0.5, c="green")
            axes[row_id][col_id].set_title(f"Fold #{fold_id}")
            axes[row_id][col_id].set_xlabel("Actual Income")
            axes[row_id][col_id].set_ylabel("Residuals (Actual - Prediction)")

    return fig


def _actual_pred_scatter_plot(
    cv_y_true: Dict[int, pd.Series], cv_y_pred: Dict[int, pd.Series]
) -> mpl.figure.Figure:
    """
    Returns the scatter plot of y_true and y_pred for each fold of cross-validation

    :param cv_y_true: Series of floats (actual values) for each fold of cross-validation
    :param cv_y_pred: Series of floats (predicted values) for each fold of cross-validation
    :return: Figure object of the graph plotted
    """

    fold_ids = list(cv_y_true.keys())
    n_folds = len(fold_ids)
    ncols = ceil(sqrt(n_folds))
    nrows = ncols

    fig, axes = plt.subplots(
        nrows=nrows,
        ncols=ncols,
        figsize=(16, 9),
        constrained_layout=True,
        squeeze=False,
    )
    fig.suptitle("Actual - Prediction Scatter Plot")

    fold_id_index = -1
    for row_id in range(nrows):
        for col_id in range(ncols):
            fold_id_index += 1
            if fold_id_index >= n_folds:
                break

            fold_id = fold_ids[fold_id_index]
            y_true = cv_y_true[fold_id]
            y_pred = cv_y_pred[fold_id]

            axes[row_id][col_id].scatter(y_true, y_pred, alpha=0.5, c="green")
            axes[row_id][col_id].set_title(f"Fold #{fold_id}")
            axes[row_id][col_id].set_xlabel("Actual Income")
            axes[row_id][col_id].set_ylabel("Predicted Income")

    return fig


def _actual_pred_box_plot(
    cv_y_true: Dict[int, pd.Series], cv_y_pred: Dict[int, pd.Series], num_quantiles=10
) -> mpl.figure.Figure:
    """
    Returns the box plot of y_true and y_pred for each fold of cross-validation

    :param cv_y_true: Series of floats (actual values) for each fold of cross-validation
    :param cv_y_pred: Series of floats (predicted values) for each fold of cross-validation
    :param num_quantiles: Number of quantiles for the box plot
    :return: Figure object of the graph plotted
    """

    fold_ids = list(cv_y_true.keys())
    n_folds = len(fold_ids)
    ncols = ceil(sqrt(n_folds))
    nrows = ncols

    fig, axes = plt.subplots(
        nrows=nrows,
        ncols=ncols,
        figsize=(16, 9),
        constrained_layout=True,
        squeeze=False,
    )
    fig.suptitle("Actual - Prediction Box Plot")

    fold_id_index = -1
    for row_id in range(nrows):
        for col_id in range(ncols):
            fold_id_index += 1
            if fold_id_index >= n_folds:
                break

            fold_id = fold_ids[fold_id_index]
            y_true = cv_y_true[fold_id]
            y_pred = cv_y_pred[fold_id]

            quantile_values = np.quantile(
                y_true.values, [e / num_quantiles for e in range(1, num_quantiles)]
            )

            y_true_quantile_index = y_true.apply(
                _get_quantile_index, args=(quantile_values,)
            ).values

            y_pred = y_pred.values

            data = [[] for _ in range(num_quantiles)]
            for i in range(len(y_true_quantile_index)):
                y = y_true_quantile_index[i]
                data[y].append(y_pred[i])

            labels = [
                _get_quantile_label(i, quantile_values) for i in range(num_quantiles)
            ]

            axes[row_id][col_id].boxplot(data, labels=labels)
            axes[row_id][col_id].set_title(f"Fold #{fold_id}")
            axes[row_id][col_id].set_xlabel("Actual Income")
            axes[row_id][col_id].set_ylabel("Predicted Income")

    return fig


def get_dataset_basic_stats_df(
    X: pd.DataFrame,
    y: pd.Series,
    bins: int,
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
    y_stats = y.value_counts(normalize=True, bins=bins)
    stats_df = pd.DataFrame(
        data=[
            ["", ""] + list(y_stats.index),
            list(X.shape) + [f"{np.round(100 * v, 2)}%" for v in y_stats.values],
        ],
        columns=stats_columns,
        index=[
            f"{prefix_tag}_target_segments{suffix_tag}",
            f"{prefix_tag}_statistics{suffix_tag}",
        ],
    )

    return stats_df
