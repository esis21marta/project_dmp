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

import catboost
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pai
import pandas as pd
import seaborn as sns
import shap
from sklearn.calibration import calibration_curve
from sklearn.metrics import roc_auc_score
from tqdm import tqdm

from src.dmp.pipelines.external.common.python.ml_facade import ClassifierFacade
from src.dmp.pipelines.external.common.python.utils import (
    add_empty_rows,
    adjust_raw_probability,
    filter_down,
    get_lift_analysis_df,
    get_score_adjustment_stats,
)

mpl.use("Agg")


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


def _log_plot_ginis_out_of_time(ginis: List[float], time_folds: List[str]) -> None:
    """
    Creates and logs a plot of gini for every train-test pair.

    :param ginis: Gini values for every train-test pair
    :param time_folds: Start and end labels for every train-test pair
    """
    gini_out_of_time = plt.figure(figsize=(16, 9))
    x = range(len(ginis))
    plt.bar(x=x, height=ginis)
    plt.xticks(x, time_folds, rotation=90, fontsize=12)
    plt.yticks(fontsize=14)
    plt.ylabel("GINI", fontsize=16)
    plt.tight_layout()

    pai.log_artifacts({"gini out of time plot": gini_out_of_time})


def _train_model(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    categorical_features: List[str],
    catboost_params: Dict,
) -> catboost.CatBoostClassifier:
    """
    Trains a catboost classifier model.

    :param X_train: Features
    :param y_train: Targets
    :param categorical_features: Columns containing the categorical features
    :param catboost_params: parameters of catboost
    :return: Trained classification model
    """
    training_pool = catboost.Pool(
        data=X_train, label=y_train, cat_features=categorical_features
    )

    params = {
        **{"loss_function": "Logloss", "verbose": False, "allow_writing_files": False},
        **catboost_params,
    }

    model = catboost.CatBoostClassifier(**params)
    model.fit(training_pool)
    return model


def _preprocess_X_y_weeks(
    df: pd.DataFrame,
    filter_query: str,
    features_used: List[str],
    target_expression_map: Dict[int, str],
    weekstart_column_name: str,
) -> Tuple[pd.DataFrame, pd.Series, pd.Series]:
    """
    Selects rows from the DataFrame using filter_query and computes targets using the target_expression_map, then splits into features, targets and weeks.

    :param df: Input DataFrame
    :param filter_query: Query used to filter down the DataFrame
    :param features_used: All features used in this run
    :param target_expression_map: Mapping of target values to the expressions used to compute them
    :param weekstart_column_name: Column containing the start of the week
    """
    df = filter_down(df, filter_query)
    X = df[features_used]
    weeks = df[weekstart_column_name]

    good = df.eval(target_expression_map[0])
    bad = df.eval(target_expression_map[1])
    if not np.all(good == ~bad):
        raise AssertionError(
            "Target 0 and target 1 must form a partition of the target column."
        )
    y = bad.map(int)

    return X, y, weeks


def validate_out_of_time(
    df: pd.DataFrame,
    features_used: List[str],
    categorical_features: List[str],
    train_query: str,
    test_query: str,
    weeks_train: int,
    weeks_test: int,
    classifier_params: Dict[str, Union[str, float]],
    weekstart_column_name: str,
    train_target_expression_map: Dict[int, str],
    test_target_expression_map: Dict[int, str],
    weeks_gap: int,
    reliability_diagrams_num_bins: int,
    gini_params: Dict[str, str],
) -> None:
    """
    Performs out-of-time validation by training on the first weeks_train, testing on the following weeks_test, then moving forward by one week.
    Logs the training and validation parameters and results.

    :param df: Input DataFrame
    :param features_used: All features used in this run
    :param categorical_features: Columns containing the categorical features
    :param train_query: Query used to filter down the training set
    :param test_query: Query used to filter down the test set
    :param weeks_train: Weeks for each training set
    :param weeks_test: Weeks for each test set
    :param classifier_params: classifier parameters
    :param train_target_expression_map: Mapping of target values to the expressions used to compute them for train
    :param test_target_expression_map: Mapping of target values to the expressions used to compute them for test
    :param weeks_gap: Weeks gap between end of train and start of test
    :param gini_params: Parameters used when calculating Ginis
    :param reliability_diagrams_num_bins: Number of bins to used when plotting the reliability diagrams
    """

    pai.log_params({"Classifier parameters": classifier_params})
    pai.log_params({"Training weeks": weeks_train})
    pai.log_params({"Test weeks": weeks_test})
    pai.log_params({"weeks Gap": weeks_gap})

    train_ginis = []
    test_ginis = []
    time_folds = []

    train_all_actual_values = pd.Series([])
    train_all_predicted_values = pd.Series([])

    test_all_actual_values = pd.Series([])
    test_all_predicted_values = pd.Series([])

    last_actual_values = pd.Series([])
    last_predicted_values = pd.Series([])

    cross_validation_dataset_basic_stats = []
    lift_analyses = []

    unique_weeks = sorted(df[weekstart_column_name].unique())

    for fold_id, week_start in enumerate(
        tqdm(
            unique_weeks[weeks_train : -(weeks_test + weeks_gap)],
            desc="Out of time validation",
        ),
        start=1,
    ):
        in_train = (df[weekstart_column_name] <= week_start) & (
            df[weekstart_column_name] >= week_start - pd.Timedelta(weeks=weeks_train)
        )

        in_test = (
            df[weekstart_column_name] > week_start + pd.Timedelta(weeks=weeks_gap)
        ) & (
            df[weekstart_column_name]
            <= week_start + pd.Timedelta(weeks=weeks_test + weeks_gap)
        )

        X_train, y_train, train_weeks = _preprocess_X_y_weeks(
            df[in_train],
            train_query,
            features_used,
            train_target_expression_map,
            weekstart_column_name,
        )
        X_test, y_test, test_weeks = _preprocess_X_y_weeks(
            df[in_test],
            test_query,
            features_used,
            test_target_expression_map,
            weekstart_column_name,
        )

        if len(y_train.unique()) == 2 and len(y_test.unique()) == 2:

            time_folds.append(
                f"Train: {train_weeks.min()} to {train_weeks.max()}\nTest: {test_weeks.min()} to {test_weeks.max()}"
            )

            model = ClassifierFacade(classifier_params)
            model.fit(X_train, y_train, categorical_features)

            y_pred_train = pd.Series(
                model.predict_proba(X_train)[:, 1], index=y_train.index
            )
            y_pred_test = pd.Series(
                model.predict_proba(X_test)[:, 1], index=y_test.index
            )

            test_lift_df = get_lift_analysis_df(
                y_test,
                y_pred_test,
                num_bins=reliability_diagrams_num_bins,
                prettify=True,
            )
            test_lift_df.insert(0, "end_week", test_weeks.max())
            test_lift_df.insert(0, "start_week", test_weeks.min())
            test_lift_df.index = [f"Fold_{fold_id}"] * test_lift_df.shape[0]
            lift_analyses.append(add_empty_rows(test_lift_df))

            train_ginis.append(
                _gini(
                    y_train,
                    y_pred_train,
                    gini_params["adjustment_type"],
                    gini_params["num_bins"],
                    gini_params["bin_type"],
                )
            )
            test_ginis.append(
                _gini(
                    y_test,
                    y_pred_test,
                    gini_params["adjustment_type"],
                    gini_params["num_bins"],
                    gini_params["bin_type"],
                )
            )

            train_all_actual_values = train_all_actual_values.append(y_train)
            train_all_predicted_values = train_all_predicted_values.append(y_pred_train)

            test_all_actual_values = test_all_actual_values.append(y_test)
            test_all_predicted_values = test_all_predicted_values.append(y_pred_test)

            last_actual_values = y_test
            last_predicted_values = y_pred_test

        cross_validation_dataset_basic_stats.append(
            _get_dataset_basic_stats_df(
                X_train, y_train, train_weeks.min(), train_weeks.max(), "train"
            )
        )

        cross_validation_dataset_basic_stats.append(
            add_empty_rows(
                _get_dataset_basic_stats_df(
                    X_test, y_test, test_weeks.min(), test_weeks.max(), "test"
                )
            )
        )

    if len(test_ginis) > 0:
        pai.log_metrics(
            {
                "Mean GINI": np.mean(test_ginis),
                "SD GINI": np.std(test_ginis),
                "Min GINI": np.min(test_ginis),
                "Max GINI": np.max(test_ginis),
                "Train Mean GINI": np.mean(train_ginis),
            }
        )

        _log_plot_ginis_out_of_time(test_ginis, time_folds)

        pai.log_artifacts(
            {
                "reliability diagrams - all test data": _plt_reliability_diagrams(
                    test_all_actual_values,
                    test_all_predicted_values,
                    num_bins=reliability_diagrams_num_bins,
                    normalize=False,
                ),
                "reliability diagrams - last test data": _plt_reliability_diagrams(
                    last_actual_values,
                    last_predicted_values,
                    num_bins=reliability_diagrams_num_bins,
                    normalize=False,
                ),
                "basic stats - cross validation dataset": pd.concat(
                    cross_validation_dataset_basic_stats
                ),
                "lift analyses data": pd.concat(lift_analyses),
                "lift curves": _plt_lift_graphs(
                    last_actual_values, last_predicted_values, num_bins=50
                ),
                "score distribution": _plt_score_distribution(
                    train_all_actual_values,
                    train_all_predicted_values,
                    test_all_actual_values,
                    test_all_predicted_values,
                    train_target_expression_map,
                    test_target_expression_map,
                    num_bins=50,
                ),
                "weight of evidence": _plt_weight_of_evidence(
                    test_all_actual_values,
                    test_all_predicted_values,
                    num_bins=gini_params["num_bins"],
                ),
            }
        )


def train_on_last_weeks(
    df: pd.DataFrame,
    features_used: List[str],
    categorical_features: List[str],
    train_query: str,
    weeks_train: int,
    classifier_params: Dict[str, Union[str, float]],
    weekstart_column_name: str,
    train_target_expression_map: Dict[int, str],
    extract_shap_values: bool,
    log_train_data: bool,
) -> None:
    """
    Trains the model on the last weeks_train weeks, logs feature importances and shap summary plot.

    :param df: Input DataFrame
    :param features_used: All features used in this run
    :param categorical_features: Columns containing the categorical features
    :param train_query: Query used to filter down the training set
    :param weeks_train: Weeks for each training set
    :param classifier_params: Classifier parameters
    :param weekstart_column_name: Column containing the start of the week
    :param train_target_expression_map: Mapping of target values to the expressions used to compute them for train
    :param extract_shap_values: To extract or not the Shapley value of features
    :param log_train_data: To log or not the final training dataset using PAI
    """

    unique_weeks = sorted(df[weekstart_column_name].unique())
    last_weeks = unique_weeks[-weeks_train:]
    df = df[df[weekstart_column_name].isin(last_weeks)]

    X, y, weeks = _preprocess_X_y_weeks(
        df,
        train_query,
        features_used,
        train_target_expression_map,
        weekstart_column_name,
    )

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
        "basic stats - final training set": _get_dataset_basic_stats_df(
            X, y, weeks.min(), weeks.max(), "train"
        ),
    }

    if extract_shap_values:
        shap_values = model.get_shap(X, y, categorical_features)
        if shap_values is not None:
            feature_effects = plt.figure()
            shap.summary_plot(shap_values, X, show=False, max_display=len(X.columns))
            plt.tight_layout()
            artifacts["feature_effects"] = feature_effects

    if log_train_data:
        artifacts["final_model_train_data"] = X

    pai.log_artifacts(artifacts)


def _plt_reliability_diagrams(
    y_true: pd.Series, y_pred: pd.Series, num_bins: int = 10, normalize: bool = False
) -> mpl.figure.Figure:
    """
    Returns plot of the calibration curve (aka reliability diagrams)

    :param y_true: Series of floats (actual values)
    :param y_pred: Series of floats (predicted values)
    :param num_bins: Number of bins of scores
    :param normalize: To normalize the prediction or not before plotting the calibration curve
    :return: Figure object of the graph plotted
    """

    y_true_values = y_true.values
    y_pred_values = y_pred.values

    mean_y_true = np.mean(y_true_values)
    mean_y_pred = np.mean(y_pred.values)

    fig, axes = plt.subplots(
        nrows=2, ncols=2, figsize=(15, 8), constrained_layout=True, squeeze=False
    )
    fig.suptitle(
        f"Reliability Diagrams (Mean of targets = {mean_y_true:.2f}, Mean of predictions = {mean_y_pred:.2f})"
    )

    for col, strategy in enumerate(["uniform", "quantile"]):
        fraction_of_positives, mean_predicted_value = calibration_curve(
            y_true_values,
            y_pred_values,
            normalize=normalize,
            n_bins=num_bins,
            strategy=strategy,
        )
        axes[0][col].plot(
            [0, 1], [0, 1], color="black", label="Perfectly calibrated", linestyle="--"
        )
        axes[0][col].plot(
            mean_predicted_value,
            fraction_of_positives,
            color="green",
            label="Our Model",
            marker=".",
        )
        axes[0][col].set_title(f"Widths of the bins: '{strategy.capitalize()}'")
        axes[0][col].set_xlabel("Mean Predicted Probability")
        axes[0][col].set_ylabel("Fraction of Default")
        axes[0][col].legend(loc="best")

        if strategy == "quantile":
            bins = np.quantile(
                y_pred_values, [e / num_bins for e in range(1, num_bins)]
            )
            current_range = None
        else:
            bins = num_bins
            if normalize:
                current_range = (0, 1)
            else:
                current_range = (min(y_pred_values), max(y_pred_values))

        axes[1][col].hist(
            y_pred_values,
            bins=bins,
            range=current_range,
            histtype="step",
            color="green",
            lw=2,
        )
        axes[1][col].set_title(f"Widths of the bins: '{strategy.capitalize()}'")
        axes[1][col].set_xlabel("Mean Predicted Probability")
        axes[1][col].set_ylabel("Count")

    return fig


def _get_dataset_basic_stats_df(
    X: pd.DataFrame, y: pd.Series, start_week: str, end_week: str, tag: str
):
    """
    Returns s DataFrame containing the basic statistics about the dataset

    :param X: DataFrame representing a dataset
    :param y: Pandas Series containing the targets
    :param start_week: String representing the min week for the dataset
    :param end_week: String representing the max week for the dataset
    :param tag: Tag  to use
    :return:
        A DataFrame containing the basic stats about the dataset
    """
    accepted_targets = [0, 1]
    stats_columns = ["week_start", "week_end", "num_rows", "num_cols"] + [
        f"target_{t}" for t in accepted_targets
    ]

    y_stats = y.value_counts(normalize=True)
    y_stats = dict(zip(y_stats.index, y_stats.values))
    stats_df = pd.DataFrame(
        data=[
            [start_week, end_week]
            + list(X.shape)
            + [f"{np.round(100 * y_stats.get(t, 0), 2)}%" for t in accepted_targets]
        ],
        columns=stats_columns,
        index=[f"{tag}_statistics"],
    )

    return stats_df


def _plt_lift_graphs(
    y_true: pd.Series, y_pred: pd.Series, num_bins: int = 10
) -> mpl.figure.Figure:
    """
    Returns plot of the Lift curves

    :param y_true: Series of floats (actual values)
    :param y_pred: Series of floats (predicted values)
    :param num_bins: Number of bins of scores
    :return: Figure object of the graph plotted
    """

    lift_df = get_lift_analysis_df(y_true, y_pred, num_bins=num_bins)

    fig, axes = plt.subplots(
        nrows=1, ncols=2, figsize=(16, 7), constrained_layout=True, squeeze=False
    )
    fig.suptitle("Cumulative Gains and Lift Curves")

    # Cumulative Gain Curves
    x_values = [0] + list(lift_df["cumulative_population_percent"])
    axes[0][0].plot(
        x_values,
        [0] + list(lift_df["cumulative_population_percent"]),
        color="gray",
        label="Random Targeting",
        linestyle="--",
    )

    axes[0][0].plot(
        x_values,
        [0] + list(lift_df["cumulative_default_percent"]),
        color="blue",
        label="Our Model",
    )

    axes[0][0].plot(
        x_values,
        [0] + list(lift_df["ideal_cumulative_default_percent"]),
        color="Green",
        label="Perfect Targeting",
        linestyle="--",
    )

    axes[0][0].set_title(f"Cumulative Gain Curves")
    axes[0][0].set_xlabel("Percent of population")
    axes[0][0].set_ylabel("Percent of default")
    axes[0][0].legend(loc="best")

    # LIFT curve
    axes[0][1].plot(
        lift_df["cumulative_population_percent"],
        pd.Series([1] * lift_df["cumulative_population_percent"].size),
        color="Gray",
        label="Random Targeting",
        linestyle="--",
    )

    axes[0][1].plot(
        lift_df["cumulative_population_percent"],
        lift_df["our_model_lift"],
        color="Blue",
        label="Our Model",
    )

    axes[0][1].set_title(f"Lift")
    axes[0][1].set_xlabel("Percent of population")
    axes[0][1].set_ylabel("Lift")
    axes[0][1].legend(loc="best")

    return fig


def _plt_score_distribution(
    train_y_true: pd.Series,
    train_y_pred: pd.Series,
    test_y_true: pd.Series,
    test_y_pred: pd.Series,
    train_target_expression_map: Dict[int, str],
    test_target_expression_map: Dict[int, str],
    num_bins: int = 10,
) -> mpl.figure.Figure:
    """
    Returns plots of score distributions for train and test over all cross-validation folds

    :param train_y_true: Actual values used to fit models across cross-validation
    :param train_y_pred: Predicted values for data used to train cross-validation models
    :param test_y_true: Test actual values used during cross-validation
    :param test_y_pred: Test predicted values during cross-validation
    :param train_target_expression_map: Mapping of target values to the expressions used to compute them for train
    :param test_target_expression_map: Mapping of target values to the expressions used to compute them for test
    :param num_bins: Number of bins of scores
    :return: Figure object of the graph plotted
    """

    data = {
        "train": {
            "target_score": pd.DataFrame({"true": train_y_true, "pred": train_y_pred}),
            "target_mapping": train_target_expression_map,
        },
        "test": {
            "target_score": pd.DataFrame({"true": test_y_true, "pred": test_y_pred}),
            "target_mapping": test_target_expression_map,
        },
    }

    fig, axes = plt.subplots(
        nrows=2, ncols=1, figsize=(16, 7), constrained_layout=True, squeeze=False
    )
    fig.suptitle("Score Distribution")

    for row_id, name in enumerate(["train", "test"]):
        target_score = data[name]["target_score"]
        target_mapping = data[name]["target_mapping"]
        distinct_target_values = sorted(target_score["true"].unique())

        for target_val in distinct_target_values:
            to_plot = target_score[target_score["true"] == target_val]["pred"]
            mapping_key = int(target_val)
            label = f"{mapping_key} ({target_mapping.get(mapping_key, target_mapping.get(str(mapping_key), ''))})"
            sns.distplot(
                to_plot, bins=num_bins, label=label, kde=True, ax=axes[row_id][0]
            )

        axes[row_id][0].set_title(f"Score Distribution in {name.capitalize()} sets")
        axes[row_id][0].set_xlabel("Scores")
        axes[row_id][0].set_ylabel("Frequency")
        axes[row_id][0].legend(loc="best")

    return fig


def _plt_weight_of_evidence(
    y_true: pd.Series, y_pred: pd.Series, num_bins: int = 10
) -> mpl.figure.Figure:
    """
    Returns plot of the weight of evidence

    :param y_true: Series of floats (actual values)
    :param y_pred: Series of floats (predicted values)
    :param num_bins: Number of bins of scores
    :return: Figure object of the graph plotted
    """

    y_pred_values = y_pred.values

    fig, axes = plt.subplots(
        nrows=2, ncols=2, figsize=(15, 8), constrained_layout=True, squeeze=False
    )
    fig.suptitle(f"Weight Of Evidence")

    for col, bin_type in enumerate(["equal_width", "quantile"]):

        stats_df = get_score_adjustment_stats(
            y_true, y_pred, "woe", num_bins, bin_type
        ).sort_values(by="score_index", ascending=True)

        axes[0][col].plot(
            stats_df.index,
            stats_df["woe"],
            color="green",
            label="Our Model",
            marker=".",
        )
        axes[0][col].plot(
            range(stats_df.shape[0]),
            [1] * stats_df.shape[0],
            color="red",
            label="",
            linestyle="--",
        )
        axes[0][col].set_xticklabels(stats_df["score_range"])
        axes[0][col].set_title(f"Widths of the bins: '{bin_type.capitalize()}'")
        axes[0][col].set_xlabel("Bin of Scores")
        axes[0][col].set_ylabel("WOE")
        axes[0][col].legend(loc="best")
        axes[0][col].invert_xaxis()
        axes[0][col].xaxis.set_tick_params(rotation=45)

        if bin_type == "quantile":
            bins = np.quantile(
                y_pred_values, [e / num_bins for e in range(1, num_bins)]
            )
            current_range = None
        else:
            bins = num_bins
            current_range = (min(y_pred_values), max(y_pred_values))

        axes[1][col].hist(
            y_pred_values,
            bins=bins,
            range=current_range,
            histtype="step",
            color="green",
            lw=2,
        )
        axes[1][col].set_title(f"Widths of the bins: '{bin_type.capitalize()}'")
        axes[1][col].set_xticklabels(stats_df["score_range"])
        axes[1][col].set_xlabel("Bin of Scores")
        axes[1][col].set_ylabel("Count")
        axes[1][col].invert_xaxis()
        axes[1][col].xaxis.set_tick_params(rotation=45)

    return fig
