import logging
from typing import Dict, List, Optional, Tuple, Union

import matplotlib.pyplot as plt
import numpy as np
import pai
import pandas as pd
import seaborn as sns
from sklearn import model_selection
from tqdm import tqdm

from mllib.model_classes.model_classes import (
    AttentiveWrapper,
    AverageTakeEffectCalculator,
    BinaryUpliftClassifier,
    QuantileCVMapModelWrapper,
    UpliftClassifier,
    UpliftRegressor,
)
from src.dmp.pipelines.model_factory.common_functions import (
    create_and_log_shap_plot,
    summarise_and_log_feature_importance,
    summarise_table,
    telkomsel_style,
)
from src.dmp.pipelines.model_factory.consts import (
    CUMGAIN_COL,
    CUMLIFT_COL,
    CUMPROP_COL,
    FEATURE_COL,
    IMPORTANCE_COL,
    IS_TREATMENT_COL,
    MODEL_COL,
    PREDICTION_COL,
    QUANTILE_COL,
    RANDOM_COL,
    TARGET_COL,
)

log = logging.getLogger(__name__)


def drop_msisdns(df: pd.DataFrame, msisdn_column: str) -> pd.DataFrame:
    """
    Removes the MSISDN column

    :param df: the dataframe to edit
    :param msisdn_column: the column to drop
    :return: a modified dataframe
    """
    return df.drop(columns=[msisdn_column])


def create_uplift_model(
    use_case: str, modeling_approach: str, uplift_iterations: int,
) -> Union[
    UpliftRegressor, UpliftClassifier, BinaryUpliftClassifier, AttentiveWrapper,
]:
    """
    Creates model for the selected use case. If you would like to tune your use case
    model, this is the place to do it.

    :param use_case: name of the use-case
    :param modeling_approach: name of the modeling approach
    :param uplift_iterations: number of CatBoost trees to make
    :return: an uplift model relevant to the use selected case
    """
    if use_case == "fourg":
        if modeling_approach == "t-learner":
            return UpliftClassifier(uplift_iterations)
        elif modeling_approach == "binary_t-learner":
            return BinaryUpliftClassifier(uplift_iterations)
        elif modeling_approach == "two_stage_model":
            return AttentiveWrapper(uplift_iterations, AverageTakeEffectCalculator)
        elif modeling_approach == "attentive":
            return AttentiveWrapper(uplift_iterations, UpliftClassifier)
        else:
            raise NotImplementedError(
                f"Unknown modeling approach: {modeling_approach} for use case: {use_case}"
            )
    elif use_case == "inlife":
        if modeling_approach == "t-learner":
            return UpliftRegressor(uplift_iterations)
        elif modeling_approach == "two_stage_model":
            return AttentiveWrapper(uplift_iterations, AverageTakeEffectCalculator)
        elif modeling_approach == "attentive":
            return AttentiveWrapper(uplift_iterations, UpliftRegressor)
        else:
            raise NotImplementedError(
                f"Unknown modeling approach: {modeling_approach} for use case: {use_case}"
            )
    else:
        raise NotImplementedError(f"Unknown use case: {use_case}")


def _train_and_get_results(
    uplift_model: Union[
        UpliftRegressor, UpliftClassifier, BinaryUpliftClassifier, AttentiveWrapper,
    ],
    df: pd.DataFrame,
    n_uplift_folds: int,
    uplift_train_size: float,
    is_taker_column: str,
    tqdm_suffix: str = "",
) -> Tuple[
    pd.DataFrame,
    Union[UpliftRegressor, UpliftClassifier, BinaryUpliftClassifier, AttentiveWrapper,],
    List[pd.DataFrame],
    List[pd.DataFrame],
]:
    """
    Trains multiple models to predict the treatment effect and finds their feature importances.
    Trains and cross-validates on uplift_folds bootstrapped data sets stratified on is_taker_column.

    :param uplift_model: Model object used for the uplift predictions
    :param df: Input DataFrame
    :param n_uplift_folds: Number of folds used for bootstrapping
    :param uplift_train_size: Fraction of observations used for training
    :param is_taker_column: Column flagging the taker population
    :param tqdm_suffix: Suffix used in progress bar display
    :return:
        - the feature dataset
        - the trained model
        - the list of DataFrames containing cumulative lifts
        - the list of DataFrames containing feature importances
    """

    x_data = df.drop(columns=[IS_TREATMENT_COL, TARGET_COL, is_taker_column])
    y_data = df[TARGET_COL]
    t_data = df[is_taker_column]
    w_data = df[IS_TREATMENT_COL]
    stratify = t_data + 2 * (1 - w_data)

    assert not np.any(x_data.index.duplicated()), "Cannot fit over duplicated index"

    cv = model_selection.StratifiedShuffleSplit(
        n_uplift_folds, train_size=uplift_train_size
    )

    bar = tqdm(
        cv.split(x_data, stratify),
        total=n_uplift_folds,
        desc=f"Uplift modelling {tqdm_suffix}",
    )

    importance_tables = []
    prediction_w_y_tables = []
    for (train_ix, test_ix) in bar:
        train_x, train_y, train_w, train_t = (
            x_data.iloc[train_ix],
            y_data.iloc[train_ix],
            w_data.iloc[train_ix],
            t_data.iloc[train_ix],
        )
        test_x, test_y, test_w = (
            x_data.iloc[test_ix],
            y_data.iloc[test_ix],
            w_data.iloc[test_ix],
        )
        bar.set_description(f"Fitting fold with data size {train_x.shape}")
        model = uplift_model.fit(train_x, train_y, train_w, train_t)

        importance_table = model.get_feature_importance()
        importance_tables.append(importance_table)

        prediction_w_y_table = pd.concat(
            [
                pd.Series(
                    model.predict(test_x), name=PREDICTION_COL, index=test_x.index
                ),
                test_w.rename(IS_TREATMENT_COL),
                test_y.rename(TARGET_COL),
            ],
            axis=1,
        )
        prediction_w_y_tables.append(prediction_w_y_table)

    final_model = uplift_model.fit(x_data, y_data, w_data, t_data)

    return (
        x_data,
        final_model,
        importance_tables,
        prediction_w_y_tables,
    )


@telkomsel_style()
def _plot_and_log_score_distributions(
    prediction_tables: List[pd.DataFrame],
    prediction_col: str = PREDICTION_COL,
    pai_log_suffix: str = "",
) -> plt.Figure:
    """
    Plots the prediction distribution across multiple folds of the cross-validation and log it into PAI.

    :param prediction_tables: list of dataframes containing the prediction scores,
                treatment and outcomes over cross validation.
    :param prediction_col: name of predictions column
    :param pai_log_suffix: suffix for purpose of logging into PAI
    """

    fig = plt.figure()
    for ix, df in enumerate(prediction_tables):
        scores = df[prediction_col]
        sns.distplot(scores, hist=False, kde=True, label=f"Fold_{ix}")

    plt.xlabel("Score")
    plt.ylabel("Density")
    plt.legend()

    pai.log_artifacts({f"score_distribution_{pai_log_suffix}": fig})
    return fig


def _compute_cumulative_metrics(
    prediction_table: pd.DataFrame,
    outcome_col: str = TARGET_COL,
    treatment_col: str = IS_TREATMENT_COL,
    prediction_col: str = PREDICTION_COL,
) -> pd.DataFrame:
    """
    Computes cumulative evaluation metrics for observations ranked using the prediction scores and random.
    - Cumulative Lift
    - Cumulative Gain

    :param prediction_table: DataFrame with outcome, treatment and prediction columns
    :param outcome_col: Name of the column containing the outcome
    :param treatment_col: Name of the column flagging the treatment group
    :param prediction_col: Name of the column containing the model predictions
    :return: DataFrame containing cumulative lift and gain columns
            for model predictions and random for each observation
            DataFrame index: cumulative proportion
    """
    EPS = 1e-16

    prediction_table = prediction_table.copy()
    prediction_table[MODEL_COL] = prediction_table[prediction_col]
    prediction_table[RANDOM_COL] = np.random.rand(prediction_table.shape[0])

    n_row, _ = prediction_table.shape

    metrics = []
    for model in [MODEL_COL, RANDOM_COL]:
        prediction_table = prediction_table.sort_values(
            model, ascending=False
        ).reset_index(drop=True)

        # compute counts
        prediction_table.index = prediction_table.index + 1
        prediction_table["cumsum_tr"] = prediction_table[treatment_col].cumsum()
        prediction_table["cumsum_ct"] = (
            prediction_table.index.values - prediction_table["cumsum_tr"]
        )
        prediction_table["cumsum_y_tr"] = (
            prediction_table[outcome_col] * prediction_table[treatment_col]
        ).cumsum()
        prediction_table["cumsum_y_ct"] = (
            prediction_table[outcome_col] * (1 - prediction_table[treatment_col])
        ).cumsum()

        # compute metrics
        cumlift = prediction_table["cumsum_y_tr"] / (
            prediction_table["cumsum_tr"] + EPS
        ) - prediction_table["cumsum_y_ct"] / (prediction_table["cumsum_ct"] + EPS)
        cumlift = cumlift.rename(f"{CUMLIFT_COL}_{model}")
        cumgain = (cumlift * prediction_table.index.values / n_row).rename(
            f"{CUMGAIN_COL}_{model}"
        )
        metrics.extend([cumlift, cumgain])

    # merge model and random
    metrics_table = pd.concat(metrics, axis=1, join="inner")
    metrics_table.index = (metrics_table.index / n_row).rename(CUMPROP_COL)
    return metrics_table


def _merge_metrics_tables(cumulative_tables: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Merge cumulative metrics from cross-validation folds
    Join dataframes by cumulative proportion with NA filled by linear inerpolation

    :param cumulative_tables: List of dataframes of cumulative metrics from each cross-validation fold
            Dataframes will be merged by index and interpolation computed on index
            Hence, dataframes should be indexed by cumulative proportion
    :return: Merged dataFrame containing cumulative lift and gain for model predictions and random
            Dataframe index: cumulative proportion
    """
    # merge dataframes by index, which should be cumulative proportion
    merged = pd.concat(cumulative_tables, axis=1, sort=True).sort_index()
    # fill null values by interpolation on index (middle), forward fill (end) and 0 (start)
    merged = merged.interpolate(method="index").fillna(method="ffill").fillna(value=0)
    return merged


def _get_metric_quantiles_table(metrics_table: pd.DataFrame) -> pd.DataFrame:
    """
    Reduces number of rows of summary metric dataframe by selecting quantiles at every 0.05

    :param metrics_table: Dataframe containing summary of cross validation metrics
            Dataframe index: cumulative proportion
    :return: Dataframe containing metric summary at selected population quantiles
            Dataframe columns: quantiles, cumprop and remaining columns
    """
    SELECTED_CUMPROP = np.arange(0.05, 1.0, 0.05)

    metrics_table = metrics_table.copy().sort_index()
    metrics_table = metrics_table.iloc[
        metrics_table.index.searchsorted(SELECTED_CUMPROP, "right")
    ]
    metrics_table = metrics_table.reset_index()
    metrics_table.insert(0, QUANTILE_COL, 1 - SELECTED_CUMPROP)
    return metrics_table


@telkomsel_style()
def _plot_cumulative_metric(
    metric_summary_table: pd.DataFrame,
    metric_label: str,
    model_col: str,
    random_col: str,
) -> plt.Figure:
    """
    Creates cumulative lift curves for model predictions, random predictions and their confidence intervals.
    The confidence intervals are computed using the minimum and maximum cumulative lift values at each proportion.
    - Cumulative Uplift
    - Cumulative Gain

    :param metric_summary_table: Summary DataFrame containing the min, median, max metric columns for
            Dataframe index: cumulative proportion, which is used as x for plotting
    :param metric_label: y label for metric
    :param model_col: Suffix in name of the output column containing the model cumulative lifts
    :param random_col: Suffix in name of the output column containing the random cumulative lifts
    :return: Plot object containing the two cumulative lift curves and their confidence intervals
    """
    fig = plt.figure()

    metric_summary_table = metric_summary_table.sort_index()
    proportion = metric_summary_table.index

    # plot metric and range for model
    plt.plot(proportion, metric_summary_table[f"median_{model_col}"], label="Model")
    plt.fill_between(
        proportion,
        metric_summary_table[f"min_{model_col}"],
        metric_summary_table[f"max_{model_col}"],
        alpha=0.5,
    )

    # plot metric and range for random
    plt.plot(proportion, metric_summary_table[f"median_{random_col}"], label="Random")
    plt.fill_between(
        proportion,
        metric_summary_table[f"min_{random_col}"],
        metric_summary_table[f"max_{random_col}"],
        alpha=0.5,
    )

    # set plot range
    y_min = min(
        metric_summary_table[f"min_{model_col}"].quantile(0.005),
        metric_summary_table[f"median_{model_col}"].quantile(0.001),
        metric_summary_table[f"median_{random_col}"].min(),
    )
    y_max = max(
        metric_summary_table[f"max_{model_col}"].quantile(0.995),
        metric_summary_table[f"median_{model_col}"].quantile(0.999),
        metric_summary_table[f"median_{random_col}"].max(),
    )
    plt.ylim(y_min, y_max)

    # labels & legend
    plt.xlabel("Cumulative Proportion")
    plt.ylabel(metric_label)
    plt.legend()
    return fig


def _summarise_and_plot_cumulative_metrics(
    prediction_tables: List[pd.DataFrame],
    metric: str,
    metric_label: str,
    pai_log_suffix: str = "",
) -> pd.DataFrame:
    """
    Computes, summarises, plots and logs cumulative metrics over cross validation folds:
    - Cumulative Uplift
    - Cumulative Gain

    :param prediction_tables: List of prediction DataFrames with outcome, treatment and prediction columns
    :param metric: Metric to compute, one of ["cumlift", "cumgain"]
    :param metric_label: Y metric label for plot
    :param pai_log_suffix: Suffix for purpose of logging into PAI
    :return: Dataframe of metric summaries at selected quantiles
        Dataframe columns: min, max, median, std of cumulative metric for model and random
    """

    # compute metrics for each fold
    cumulative_tables = []
    for ix, df in enumerate(prediction_tables):
        cumulative_table = _compute_cumulative_metrics(df)
        cumulative_table = cumulative_table.rename(columns=lambda col: f"{col}_{ix}")
        cumulative_tables.append(cumulative_table)

    # merge and summarise metrics
    merged_cumulative_table = _merge_metrics_tables(cumulative_tables)

    metric_model_col = f"{metric}_{MODEL_COL}"
    metric_random_col = f"{metric}_{RANDOM_COL}"
    summary_table = summarise_table(
        merged_cumulative_table.reset_index(),
        CUMPROP_COL,
        (metric_model_col, metric_random_col),
    ).set_index(CUMPROP_COL)

    quantile_summary_table = _get_metric_quantiles_table(summary_table)

    # plot metric
    metric_plot = _plot_cumulative_metric(
        summary_table, metric_label, metric_model_col, metric_random_col
    )

    # log artifacts: quantile, plot
    pai.log_artifacts(
        {
            f"{metric}_quantiles_{pai_log_suffix}": quantile_summary_table.round(6),
            f"{metric}_plot_{pai_log_suffix}": metric_plot,
        }
    )

    return quantile_summary_table


def _summarise_and_plot_cumlift(
    prediction_tables: List[pd.DataFrame],
    metric_label: str = None,
    pai_log_suffix: str = "",
) -> pd.DataFrame:
    """
    Computes, summarises, plots and logs cumulative uplift over cross validation folds

    :param prediction_tables: List of prediction DataFrames with outcome, treatment and prediction columns
    :param metric_label: Y metric label for plot
    :param pai_log_suffix: Suffix for purpose of logging into PAI
    :return: Dataframe of metric summaries at selected quantiles
        Dataframe columns: min, max, median, std of cumulative uplift for model and random
    """
    return _summarise_and_plot_cumulative_metrics(
        prediction_tables, "cumlift", metric_label or "Cumulative Lift", pai_log_suffix,
    )


def _summarise_and_plot_cumgain(
    prediction_tables: List[pd.DataFrame],
    metric_label: str = None,
    pai_log_suffix: str = "",
) -> pd.DataFrame:
    """
    Computes, summarises, plots and logs cumulative gain over cross validation folds

    :param prediction_tables: List of prediction DataFrames with outcome, treatment and prediction columns
    :param metric_label: Y metric label for plot
    :param pai_log_suffix: Suffix for purpose of logging into PAI
    :return: Dataframe of metric summaries at selected quantiles
        Dataframe columns: min, max, median, std of cumulative gain for model and random
    """
    return _summarise_and_plot_cumulative_metrics(
        prediction_tables, "cumgain", metric_label or "Cumulative Gain", pai_log_suffix,
    )


def _compute_auuc(
    cumulative_table: pd.DataFrame, model_col: str, random_col: str,
) -> float:
    """
    Compute area between model and random curves

    :param cumulative_table: Dataframe containing cumulative metrics
            Dataframe index: cumulative proportion used as x values
    :param model_col: Column containing values of model curve
    :param random_col: Column containing values of random curve
    :return: area between curves
    """
    x = cumulative_table.index.values
    y_model = cumulative_table[model_col].values
    y_random = cumulative_table[random_col].values
    auuc = np.trapz(y_model, x) - np.trapz(y_random, x)
    return auuc


def _summarise_auuc(
    prediction_tables: List[pd.DataFrame], pai_log_suffix: str = "",
) -> Dict[str, np.ndarray]:
    """
    Compute AUUC of cumulative metrics over cross validation folds:
    - Cumulative Gain

    :param prediction_tables: List of prediction DataFrames with outcome, treatment and prediction columns
    :param pai_log_suffix: Suffix for purpose of logging into PAI
    :return: mean AUUCs
    """
    METRICS = [CUMGAIN_COL]

    # compute metrics for each fold
    cumulative_tables = []
    for df in prediction_tables:
        cumulative_table = _compute_cumulative_metrics(df)
        cumulative_tables.append(cumulative_table)

    # compute auuc for cumgain
    auuc_mean = {}
    for metric in METRICS:
        cv_auuc = []
        metric_model_col = f"{metric}_{MODEL_COL}"
        metric_random_col = f"{metric}_{RANDOM_COL}"

        # compute auuc for each fold
        for df in cumulative_tables:
            cv_auuc.append(_compute_auuc(df, metric_model_col, metric_random_col))

        pai.log_params(
            {
                f"{metric}_AUUC_mean_{pai_log_suffix}": np.mean(cv_auuc),
                f"{metric}_AUUC_std_{pai_log_suffix}": np.std(cv_auuc),
            }
        )

        auuc_mean[f"{metric}_AUUC_mean"] = np.mean(cv_auuc)

    return auuc_mean


def _compute_uplift_by_quantile(
    prediction_tables: List[pd.DataFrame],
    n_quantiles: int = 5,
    outcome_col: str = TARGET_COL,
    treatment_col: str = IS_TREATMENT_COL,
    prediction_col: str = PREDICTION_COL,
    model_col: str = MODEL_COL,
) -> pd.DataFrame:
    """
    Computes model uplift by quantile values over cross validation fits
        1. Compute and assign quantile bins on prediction scores
        2. Compute mean outcome for treatment and control for each quantile
        3. Take the difference of treatment and control within each quantile to get uplift

    Args:
        prediction_tables: list of dataframes containing outcome, treatment and prediction columns
        n_quantiles: number of quantiles to compute
        outcome_col: name of outcome column
        treatment_col: name of treatment column
        prediction_col: name of prediction column
        model_col: name of model column

    Returns:
        dataframe of uplift by quantiles with bin ranges as index
            and uplift fo each cross validation model fits as columns
    """
    BIN_COL = "bin"

    # compute quantile bin edges
    scores = pd.concat(
        (df.loc[df[treatment_col] == 1, prediction_col] for df in prediction_tables),
        ignore_index=True,
    )
    _, bin_edges = pd.qcut(x=scores, q=n_quantiles, duplicates="drop", retbins=True,)
    bin_edges[0], bin_edges[-1] = -np.Inf, np.Inf

    # apply bins on each fold
    quantile_tables = []
    for ix, df in enumerate(prediction_tables):
        df = df.copy()
        # apply quantile bins
        df[BIN_COL] = pd.cut(df[prediction_col], bins=bin_edges)
        # compute mean outcome per bin for treatment and control
        mean_outcome = (
            df.groupby([BIN_COL, treatment_col])[outcome_col].mean().unstack().fillna(0)
        )
        # compute uplift per bin
        quantile_table = (mean_outcome[1] - mean_outcome[0]).rename(f"{model_col}_{ix}")
        quantile_tables.append(quantile_table)

    # merge tables
    uplift_by_quantile_table = pd.concat(quantile_tables, axis=1)
    return uplift_by_quantile_table


@telkomsel_style()
def _plot_uplift_by_quantile(uplift_by_quantile_table: pd.DataFrame) -> plt.Figure:
    """
    Plots the uplift distribution by quantiles of models trained over cross-validation.

    Args:
        uplift_by_quantile_table: dataframe of uplift by quantiles

    Returns:
        uplift by quantile plot
    """
    uplift_stats = uplift_by_quantile_table.quantile(
        [0.0, 0.1, 0.5, 0.9, 1.0], axis=1
    ).T
    uplift_stats.columns = [f"p{p}" for p in [0, 10, 50, 90, 100]]
    n_row, _ = uplift_by_quantile_table.shape
    quantiles = (uplift_by_quantile_table.index.codes + 1) / n_row

    fig = plt.figure()
    plt.fill_between(
        quantiles,
        uplift_stats["p0"],
        uplift_stats["p100"],
        alpha=0.2,
        label="p0 - p100",
    )
    plt.fill_between(
        quantiles,
        uplift_stats["p10"],
        uplift_stats["p90"],
        facecolor="C0",
        alpha=0.5,
        label="p10 - p90",
    )
    plt.plot(quantiles, uplift_stats["p50"], label="p50")

    plt.xlabel("Quantile")
    plt.ylabel("Uplift")
    plt.legend()
    return fig


def _compute_and_plot_uplift_by_quantile(
    prediction_tables: List[pd.DataFrame],
    n_quantiles: int = 5,
    pai_log_suffix: str = "",
) -> pd.DataFrame:
    """
    Computes model uplift by quantile values over cross validation fits
        plot and log results

    Args:
        prediction_tables: list of dataframes containing outcome, treatment and prediction columns
        n_quantiles: number of quantiles to compute
        pai_log_suffix: suffix for logging purposes

    Returns:
        dataframe of uplift by quantiles with bin ranges as index
            and uplift fo each cross validation model fits as columns
    """
    uplift_by_quantile_table = _compute_uplift_by_quantile(
        prediction_tables, n_quantiles=n_quantiles
    )
    uplift_by_quantile_plot = _plot_uplift_by_quantile(uplift_by_quantile_table)
    # convert index to IntervalIndex required for QuantileModelWrapper
    uplift_by_quantile_table.index = uplift_by_quantile_table.index.categories.rename(
        "bin"
    )

    pai.log_artifacts(
        {
            f"uplift_by_quantile_{pai_log_suffix}": uplift_by_quantile_table.reset_index().round(
                6
            ),
            f"uplift_by_quantile_plot_{pai_log_suffix}": uplift_by_quantile_plot,
        }
    )
    return uplift_by_quantile_table


# noinspection PyPep8Naming
def _model_uplift(
    uplift_model: Union[
        UpliftRegressor, UpliftClassifier, BinaryUpliftClassifier, AttentiveWrapper,
    ],
    df: pd.DataFrame,
    uplift_folds: int,
    uplift_train: float,
    is_taker_column: str,
    node_suffix: str = "",
    n_quantiles: int = 5,
    n_features_to_select: Optional[int] = None,
    expose_wrapped_model: bool = False,
    random_seed: int = 44,
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, QuantileCVMapModelWrapper]]:
    """
    Trains models to predict the treatment effect, tests them using bootstrapped cross-validation
    and logs the results to PAI. Logs to PAI the feature importances across different folds of bootstrapping.
    If all_features_mode is True returns a DataFrame containing the n_features_to_select most important features,
    is_taker_column, IS_TREATMENT flag and TARGET as input for one more iteration of training and
    cross-validation. Otherwise logs the most important features and their importances as well as uplift at each
    quantile as performance metric to PAI.

    :param uplift_model: Model object used for the uplift predictions
    :param df: Input DataFrame
    :param uplift_folds: Number of folds used for bootstrapping
    :param uplift_train: Fraction of observations used for training
    :param is_taker_column: Column flagging the taker population
    :param node_suffix: Suffix added in storage and display of node-related objects
    :param n_quantiles: Number of quantiles to consider in the wrapper
    :param n_features_to_select: Number of most important features to return
    :param expose_wrapped_model: Flag disabling the score mapping in the wrapper
    :param random_seed: Seed for the numpy random number generator
    :return: DataFrame containing data for one more iteration of training and cross-validation or
        an instance of FitFoldResults
    """

    np.random.seed(random_seed)

    (
        x_data,
        final_model,
        importance_tables,
        prediction_w_y_tables,
    ) = _train_and_get_results(
        uplift_model=uplift_model,
        df=df,
        n_uplift_folds=uplift_folds,
        uplift_train_size=uplift_train,
        is_taker_column=is_taker_column,
        tqdm_suffix=node_suffix,
    )

    importance_summary_table = summarise_and_log_feature_importance(
        importance_tables, pai_log_suffix=node_suffix
    )
    _plot_and_log_score_distributions(prediction_w_y_tables, pai_log_suffix=node_suffix)
    cumlift_summary_table = _summarise_and_plot_cumlift(
        prediction_w_y_tables, pai_log_suffix=node_suffix
    )
    _summarise_and_plot_cumgain(prediction_w_y_tables, pai_log_suffix=node_suffix)
    auuc_mean = _summarise_auuc(prediction_w_y_tables, pai_log_suffix=node_suffix)
    uplift_by_quantile_table = _compute_and_plot_uplift_by_quantile(
        prediction_w_y_tables, n_quantiles=n_quantiles, pai_log_suffix=node_suffix
    )

    if n_features_to_select:
        pai.log_features(
            features=importance_summary_table[FEATURE_COL],
            importance=importance_summary_table[f"median_{IMPORTANCE_COL}"],
        )
        most_important_features = importance_summary_table.loc[
            :n_features_to_select, FEATURE_COL
        ].to_list()
        return df[
            [IS_TREATMENT_COL, TARGET_COL, is_taker_column] + most_important_features
        ]
    else:
        cumlift_summary_table = cumlift_summary_table.iloc[::2]
        metric_col = f"median_{CUMLIFT_COL}_{MODEL_COL}"
        for quantile, uplift in zip(
            cumlift_summary_table[QUANTILE_COL].round(2),
            cumlift_summary_table[metric_col],
        ):
            pai.log_metrics({f"Quantile {quantile}": uplift})

        pai.log_metrics(auuc_mean)

        return (
            x_data,
            QuantileCVMapModelWrapper(
                final_model,
                uplift_by_quantile_table,
                q=0.10,
                expose_wrapped_model=expose_wrapped_model,
            ),
        )


def model_uplift_all_features(
    uplift_model: Union[
        UpliftRegressor, UpliftClassifier, BinaryUpliftClassifier, AttentiveWrapper,
    ],
    df: pd.DataFrame,
    is_taker_column: str,
    uplift_folds: int,
    uplift_train: float,
    n_quantiles: int,
    n_features_to_select: int,
    expose_wrapped_model: bool,
) -> pd.DataFrame:
    """
    Trains models to predict the treatment effect, tests them using bootstrapped cross-validation
    and logs the results to PAI. Logs to PAI the feature importances across different folds of bootstrapping.
    Returns a DataFrame containing the n_features_to_select most important features, is_taker_column,
    IS_TREATMENT flag and TARGET as input for one more iteration of training and
    cross-validation.

    :param uplift_model: Model object used for the uplift predictions
    :param df: Input DataFrame
    :param is_taker_column: Column flagging the taker population
    :param uplift_folds: Number of folds used for bootstrapping
    :param uplift_train: Fraction of observations used for training
    :param n_quantiles: Number of quantiles to consider in the wrapper
    :param n_features_to_select: Number of the most important features to select and save
    :param expose_wrapped_model: Flag disabling the score mapping in the wrapper
    :return: A list of features that we might want to use downstream.
    """
    return _model_uplift(
        uplift_model=uplift_model,
        df=df,
        uplift_folds=uplift_folds,
        uplift_train=uplift_train,
        is_taker_column=is_taker_column,
        node_suffix="all_features",
        n_quantiles=n_quantiles,
        n_features_to_select=n_features_to_select,
        expose_wrapped_model=expose_wrapped_model,
    )


def model_uplift_best_features(
    uplift_model: Union[
        UpliftRegressor, UpliftClassifier, BinaryUpliftClassifier, AttentiveWrapper,
    ],
    df: pd.DataFrame,
    is_taker_column: str,
    uplift_folds: int,
    uplift_train: float,
    n_quantiles: int,
    expose_wrapped_model: bool,
) -> Tuple[pd.DataFrame, QuantileCVMapModelWrapper]:
    """
    Trains models to predict the treatment effect, tests them using bootstrapped cross-validation
    and logs the results to PAI. Logs to PAI the feature importances across different folds of bootstrapping.
    Logs the most important features and their importances as well as uplift at each quantile as performance
    metric to PAI.

    :param uplift_model: Model object used for the uplift predictions
    :param df: Input DataFrame
    :param is_taker_column: Column flagging the taker population
    :param uplift_folds: Number of folds used for bootstrapping
    :param uplift_train: Fraction of observations used for training
    :param n_quantiles: Number of quantiles to consider in the wrapper
    :param expose_wrapped_model: Flag disabling the score mapping in the wrapper
    """
    x_data, calibrated_model = _model_uplift(
        uplift_model=uplift_model,
        df=df,
        uplift_folds=uplift_folds,
        uplift_train=uplift_train,
        is_taker_column=is_taker_column,
        node_suffix="best_features",
        n_quantiles=n_quantiles,
        expose_wrapped_model=expose_wrapped_model,
    )
    return x_data, calibrated_model


def log_shap_best_features(
    x_data: pd.DataFrame, calibrated_model: QuantileCVMapModelWrapper
) -> None:
    """
    Creates SHAP plot of the calibrated_model using x_data and logs it in PAI.

    :param x_data: Entire x data used for training
    :param calibrated_model: Model to be explained
    """
    create_and_log_shap_plot(
        x=x_data,
        explainer_model=calibrated_model.explainer,
        artifact_name="uplift_shap_best_features",
    )


def log_calibrated_model_to_pai(calibrated_model: QuantileCVMapModelWrapper) -> None:
    """
    Log the calibrated_model  in PAI.

    :param calibrated_model: Model to log
    """
    pai.log_model({"model": calibrated_model})
