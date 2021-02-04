import logging
import numbers
from math import ceil, floor
from typing import Dict, Iterable, List

import matplotlib.pyplot as plt
import numpy as np
import pai
import pandas as pd
import pyspark
import pyspark.sql.functions as F
import seaborn as sns
from catboost import CatBoostClassifier
from lifelines import KaplanMeierFitter
from sklearn import base, model_selection, tree
from sklearn.metrics import roc_auc_score
from tqdm import tqdm

from src.dmp.pipelines.model_factory.common_functions import (
    create_and_log_shap_plot,
    summarise_and_log_feature_importance,
    telkomsel_style,
)
from src.dmp.pipelines.model_factory.consts import (
    FEATURE_COL,
    IS_TREATMENT_COL,
    TARGET_COL,
    TARGET_SOURCE_COL,
)


def add_pai_tag(tags: List[str]) -> None:
    """
    Add pai tags for the experiment. Note that this utility function is
    needed for all experiments in the pipeline. Refer to
    https://tsdmp.atlassian.net/wiki/spaces/IN/pages/640417800/Kedro+Migration+PAI+usage+updates
    for more info.

    :param tags: PAI tags that is read from parameters.yml
    """
    pai.add_tags(tags)


# noinspection PyPep8Naming
@telkomsel_style()
def plot_target_source(
    df: pyspark.sql.DataFrame,
    weekstart_column: str,
    is_taker_column: str,
    is_control_column: str,
    plot_labels: Dict[str, str],
) -> None:
    """
    Plots average TARGET_SOURCE_COL for every week for takers, non-takers and control.

    :param df: Input DataFrame, must contain TARGET_SOURCE_COL
    :param weekstart_column: Column containing the weekstarts
    :param is_taker_column: Column flagging the taker population
    :param is_control_column: Column flagging the control population
    :param plot_labels: Dictionary containing various plots labels
    """
    AVERAGE_TARGET_SOURCE = "average_source_to_plot"
    IS_NONTAKER = "is_nontaker"
    GROUP = "Group"

    assert (
        TARGET_SOURCE_COL in df.columns
    ), f"Input table does not contain {TARGET_SOURCE_COL}"
    y_label = plot_labels["target_source_y_label"]

    df = df.withColumn(
        IS_NONTAKER, 1 - F.col(is_taker_column) - F.col(is_control_column)
    )

    average_sources = (
        df.fillna(0, subset=TARGET_SOURCE_COL)
        .groupby(weekstart_column, is_control_column, is_taker_column, IS_NONTAKER)
        .agg(F.mean(TARGET_SOURCE_COL).alias(AVERAGE_TARGET_SOURCE))
        .toPandas()
        .sort_values(by=weekstart_column)
    )
    average_sources[GROUP] = average_sources[
        [is_control_column, is_taker_column, IS_NONTAKER]
    ].idxmax(axis=1)

    fig = plt.figure()
    ax = sns.barplot(
        x=weekstart_column, y=AVERAGE_TARGET_SOURCE, hue=GROUP, data=average_sources,
    )
    ax.set_xlabel("Week start")
    ax.set_ylabel(y_label)
    plt.xticks(rotation=70)
    pai.log_artifacts({"target_source_plot": fig})


# noinspection PyUnresolvedReferences
@telkomsel_style()
def _plot_target_per_group(
    group_1: pd.Series,
    group_2: pd.Series,
    label_group_1: str,
    label_group_2: str,
    x_label: str = "Observed target value",
    y_label: str = "Density",
) -> plt.Figure:
    """
    Plots the histograms for group_1 and group_2, labeled with label_group_1 and label_group_2.

    :param group_1: Series with input for the first histogram
    :param group_2: Series with input for the second histogram
    :param label_group_1: Label for the first histogram
    :param label_group_2: Label for the second histogram
    :param x_label: Label for x axis
    :param y_label: Label for y axis
    :return: Plot object containing two histograms
    """
    # Floor and ceil are for compatibility with binary targets
    range_lower = min(floor(group_1.quantile(q=0.01)), floor(group_2.quantile(q=0.01)))
    range_upper = max(ceil(group_1.quantile(q=0.99)), ceil(group_2.quantile(q=0.99)))
    fig = plt.figure()
    plt.hist(
        [group_1, group_2],
        bins=50,
        density=True,
        label=[label_group_1, label_group_2],
        range=(range_lower, range_upper),
    )

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.legend()
    return fig


@telkomsel_style()
def plot_target(df: pd.DataFrame, is_taker_column: str) -> None:
    """
    Plots the TARGET_COL for treatment vs control and takers vs non-takers, log the plots to PAI.

    :param df: Input DataFrame
    :param is_taker_column: Column flagging the taker population
    """

    tmt_v_ctrl = _plot_target_per_group(
        group_1=df[df[IS_TREATMENT_COL] == 1][TARGET_COL],
        group_2=df[df[IS_TREATMENT_COL] == 0][TARGET_COL],
        label_group_1="Treatment",
        label_group_2="Control",
        x_label=f"Observed {TARGET_COL} value",
    )
    if df[is_taker_column].nunique() == 1:
        logging.warning(
            "Unable to find takers or non-takers - this may indicate a campaign setup issue"
        )
        taker_v_non_taker = None
    else:
        taker_v_non_taker = _plot_target_per_group(
            group_1=df[df[is_taker_column] == 1][TARGET_COL],
            group_2=df[(df[IS_TREATMENT_COL] == 1) & (df[is_taker_column] == 0)][
                TARGET_COL
            ],
            label_group_1="Takers",
            label_group_2="Non-takers",
            x_label=f"Observed {TARGET_COL} value",
        )
    pai.log_artifacts(
        {
            f"{TARGET_COL}_treatment_vs_control": tmt_v_ctrl,
            f"{TARGET_COL}_takers_vs_non_takers": taker_v_non_taker,
        }
    )


def count_populations(df: pd.DataFrame, is_taker_column: str) -> None:
    """
    Computes the populations of treatment, takers and control groups and logs them in PAI.

    :param df: Input DataFrame
    :param is_taker_column: Column flagging the taker population
    """
    population_treatment = df[IS_TREATMENT_COL].sum()
    population_control = df.shape[0] - population_treatment
    population_takers = df[is_taker_column].sum()
    pai.log_params({"population_control": population_control})
    pai.log_params({"population_treatment": population_treatment})
    pai.log_params({"population_takers": population_takers})
    pai.log_params(
        {"population_take_up_rate": round(population_takers / population_treatment, 2)}
    )


class DataSetError(Exception):
    pass


def _quick_sanity_check(df: pd.DataFrame) -> None:
    """
    checks dataframe to ensure it contains rows and more than 1 unique TARGET value
    raises error if checks failed

    :param df: dataframe to check
    """
    if df.size == 0:
        raise DataSetError("No rows found in dataframe")
    if df[TARGET_COL].nunique() == 1:
        msg = f"Only one value appears in dataset: {df[TARGET_COL].iloc[0]}"
        raise DataSetError(msg)


@telkomsel_style()
def target_mean_by_group(df: pd.DataFrame) -> None:
    """
    This calculates standard conversion rate metrics  for discrete event
    type campaigns, and will compute average metrics for continuous type
    campaigns (eg, 4g and inlife, respectively).

    These results will be output to several places:

     - As artifact in PAI - saved as a table

    Additionally, it will run a few sanity checks:

     - Are there even rows?!
     - Are there conversions?
     - And, all of the above for treatment & control groups separately.

    :param df: input dataframe
    """

    is_treatment = df[IS_TREATMENT_COL].astype(bool)
    _quick_sanity_check(df)
    _quick_sanity_check(df[is_treatment])
    _quick_sanity_check(df[~is_treatment])

    treatment_status = is_treatment.map({False: "control", True: "treatment"}).rename(
        "group"
    )
    is_event_example = set(df[TARGET_COL]) == {0, 1}
    if not is_event_example:
        # This just checks that we see a reasonable number of  different
        # values for continuous attributes.  Otherwise it probably means
        # that there are actually several categories involved instead of
        # a very small number of continuous values.
        if df[TARGET_COL].nunique() < 10:
            msg = f"Suspiciously small number of cases for non-binary case {df[TARGET_COL].unique()}"
            raise DataSetError(msg)
    target_value = df[TARGET_COL]
    # Create our converison info:
    #   group by treated / control
    #   take average of target
    #   rename it - so that when we make it into  a datafame it will end
    #     up with a nice name
    target_mean = target_value.groupby(treatment_status).mean().rename("target mean")

    pai.log_artifacts({"target_mean": target_mean.to_frame()})


@telkomsel_style()
def plot_histogram_for_churn_split(
    df: pd.DataFrame, cols_to_plot: Iterable[str],
) -> None:
    """
    Plots the histogram of given columns splitting on TARGET variable.

    :param df: Input DataFrame.
    :param cols_to_plot: Values to plot.
    """
    for col_to_plot in cols_to_plot:
        x_label = col_to_plot[0:40]
        logging.info(f"Creating churn split histogram for {x_label}")
        target = df[TARGET_COL]
        to_plot = df[col_to_plot]
        histogram_plot = _plot_target_per_group(
            group_1=to_plot[target == 0],
            group_2=to_plot[target == 1],
            label_group_1="Non-churner",
            label_group_2="Churner",
            x_label=x_label,
        )
        pai.log_artifacts({f"churn_vs_{x_label}": histogram_plot})


def count_targets(df: pd.DataFrame, msisdn_column: str) -> None:
    """
    Counts the population split by TARGET, saves it to pai.

    :param msisdn_column: Name of msisdn columns
    :param df: Input DataFrame
    """
    population_by_target = df[[TARGET_COL, msisdn_column]].groupby([TARGET_COL]).count()
    pai.log_artifacts({"population_by_target": population_by_target})


def _learn_comparison_model(
    x: pd.DataFrame,
    y: pd.Series,
    stratify: pd.Series,
    comparison_title: str,
    shap_settings: dict,
) -> None:
    """
    Trains classifiers to distinguish between two groups of population, finds the feature importances.
    Logs mean and std AUC, feature importance table and SHAP plot to PAI.

    :param x: Feature dataframe used to try to distinguish between the two groups
    :param y: Target vector defining the two groups to distinguish
    :param stratify: Segments to consider for stratified bootstrapping
    :param comparison_title: title given to the comparison exercise
    :param shap_settings: configures the SHAP model constructions, with keys:
        - n_folds: Number of folds used for bootstrapping
        - n_iterations: Number of iterations used for classification model and its explainer
        - train_frac: Fraction of observations used for training
        - n_features: Number of most important features explained using SHAP
    """
    aucs = []
    importance_tables = []

    base_model = CatBoostClassifier(
        iterations=(shap_settings["n_iterations"]),
        verbose=False,
        allow_writing_files=False,
        loss_function="Logloss",
    )
    cv = model_selection.StratifiedShuffleSplit(
        n_splits=(shap_settings["n_folds"]), train_size=(shap_settings["train_frac"]),
    )
    cv_iter = tqdm(
        enumerate(cv.split(x, stratify), start=1),
        total=shap_settings["n_folds"],
        desc="Fitting dataset folds",
    )
    for fold, (train_ix, test_ix) in cv_iter:
        x_train, y_train = x.iloc[train_ix], y.iloc[train_ix]
        x_test, y_test = x.iloc[test_ix], y.iloc[test_ix]
        fold_model = base.clone(base_model)
        fold_model.fit(x_train, y_train)
        y_pred = fold_model.predict_proba(x_test)[:, 1]
        aucs.append(roc_auc_score(y_test, y_pred))
        importance_table = fold_model.get_feature_importance(prettified=True)
        importance_tables.append(importance_table)

    pai.log_params({f"{comparison_title}_AUC_mean": np.mean(aucs)})
    pai.log_params({f"{comparison_title}_AUC_std": np.std(aucs)})
    # For convenient comparisons in PAI
    pai.log_metrics({f"{comparison_title}_AUC_mean": np.mean(aucs)})

    importance_summary_table = summarise_and_log_feature_importance(
        importance_tables, pai_log_suffix=comparison_title,
    )

    most_important_features = importance_summary_table.loc[
        : shap_settings["n_features"], FEATURE_COL
    ].tolist()
    x_train_for_shap = x[most_important_features]
    y_train_for_shap = y

    explainer_model = CatBoostClassifier(
        iterations=shap_settings["n_iterations"],
        verbose=False,
        allow_writing_files=False,
    )
    explainer_model.fit(x_train_for_shap, y_train_for_shap)

    create_and_log_shap_plot(
        x=x_train_for_shap,
        explainer_model=explainer_model,
        artifact_name=f"{comparison_title}_shap",
    )


def learn_treatment_vs_control(
    df: pd.DataFrame, is_taker_column: str, msisdn_column: str, shap_settings: dict
) -> None:
    """
    Trains classifiers to distinguish between treatment and control, finds the feature importances.
    Logs mean and std AUC, feature importance table and SHAP plot to PAI.

    :param df: Input DataFrame
    :param is_taker_column: Column flagging the taker population
    :param msisdn_column: Column name for MSISDNs
    :param shap_settings: configures the SHAP model constructions, with keys:
        - n_folds: Number of folds used for bootstrapping
        - n_iterations: Number of iterations used for classification model and its explainer
        - train_frac: Fraction of observations used for training
        - n_features: Number of most important features explained using SHAP
    """
    y = df[IS_TREATMENT_COL]
    stratify = df[is_taker_column]
    x = df.drop(columns=[IS_TREATMENT_COL, TARGET_COL, is_taker_column, msisdn_column])

    _learn_comparison_model(x, y, stratify, "treatment_vs_control", shap_settings)


def learn_taker_vs_nontaker(
    df: pd.DataFrame, is_taker_column: str, msisdn_column: str, shap_settings: dict
) -> None:
    """
    Trains classifiers to distinguish between takers and non-takers, finds the feature importances.
    Logs mean and std AUC, feature importance table and SHAP plot to PAI.

    :param df: Input DataFrame
    :param is_taker_column: Column flagging the taker population
    :param msisdn_column: Column name for MSISDNs
    :param shap_settings: configures the SHAP model constructions, with keys:
        - n_folds: Number of folds used for bootstrapping
        - n_iterations: Number of iterations used for classification model and its explainer
        - train_frac: Fraction of observations used for training
        - n_features: Number of most important features explained using SHAP
    """
    df = df.query(f"{IS_TREATMENT_COL} == 1").reset_index(drop=True)
    stratify = y = df[is_taker_column]
    x = df.drop(columns=[IS_TREATMENT_COL, TARGET_COL, is_taker_column, msisdn_column])

    _learn_comparison_model(x, y, stratify, "taker_vs_nontaker", shap_settings)


class SurvivalSpec:
    """
    A survival spec helps you build survival graphs from dictionary conf

    The design goals are:

     - Have a self-documenting-ish format for the config
     - Have something that'll error-check an entry in `parameters.yml`

    Example:

    yaml content

    ```
    - col: foo   # Sets the column we'll split groups upon
      date: bar  # Sets the column we'll use for the date.  A NULL value
                 # considered censored
      measure_mode: auto # Sets up the problem
    ```

    Usage:
    ```
    SurvivalSpec.from_config(yaml.loads(content))
    ```

    There are 4 modes:

     - auto: guess as to which category:
       - If there are less than 8 values: categorical
       - Else:
           If column is numeric: split
           Else: high_categorical
     - categorical: every value has it's own KM plot
     - high-categorical: only the top 5 most frequent values are shown
     - split: continuous data are split into groups automagically.
    """

    def __init__(self, measure_col, expiry_date_col, measure_mode, name):
        self.measure_col = measure_col
        self.expiry_date_col = expiry_date_col
        if measure_mode not in {"categorical", "auto", "split", "high_categorical"}:
            raise NotImplementedError(f"Unknown mode - {measure_mode}")
        self.measure_mode = measure_mode
        self.name = name

    def __repr__(self):
        return (
            f"SurvivalSpec({self.measure_col}, {self.expiry_date_col},"
            f" {self.measure_mode}, {self.name})"
        )

    @classmethod
    def from_config(cls, config):
        name = config.get("name", config["col"])

        try:
            return SurvivalSpec(
                config["col"],
                config.get("date", "deactivation_date"),
                config.get("measure_mode", "auto"),
                name,
            )
        except NotImplementedError:
            raise NotImplementedError(f"Unknown mode found in {config}")
        except KeyError as e:
            raise KeyError(f"Missing key from {config} - {e.args[0]}")


def survival_curves(df: pd.DataFrame, survival_configs):
    specs = []
    for conf in survival_configs:
        # Check for issues in the config
        specs.append(SurvivalSpec.from_config(conf))

    for spec in specs:
        plt.close("all")
        plt.figure()
        survival_curve(df, spec)
        pai.log_artifacts({f"survival-curve-{spec.name}": plt.gcf()})
        plt.savefig("./results/{}.png".format(spec.name), bbox_inches="tight")
        plt.close("all")


def survival_curve(df: pd.DataFrame, spec: SurvivalSpec):
    # Get the relevant columns from the dataframe. Good for flagging any
    # errors or mistakes early in the process.
    measure = df[spec.measure_col]
    expiration = pd.to_datetime(df[spec.expiry_date_col])
    expiration = (expiration - expiration.min()).dt.total_seconds() / (3600 * 24)

    # The censored points have a null expiration
    timeline_time = expiration.fillna(expiration.max())
    is_censored = df[spec.expiry_date_col].isnull()

    # Resolve the measure mode. If the mode is auto,  and there are 8 or
    # fewer cases, it's categorical. If auto and more than 8, then we go
    # to the next set of decisions, and if it is numeric, then we assume
    # it's the split case, otherwise if non-numeric, we do high cardinal
    # categorical.
    measure_mode = spec.measure_mode
    if measure_mode == "auto":
        if measure.nunique() <= 8:
            measure_mode = "categorical"
        else:
            if issubclass(measure.dtype.type, numbers.Number):
                measure_mode = "split"
            else:
                measure_mode = "high-categorical"

    if measure_mode in {"categorical", "high-categorical"}:
        # This is converted to string to allow for funny values like NaN
        measure = measure.astype(str)

        # If categorical, we just use all groups.  For high categorical,
        # we only look at the top 5 values.
        if measure_mode == "categorical":
            groups = measure.unique()
        else:
            groups = measure.value_counts().nlargest(5).index

        for group in groups:
            # No need for fancy comparison - due to the str conversion.
            filt = measure == group
            group_name = group
            # Shorten the name - to avoid spam
            if len(group_name) > 26:
                group_name = group[:11] + "..." + group_name[-11:]
            kmf = KaplanMeierFitter()
            kmf.fit(
                durations=timeline_time[filt],
                event_observed=~is_censored[filt],
                label=f"Group: {group_name}",
            )
            kmf.plot()
    elif measure_mode == "split":
        # Split mode uses a decision tree to infer good cutoffs  for the
        # dataset. These are used as the groups in the KMF fit.
        measure_nums = measure.fillna(-999)
        meas_tree = tree.DecisionTreeRegressor(max_leaf_nodes=5).fit(
            measure_nums.values[:, None], timeline_time
        )
        leafs = meas_tree.apply(measure_nums.values[:, None])
        for leaf in set(leafs):
            # Infer the names by looking at min & max. This is a lot lot
            # easier than poking around inside the tree.
            filt = leafs == leaf
            group_min = measure_nums[filt].min()
            group_max = measure_nums[filt].max()
            if group_min == -999:
                group_min = "Unknown"
            if group_max == -999:
                group_max = "Unknown"
            group_name = f"{group_min} -- {group_max}"
            kmf = KaplanMeierFitter()
            kmf.fit(
                durations=timeline_time[filt],
                event_observed=~is_censored[filt],
                label=f"Group: {group_name}",
            )
            kmf.plot()
    else:
        raise NotImplementedError(f"Unknown mode: {measure_mode} for case {spec}")

    return plt.gcf()
