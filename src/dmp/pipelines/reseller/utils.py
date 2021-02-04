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
import ast
import datetime
import warnings
from functools import reduce
from typing import Any, Dict, List, Tuple, Union

import matplotlib

# third-party libraries
import numpy
import pandas
import pyspark

# modification of build trees parallel on sklearn
import sklearn.ensemble._forest as sklearn_forest
from dateutil.relativedelta import relativedelta
from matplotlib.ticker import FuncFormatter
from pyspark.sql import SparkSession
from scipy.stats import percentileofscore
from seaborn import scatterplot, set_style
from sklearn.ensemble._forest import _generate_sample_indices
from sklearn.metrics import mean_squared_error, roc_auc_score
from sklearn.utils import compute_sample_weight

# internal code
from .misc.outlet_id_filters import (
    get_outlet_ids_to_filter,
    include_outlet_ids_in_test_set,
)


def get_filter_month(filter_month: Union[datetime.date, None]) -> datetime.date:
    """Return previous month's start date (e.g. 2020-05-01) if input is None else return original string

    Args:
        filter_month: Filter month parameter from parameters.yml

    Returns:
        Date representing month to filter dataset on
    """

    output_filter_month = (
        (datetime.datetime.now().date() - relativedelta(months=1)).replace(day=1)
        if not filter_month
        else filter_month
    )

    return output_filter_month


def master_remove_duplicates(
    ra_master: pandas.DataFrame,
    numerical_columns: List[str],
    categorical_columns: List[str],
    model_params: Dict[str, Any],
) -> pandas.DataFrame:
    """Remove duplicate rows from joined reseller master table.

    Also imputes geospatial POI minimum distance features with a maximum value
        - Geohashing results in a limit in maximum distance for these features
        - POI minimum distance whose nearest POI is not within range of geohash neighbourhood is defaulted to null
        - Imputation is done to account for these 'out-of-range' geospatial POI minimum distance cases

    Args:
        ra_master: Joined reseller master table
        numerical_columns: Numerical features column names
        categorical_columns: Categorical features column names
        model_params: Model parameters from parameters.yml

    Returns:
        Deduplicated joined reseller master table
    """

    # filtering dataframe to only consist of numerical and categorical features to be used with model then deduplicate
    columns = (
        numerical_columns
        + categorical_columns
        + [model_params["target_variable"], "outlet_id"]
    )
    ra_master_deduplicated = ra_master.drop_duplicates(subset=columns)

    # impute minimum distance features with a maximum distance value
    min_dist_features_impute_dict = {
        feature: model_params["min_dist_max_value"]
        for feature in numerical_columns
        if "min_dist" in feature
    }
    ra_master_deduplicated_filled = ra_master_deduplicated.fillna(
        value=min_dist_features_impute_dict
    ).loc[:, columns]

    return ra_master_deduplicated_filled


def master_sample(
    ra_master: pandas.DataFrame,
    model_params: Dict[str, Any],
    return_removed_outlets=False,
) -> pandas.DataFrame:
    """Performs sampling and filtering of outlets based on target variable on joined reseller master table

    Args:
        ra_master: Joined reseller master table
        model_params: Model parameters from parameters.yml, contains
            - Sampling fraction (for pandas sampling)
            - Boolean on removal of outlets with zero-valued target variable
            - Target variable thresholds
                - Defined threshold values
                - Quantile threshold values
        return_removed_outlets: Return removed outlets from filtering for saving to hive table

    Returns:
        Sampled and target variable filtered joined reseller master table
    """

    include_in_test_set_outlet_data = None
    bottom_removed_outlets = ""
    top_removed_outlets = ""

    if model_params["filter_specific_outlets_for_full_dataset"]:
        ra_master_sampled = ra_master[
            ra_master.outlet_id.isin(get_outlet_ids_to_filter())
        ]
    else:
        filter_conditions = []

        if model_params["include_specific_outlet_ids_in_test_set"]:
            include_in_test_set_outlet_data = ra_master[
                ra_master.outlet_id.isin(include_outlet_ids_in_test_set())
            ]

        # removal of outlets with zero-valued target variable
        if model_params["remove_target_zero"]:
            filter_conditions.append((ra_master[model_params["target_variable"]] != 0))

        # default threshold values
        threshold_values = [
            ra_master[model_params["target_variable"]].min(),
            ra_master[model_params["target_variable"]].max(),
        ]

        # update threshold values according to parameters
        threshold_parameter_keys = [
            ("remove_target_below_threshold", "remove_target_below_quantile"),
            ("remove_target_above_threshold", "remove_target_above_quantile"),
        ]
        for idx, threshold_tuple in enumerate(threshold_parameter_keys):
            for threshold_idx, threshold_key in enumerate(threshold_tuple):
                if model_params[threshold_key]:
                    threshold_values[idx] = (
                        ra_master[model_params["target_variable"]].quantile(
                            model_params[threshold_key]
                        )
                        if threshold_idx
                        else model_params[threshold_key]
                    )
                    break

        filter_conditions.extend(
            [
                (ra_master[model_params["target_variable"]] >= threshold_values[0]),
                (ra_master[model_params["target_variable"]] <= threshold_values[1]),
            ]
        )

        # obtain removed outlets
        bottom_removed_outlets = ra_master[
            ra_master[model_params["target_variable"]] < threshold_values[0]
        ][["outlet_id"]]
        top_removed_outlets = ra_master[
            ra_master[model_params["target_variable"]] > threshold_values[1]
        ][["outlet_id"]]

        # apply filter conditions
        ra_master_filtered = ra_master[
            reduce(lambda cond1, cond2: cond1 & cond2, filter_conditions)
        ]

        # sampling of remaining outlet data with provide
        ra_master_sampled = ra_master_filtered.sample(
            **model_params["pandas_sample_frac"]
        ).reset_index(drop=True)

        if model_params["include_specific_outlet_ids_in_test_set"]:
            ra_master_sampled = ra_master_sampled[
                ~ra_master_sampled.outlet_id.isin(include_outlet_ids_in_test_set())
            ]
            ra_master_sampled = ra_master_sampled.append(
                include_in_test_set_outlet_data, ignore_index=True
            ).drop_duplicates()

    if return_removed_outlets:
        return ra_master_sampled, bottom_removed_outlets, top_removed_outlets

    return ra_master_sampled


def unpack_if_single_element(
    datasets: Union[List[pandas.DataFrame], pandas.DataFrame]
) -> Union[List[pandas.DataFrame], pandas.DataFrame]:
    """Unpack list of data sets if only 1 data set is present

    Args:
        datasets: List of datasets

    Returns:
        Unpacked dataset if it is the only dataset else return original list of datasets
    """

    return datasets[0] if len(datasets) == 1 else datasets


def obtain_features_target(
    model_params: Dict[str, Any],
    columns_to_encode: List[str],
    columns_to_scale: List[str],
    *datasets: pandas.DataFrame,
) -> Tuple[pandas.DataFrame, pandas.Series]:
    """Select features and target variable from training set prior to commencing model fitting

    Args:
        model_params: Contains model arguments and list of features and target variable names
        columns_to_encode: Categorical features to encode; to drop if encoding has been performed
        columns_to_scale: Numerical features to scale; to drop if scaling has been performed
        *datasets: Data sets to extract features and target variable from

    Returns:
        Features and target variable to pass into the model as inputs
    """

    # determine main target variable and list of associated target variable column(s)
    target_variable = model_params["target_variable"]
    targets = [target_variable]
    if model_params["model_type"] == "classification":
        target_variable = f'{model_params["target_variable"]}_binned'
        targets.append(target_variable)

    # drop irrelevant columns that are not going to be features
    drop_columns = model_params["drop_columns"] + targets
    if model_params["encode_features"]:
        drop_columns += columns_to_encode
    if model_params["scale_features"]:
        drop_columns += columns_to_scale

    # append datasets (features, targets) to output list
    output_list = []
    for dataset in datasets:
        output_list.append(dataset.drop(columns=drop_columns, errors="ignore"))
        output_list.append(dataset[target_variable])

    return output_list


def obtain_outlet_performance_percentile(
    performance_percentiles: pandas.DataFrame,
) -> pandas.DataFrame:
    """Produce outlet performance percentile using SciPy's percentileofscore on similar outlets cashflows

    Args:
        performance_percentiles: Contains outlets' ground truth cashflows and similar outlets cashflows

    Returns:
        Outlets with performance percentile value and number of similar outlets
    """

    # save length of ground truth list excluding outlet as a value
    performance_percentiles["training_ground_truths_list_length"] = list(
        map(len, performance_percentiles["training_ground_truths_list"])
    )

    # add outlet ground truth to ground truth list (to deal with edge cases)
    performance_percentiles["training_ground_truths_list"] = performance_percentiles[
        "training_ground_truths_list"
    ] + performance_percentiles["ground_truth"].map(lambda value: [value])

    # obtain each outlet's performance percentile
    performance_percentiles["performance_percentile"] = list(
        map(
            lambda ground_truth_list, ground_truth: percentileofscore(
                ground_truth_list, ground_truth, kind="strict"
            ),
            performance_percentiles["training_ground_truths_list"],
            performance_percentiles["ground_truth"],
        )
    )

    def numpy_percentile_helper(percentile):
        def np_percentile(array):
            return numpy.percentile(a=array, q=percentile, interpolation="lower")

        return np_percentile

    # obtain 90th, 95th and 100th percentile potential cashflow
    performance_percentiles["potential_cashflow_90th_percentile"] = list(
        map(
            numpy_percentile_helper(90),
            performance_percentiles["training_ground_truths_list"],
        )
    )
    performance_percentiles["potential_cashflow_95th_percentile"] = list(
        map(
            numpy_percentile_helper(95),
            performance_percentiles["training_ground_truths_list"],
        )
    )
    performance_percentiles["potential_cashflow_100th_percentile"] = list(
        map(
            numpy_percentile_helper(100),
            performance_percentiles["training_ground_truths_list"],
        )
    )

    # typecasting
    performance_percentiles = performance_percentiles.astype(
        {"training_ground_truths_list_length": int, "performance_percentile": float}
    )

    return performance_percentiles.sort_values(by="outlet_id")


def plot_overall_performance_dist(
    performance_percentile_with_predictions: pandas.DataFrame,
    scatter_params: Dict[str, Any],
    trim_above_quantile: float = 1.0,
) -> matplotlib.figure.Figure:
    """Plot and return overall performance percentile scatter plot
    x-axis: Performance percentile of outlet (0 to 100)
    y-axis: Actual cashflow of outlet

    Args:
        performance_percentile_with_predictions: Contains ground truth and performance percentile values for outlets
        scatter_params: Scatter plot parameters for colouring of different quartiles
        trim_above_quantile: Quantile of data to retain for plotting; default of 1.0 results in no trimming

    Returns:
        Performance percentile - ground truth scatter plot of outlets in the performance percentile with prediction data
    """

    # set white grid background for plot
    set_style("whitegrid")

    # create plot figure and ax
    fig, ax = matplotlib.pyplot.subplots(figsize=(18.3, 10))

    # trim outliers (top %)
    outlier_cutoff_value = performance_percentile_with_predictions[
        "ground_truth"
    ].quantile(trim_above_quantile)
    performance_percentile_with_predictions = performance_percentile_with_predictions[
        performance_percentile_with_predictions["ground_truth"] <= outlier_cutoff_value
    ]

    # By colour coded zones
    scatterplot(
        x=performance_percentile_with_predictions["performance_percentile"],
        y=performance_percentile_with_predictions["ground_truth"],
        color="black",
        marker="o",
        ax=ax,
    )

    # background colours
    for idx, percentile in enumerate(scatter_params["scatter_percentiles"][:-1]):
        ax.axvspan(
            percentile,
            scatter_params["scatter_percentiles"][idx + 1],
            facecolor=scatter_params["background_colours"][idx],
            alpha=0.5,
        )

    # set patches for legend (background colours)
    patches = [
        matplotlib.patches.Patch(
            color=scatter_params["background_colours"][quarter - 1],
            label=f"Priority {quarter}",
        )
        for quarter in range(1, 4)
    ] + [
        matplotlib.patches.Patch(
            color="white",
            label=f"Total: {performance_percentile_with_predictions.shape[0]} outlets",
        )
    ]
    matplotlib.pyplot.legend(handles=patches)

    # set x-axis range
    matplotlib.pyplot.xticks(range(0, 101, 10))

    # formatting: set axis labels, title and thousand separator
    ax.set_title(f"Overall outlet performance scatter plot")
    ax.set_xlabel("Performance percentile")
    ax.set_ylabel("Outlet mean daily cashflow")
    ax.get_yaxis().set_major_formatter(FuncFormatter(lambda x, p: format(int(x), ",")))

    return fig


def prepare_dashboard_input_table(
    ra_master: pandas.DataFrame,
    ra_performance_percentiles: pandas.DataFrame,
    master_table_columns: List[str],
    performance_percentile_columns: List[str],
    typecast_dict: Dict[str, Dict[str, str]],
) -> pyspark.sql.DataFrame:
    """Concatenate important model outputs and feature before saving to hive table

    Args:
        ra_master: Joined reseller master table
        ra_performance_percentiles: Contains performance percentile of outlets
        master_table_columns: Columns to extract from joined reseller master table
        performance_percentile_columns: Columns to extract from performance percentiles table
        typecast_dict: Dictionary containing columns and corresponding type to typecast to

    Returns:
        Tabular data to be used for reseller dashboard
    """

    # select columns needed for dashboard
    ra_master_selected = ra_master[["outlet_id", "month"] + master_table_columns]

    # obtain performance percentile for outlets
    ra_performance_percentiles_selected = ra_performance_percentiles[
        ["outlet_id"] + performance_percentile_columns
    ]

    # join tables
    output_table = ra_performance_percentiles_selected.set_index("outlet_id").join(
        ra_master_selected.set_index("outlet_id"), how="left"
    )

    # calculate percentage change in mkios cashflow from previous month
    past_month_cashflow = output_table[
        "fea_outlet_decimal_total_cashflow_mkios_pv_mean_past_1m"
    ]
    curr_month_cashflow = output_table[
        "fea_outlet_decimal_total_cashflow_mkios_pv_mean"
    ]
    output_table[
        "fea_outlet_decimal_total_cashflow_mkios_pv_mean_1m_percentage_change"
    ] = ((curr_month_cashflow - past_month_cashflow) * 100 / past_month_cashflow)

    # filter relevant columns in order
    output_cols = (
        ["outlet_id", "month"]
        + master_table_columns
        + ["fea_outlet_decimal_total_cashflow_mkios_pv_mean_1m_percentage_change"]
        + performance_percentile_columns
    )
    output_table = output_table.reset_index()[output_cols].drop_duplicates()

    # typecast columns
    typecast_dict = {
        **{
            column: typecast_type
            for typecast_type, column_list in typecast_dict.items()
            for column in column_list
        }
    }
    output_table = output_table.astype(typecast_dict).round(2)

    # convert to spark DataFrame
    spark = SparkSession.builder.getOrCreate()
    output_spark_df = spark.createDataFrame(output_table)

    return output_spark_df


def prepare_dashboard_performance_plot_data(
    ra_master: pandas.DataFrame,
    ra_performance_percentiles: pandas.DataFrame,
    master_table_columns: List[str],
    performance_percentile_columns: List[str],
    typecast_dict: Dict[str, Dict[str, str]],
    melt_id_vars: List[str],
) -> pyspark.sql.DataFrame:
    """Prepare coordinate points for each outlet for performance curves before saving to hive table

    Args:
        ra_master: Joined reseller master table
        ra_performance_percentiles: Contains performance percentile of outlets
        master_table_columns: Columns to extract from joined reseller master table
        performance_percentile_columns: Columns to extract from performance percentiles table
        typecast_dict: Dictionary containing columns and corresponding type to typecast to
        melt_id_vars: List of columns to perform pandas melt

    Returns:
        Tabular table with curve coordinates and other outlet features to be used for performance curve plots
    """

    # select columns needed for dashboard
    ra_master_selected = ra_master[["outlet_id", "month"] + master_table_columns]

    # obtain performance percentiles and similar outlets cashflows for outlets
    ra_performance_percentiles_selected = ra_performance_percentiles[
        ["outlet_id", "training_ground_truths_list"] + performance_percentile_columns
    ]

    # join tables
    intermediate_table = ra_performance_percentiles_selected.set_index(
        "outlet_id"
    ).join(ra_master_selected.set_index("outlet_id"), how="left")

    # cast column type to list before operations can be performed on them
    if isinstance(
        intermediate_table.reset_index().loc[0, "training_ground_truths_list"], str
    ):
        intermediate_table["training_ground_truths_list"] = intermediate_table[
            "training_ground_truths_list"
        ].map(lambda lst: ast.literal_eval(lst))

    # produce xy coordinates for curve
    for percentile in range(0, 101, 10):
        intermediate_table[
            f"curve_cashflow_percentile_{percentile}"
        ] = intermediate_table["training_ground_truths_list"].map(
            lambda lst: numpy.percentile(lst, percentile)
        )

    # output cols in order
    curve_coordinate_columns = [
        f"curve_cashflow_percentile_{percentile}" for percentile in range(0, 101, 10)
    ]
    output_cols = (
        ["outlet_id", "month"]
        + master_table_columns
        + performance_percentile_columns
        + curve_coordinate_columns
    )
    intermediate_table = intermediate_table.reset_index()[output_cols].drop_duplicates()

    # typecast columns
    typecast_dict = {
        **{
            column: typecast_type
            for typecast_type, column_list in typecast_dict.items()
            for column in column_list
        }
    }
    intermediate_table = intermediate_table.astype(typecast_dict)

    # transformations on DataFrame to format Tableau dashboard can digest
    # prepare final output
    output_table = intermediate_table.melt(
        id_vars=melt_id_vars, var_name="original_col_name", value_name="cashflow",
    )

    output_table["performance_percentile"] = output_table[
        ["performance_percentile", "original_col_name"]
    ].apply(
        lambda ele: float(ele.original_col_name.split("_")[-1])
        if ele.original_col_name.split("_")[-1] != "mean"
        else ele.performance_percentile,
        axis=1,
    )
    output_table["colour"] = output_table["original_col_name"].map(
        lambda ele: "black" if ele.split("_")[-1] != "mean" else "red"
    )

    # formatting
    output_table = (
        output_table.drop(columns=["original_col_name"])
        .sort_values(["month", "outlet_id", "colour", "performance_percentile"])
        .reset_index(drop=True)
        .round(2)
    )

    # convert to spark DataFrame
    spark = SparkSession.builder.getOrCreate()
    output_spark_df = spark.createDataFrame(output_table)

    return output_spark_df


def _parallel_build_trees(
    tree,
    forest,
    X,
    y,
    sample_weight,
    tree_idx,
    n_trees,
    verbose=0,
    class_weight=None,
    n_samples_bootstrap=None,
):
    """Monkey patch sklearn tree building function to allow retaining
    of the bootstrap sample indices for the quantile forest implementation
    created by Konstantinos and Tsakalis on 4/5/2017, updated by Jia Xiang on 14/5/2020

    Private function used to fit a single tree in parallel.
    """

    if verbose > 1:
        print("building tree %d of %d" % (tree_idx + 1, n_trees))

    tree.bootstrap_indices = None

    if forest.bootstrap:
        n_samples = X.shape[0]
        if sample_weight is None:
            curr_sample_weight = numpy.ones((n_samples,), dtype=numpy.float64)
        else:
            curr_sample_weight = sample_weight.copy()

        indices = _generate_sample_indices(
            tree.random_state, n_samples, n_samples_bootstrap
        )

        # retaining of bootstrap indices
        tree.bootstrap_indices = indices

        sample_counts = numpy.bincount(indices, minlength=n_samples)
        curr_sample_weight *= sample_counts

        if class_weight == "subsample":
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                curr_sample_weight *= compute_sample_weight("auto", y, indices)
        elif class_weight == "balanced_subsample":
            curr_sample_weight *= compute_sample_weight("balanced", y, indices)

        tree.fit(X, y, sample_weight=curr_sample_weight, check_input=False)
    else:
        tree.fit(X, y, sample_weight=sample_weight, check_input=False)

    return tree


def rmse(y_true, y_pred):
    return numpy.sqrt(mean_squared_error(y_true, y_pred))


def rse(y_true, y_pred, **kwargs):
    deno = y_true.shape[0] - kwargs["k"] - 1

    return numpy.sqrt(numpy.sum(numpy.square(y_true - y_pred)) / deno)


def rrse(y_true, y_pred, **kwargs):
    deno = y_true.shape[0] - kwargs["k"] - 1

    return numpy.sqrt(numpy.sum(numpy.square(y_true - y_pred)) / deno) / numpy.mean(
        y_true
    )


def auc_average(average: str, **kwargs: Dict[str, Any]):
    """Simple wrapper for sklearn roc_auc_score function, set to one-vs-one for multiclass

    Args:
        average: Average parameter; e.g. macro, weighted
        **kwargs: Additional keyword arguments

    Returns:
        Sklearn roc_auc_score function with specified average argument
    """

    def auc(y_true, y_score):
        return roc_auc_score(
            y_true, y_score, average=average, multi_class="ovo", **kwargs
        )

    return auc


sklearn_forest._parallel_build_trees = _parallel_build_trees
