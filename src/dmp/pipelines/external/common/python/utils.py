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

from typing import Dict

# Possible dependencies used inside the filter_query
import numpy as np
import pai
import pandas as pd
import pyspark


def filter_down(df: pd.DataFrame, filter_query: str) -> pd.DataFrame:
    """
    Filters down the DataFrame using filter_query, if the query is empty returns the original DataFrame

    :param df: Input DataFrame
    :param filter_query: Filter query applied to the DataFrame
    :return: DataFrame containing a subset of rows
    """
    if filter_query:
        return df.query(filter_query, engine="python")
    else:
        return df


def spark_filter_down(
    df: pyspark.sql.DataFrame, filter_query: str
) -> pyspark.sql.DataFrame:
    """
    Filters down the DataFrame using filter_query, if the query is empty returns the original DataFrame

    :param df: Input DataFrame
    :param filter_query: Filter query applied to the DataFrame
    :return: DataFrame containing a subset of rows
    """
    if filter_query:
        return df.filter(filter_query)
    else:
        return df


def rename_columns(df: pd.DataFrame, column_mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Returns the dataframe with renamed columns

    :param df: Input dataframe
    :param column_mapping: The column to be renamed
    :return: The processed table
    """

    df = df.rename(column_mapping, axis=1)

    return df


def to_datetime(df: pd.DataFrame, date_column: str, date_format: str) -> pd.DataFrame:
    """
    Change date column to datetime format

    :param df: Input dataframe
    :param date_column: The name of the date column
    :param date_format: The format of the date column
    :return: DataFrame with corrected date column to datetime
    """

    df[date_column] = pd.to_datetime(df[date_column], format=date_format)

    return df


def add_empty_rows(df: pd.DataFrame, num_empty_rows: int = 1):
    """
    Returns a Dataframe with 'num_empty_rows' empty rows added at the end
    :param df: input Dataframe to which empty rows will be added
    :param num_empty_rows: Number of empty rows to add to the input Dataframe
    :return: A DataFrame with empty row(s) added to the end
    """
    return pd.concat(
        [
            df,
            pd.DataFrame(
                [[""] * df.shape[1]] * num_empty_rows,
                columns=df.columns,
                index=[""] * num_empty_rows,
            ),
        ]
    )


def get_lift_analysis_df(
    y_true: pd.Series, y_pred: pd.Series, num_bins: int = 10, prettify: bool = False
):
    """
    Return a Dataframe containing LIFT information

    :param y_true: Series of floats (actual values)
    :param y_pred: Series of floats (predicted values)
    :param num_bins: Number of bins of scores
    :param prettify: Whether to prettify the output Dataframe or not
    :return: A Dataframe containing LIFT information
    """

    count = y_true.size
    total_num_defaults = y_true.sum()
    ideal_model_coef = 1 / y_true.mean()
    res = pd.DataFrame({"y_true": y_true, "y_pred": y_pred})

    res["score_range"] = (
        pd.qcut(res["y_pred"], q=num_bins, duplicates="drop")
        .apply(
            lambda i: pd.Interval(
                np.round(max(0, i.left), decimals=2),
                np.round(i.right, decimals=2),
                i.closed,
            )
        )
        .astype(str)
    )
    res["score_index"] = pd.qcut(
        res["y_pred"], q=num_bins, labels=range(1, num_bins + 1), duplicates="drop"
    ).astype(int)

    res = (
        res.groupby(by="score_range")
        .agg(
            score_index=("score_index", "mean"),
            default_sum=("y_true", "sum"),
            population_sum=("y_true", "count"),
        )
        .sort_values(by="score_index", ascending=False)
        .drop(columns="score_index")
        .reset_index()
    )

    res["population_percent"] = 100 * res["population_sum"] / count
    res["default_percent"] = 100 * res["default_sum"] / total_num_defaults
    res = res[["score_range", "population_percent", "default_percent"]]
    res["cumulative_population_percent"] = res["population_percent"].cumsum()
    res["ideal_cumulative_default_percent"] = (
        res["cumulative_population_percent"] * ideal_model_coef
    ).map(lambda x: min(x, 100))
    res["cumulative_default_percent"] = res["default_percent"].cumsum()
    res["our_model_lift"] = (
        res["cumulative_default_percent"] / res["cumulative_population_percent"]
    )

    if prettify:
        for col in [
            "population_percent",
            "default_percent",
            "cumulative_population_percent",
            "ideal_cumulative_default_percent",
            "cumulative_default_percent",
            "our_model_lift",
        ]:
            res[col] = res[col].map("{:,.2f}%".format)

    return res


def _woe(bin_target: pd.Series, overall_count_1: int, overall_count_0: int) -> float:
    """
    Returns the weight of evidence for a bin

    :param bin_target: Pandas Series of target values in the bin
    :param overall_count_1: Overall number of 1
    :param overall_count_0: Overall number of 0
    :return: The weight of evidence for the input Series
    """

    values = dict(bin_target.value_counts())
    fraction_0 = values.get(0, 0) / overall_count_0
    fraction_1 = values.get(1, 0) / overall_count_1

    if fraction_0 == 0:
        return 100  # np.inf
    elif fraction_1 == 0:
        return -100  # -np.inf
    else:
        return np.log(fraction_1 / fraction_0)


def get_score_adjustment_stats(
    y_true, y_pred, adjustment_type, num_bins, bin_type
) -> pd.DataFrame:
    """
    Returns a DataFrame containing the score range and the related scores adjusted using the adjustment_type

    :param y_true: Series of floats (actual values)
    :param y_pred: Series of floats (predicted values)
    :param adjustment_type: Type of adjustment ('avg_raw_proba' or 'woe')
    :param num_bins: Number of bins of scores
    :param bin_type: Type of bins to use ('quantile' or 'equal_size')
    :return: DataFrame containing the score range and the related scores adjusted using the adjustment_type
    """

    df = pd.DataFrame({"y_true": y_true, "y_pred": y_pred})

    if bin_type == "quantile":
        df["score_range"] = pd.qcut(df["y_pred"], q=num_bins)
        df["score_index"] = pd.qcut(
            df["y_pred"], q=num_bins, labels=range(1, num_bins + 1)
        ).astype(int)
    else:
        df["score_range"] = pd.cut(df["y_pred"], bins=num_bins)
        df["score_index"] = pd.cut(
            df["y_pred"], bins=num_bins, labels=range(1, num_bins + 1)
        ).astype(int)

    y_true_values = dict(y_true.value_counts())
    overall_count_1 = y_true_values.get(1, 0)
    overall_count_0 = y_true_values.get(0, 0)

    stats_df = df.groupby(by="score_range")
    if adjustment_type == "woe":
        stats_df = stats_df.agg(
            score_index=("score_index", "mean"),
            adjusted_score=(
                "y_true",
                lambda x: _woe(
                    x, overall_count_1=overall_count_1, overall_count_0=overall_count_0
                ),
            ),
        )
    else:
        stats_df = stats_df.agg(
            score_index=("score_index", "mean"), adjusted_score=("y_pred", "mean")
        )
    stats_df = stats_df.rename(
        columns={"adjusted_score": adjustment_type}
    ).reset_index()

    return stats_df


def adjust_raw_probability(
    y_true: pd.Series,
    y_pred: pd.Series,
    adjustment_type: str,
    num_bins: int,
    bin_type: str,
) -> pd.Series:
    """
    Returns a Series containing the scores adjusted using the adjustment_type

    :param y_true: Series of floats (actual values)
    :param y_pred: Series of floats (predicted values)
    :param adjustment_type: Type of adjustment ('raw_proba' or 'avg_raw_proba' or 'woe')
    :param num_bins: Number of bins of scores
    :param bin_type: Type of bins to use ('quantile' or 'equal_size')
    :return: Pandas Series containing the scores adjusted using the adjustment_type
    """
    if adjustment_type == "raw_proba":
        return y_pred

    if bin_type not in ["quantile", "equal_size"]:
        raise Exception(f"Unknown bin type ('{bin_type}')")

    if adjustment_type not in ["avg_raw_proba", "woe"]:
        raise Exception(f"Unknown group type ('{adjustment_type}')")

    df = pd.DataFrame({"y_pred": y_pred}, index=y_pred.index)
    if bin_type == "quantile":
        df["score_range"] = pd.qcut(df["y_pred"], q=num_bins)
    else:
        df["score_range"] = pd.cut(df["y_pred"], bins=num_bins)

    stats_df = get_score_adjustment_stats(
        y_true, y_pred, adjustment_type, num_bins, bin_type
    )
    df = df.merge(stats_df, how="left", on="score_range")

    return df[adjustment_type]


def add_pai_tag(tag):
    pai.add_tags(tag)
