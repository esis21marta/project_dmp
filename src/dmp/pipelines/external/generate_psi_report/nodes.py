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

from typing import Any, Dict, List

import numpy as np
import pandas as pd
import pyspark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def _psi(baseline_col: float, current_col: float) -> float:
    """
    PSI Formula

    :param baseline_col: The baseline proportion value
    :param current_col: The current proportion value
    :return: PSI
    """
    psi = (current_col - baseline_col) * np.log(current_col / baseline_col)
    return float(psi)


def calculate_psi(
    df: pyspark.sql.DataFrame, bin_threshold: Dict[Any, Any], column_to_bin: str
):
    """
    Calculate the PSI based on the pre-defined binning threshold

    :param df: Scoring dataframe which contains features and scores
    :param bin_threshold: Dictionary of bins and the min,max,proportion values for the bins
    :param column_to_bin: The name of the feature to bin
    :return: PSI per features and score
    """
    when_condition = []
    for key, val in bin_threshold.items():
        max_val = val["max"] if val["max"] != "inf" else np.inf
        min_val = val["min"] if val["min"] != "-inf" else -np.inf
        when_condition.append(
            f.when(f.col(column_to_bin).between(min_val, max_val), key)
        )

    select_cols = ["weekstart", "created_at", "refresh_date", "model_id"]
    df = df.select(*(select_cols + [column_to_bin])).fillna(0)
    df = df.select("*", f.coalesce(*when_condition).alias("bins"))

    agg_bins = (
        df.groupby(select_cols + ["bins"])
        .count()
        .withColumn("dimension", f.lit(column_to_bin))
        .withColumnRenamed("count", "count_current")
        .toPandas()
    )

    return agg_bins


def calculate_psi_all(
    df: pyspark.sql.DataFrame, features_binning: Dict[str, Any]
) -> pyspark.sql.DataFrame:
    """
    Iterate over the features and calculate the PSI for each of them

    :param df: Scoring dataframe which contains features and scores
    :param features_binning: The binning thresholds for each features
    :return: PSI per features and score
    """

    psi_outputs = pd.DataFrame()
    for column_to_bin, bins in features_binning.items():
        psi_output = calculate_psi(df, bins, column_to_bin)

        n_repeat = len(bins.keys())
        pd_bins = pd.DataFrame(bins.keys(), columns=["bins"])

        base_bins = psi_output.drop(["bins", "count_current"], axis=1).head(1)
        base_bins_col = list(base_bins.columns)

        base_bins = pd.concat([base_bins] * n_repeat, ignore_index=True).join(pd_bins)
        psi_output = pd.merge(
            base_bins, psi_output, on=base_bins_col + ["bins"], how="left"
        ).fillna(1)
        psi_output["proportion_baseline"] = psi_output.apply(
            lambda x: features_binning[x["dimension"]][x["bins"]]["proportion"], axis=1
        )
        psi_output["proportion_current"] = (
            psi_output["count_current"] / psi_output["count_current"].sum()
        )
        psi_output["psi"] = psi_output.apply(
            lambda x: _psi(x["proportion_baseline"], x["proportion_current"]), axis=1
        )
        psi_outputs = pd.DataFrame.append(psi_outputs, psi_output)

    spark = SparkSession.builder.getOrCreate()
    psi_outputs = spark.createDataFrame(psi_outputs)

    return psi_outputs


def filter_psi_columns(
    df: pyspark.sql.DataFrame, columns_to_include: List
) -> pyspark.sql.DataFrame:
    """
    Get the summary of the PSI

    :param df: PSI tables
    :param columns_to_include: Include the columns
    :return: PSI summary report
    """

    df = df.select(columns_to_include)

    return df
