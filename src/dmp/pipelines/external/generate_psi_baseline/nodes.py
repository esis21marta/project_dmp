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

import numpy as np
import pyspark
import pyspark.sql.functions as f
from optbinning import BinningProcess
from pyspark.sql.window import Window


def calculate_csi_baseline(
    df: pyspark.sql.DataFrame,
    columns_of_interests: list,
    class_column_name: str,
    max_bin: int,
    filter_dataset: bool,
    filter_column: str,
    first_period: str,
    last_period: str,
):
    """
    Perform binning by using optbinning on the selected features

    :param df: Dataframe containing the features and the class label to be used
    :param columns_of_interests: List of features names and the class column
    :param class_column_name: The name of the column for class label, to be used to optimize the binning
    :param filter_dataset: Whether we want to filter the dataset or not
    :param filter_column: The name of the column to filter (date column)
    :param first_period: The beginning of the period to filter
    :param last_period: The last of the period to filter
    :return: YAML file containing the min, max, and proportion of each features and each bins
    """
    if filter_dataset:
        df = df.filter(f.col(filter_column).between(first_period, last_period))

    df = df.select(columns_of_interests).fillna(0).toPandas()
    features_only = [fea for fea in columns_of_interests if fea.startswith("fea_")]

    for i in features_only:
        df[i] = df[i].astype(float)

    x = df[features_only].values
    y = df[class_column_name].values
    binning_process = BinningProcess(features_only, max_n_bins=5)
    binning_process.fit(x, y)

    dict_of_bins = {}

    for fea in features_only:
        dict_of_bins[fea] = {}

        optb = binning_process.get_binned_variable(fea)
        optb_table = optb.binning_table.build()

        max_splits = optb.binning_table.splits.tolist()
        min_splits = optb.binning_table.splits.tolist()

        max_splits.append("inf")
        min_splits.insert(0, "-inf")
        proportion = optb_table["Count"][:-3].values / np.sum(
            optb_table["Count"][:-3].values
        )

        for num, bins in enumerate(zip(min_splits, max_splits, proportion)):
            dict_of_bins[fea][num] = {}
            dict_of_bins[fea][num]["min"] = bins[0]
            dict_of_bins[fea][num]["max"] = bins[1]
            dict_of_bins[fea][num]["proportion"] = float(np.round(bins[2], 4))

    return dict_of_bins


def calculate_psi_baseline(
    df: pyspark.sql.DataFrame,
    column_to_bin: str,
    num_of_bins: int,
    filter_dataset: bool,
    filter_column: str,
    first_period: str,
    last_period: str,
):
    """
    Perform binning using n-tile based on the num_of_bins

    :param df: Dataframe containing data to bin
    :param column_to_bin: The name of the column to be binned
    :param num_of_bins: The number of bins to be produced
    :param filter_dataset: Whether we want to filter the dataset or not
    :param filter_column: The name of the column to filter (date column)
    :param first_period: The beginning of the period to filter
    :param last_period: The last of the period to filter
    :return: YAML file containing the min, max, and proportion of the column and bins
    """
    if filter_dataset:
        df = df.filter(f.col(filter_column).between(first_period, last_period))

    df = df.select(column_to_bin)
    df = df.withColumn(
        "bins",
        f.ntile(num_of_bins).over(Window.partitionBy().orderBy(f.col(column_to_bin))),
    )
    binning = (
        df.groupby("bins")
        .agg(
            f.min(f.col(column_to_bin)).alias("min"),
            f.max(f.col(column_to_bin)).alias("max"),
            f.count(f.col(column_to_bin)).alias("count_baseline"),
        )
        .toPandas()
    )

    binning["proportion"] = binning["count_baseline"] / binning["count_baseline"].sum()
    binning_list = binning.set_index(["bins"]).to_dict("record")

    dict_of_bins = {}

    dict_of_bins[column_to_bin] = {}
    n_element = len(binning_list)
    for num, bins in enumerate(binning_list):
        dict_of_bins[column_to_bin][num] = {}
        dict_of_bins[column_to_bin][num]["min"] = bins["min"] if num > 0 else "-inf"
        dict_of_bins[column_to_bin][num]["max"] = (
            bins["max"] if num < (n_element - 1) else "inf"
        )
        dict_of_bins[column_to_bin][num]["proportion"] = float(
            np.round(bins["proportion"], 4)
        )

    return dict_of_bins
