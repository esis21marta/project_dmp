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

from collections import Counter
from typing import List, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyspark
import pyspark.sql.functions as f
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.functions import col, lit, size, when


def select_df_to_downsampled_or_train(
    fg_master_complete_training,
    fg_master_complete_downsampled,
    lte_map,
    weekstart,
    fg_downsampled_or_train_tag,
) -> pyspark.sql.DataFrame:
    """
    The previous node will have decided whether to run the pipeline for scoring or training
    The scoring pipeline utilises the whole master table, while the training one uses the downsampled version

    :param fg_master_complete: master table (dmp master table + lte map + network features)
    :param fg_master_complete_downsampled: dowsampled master table (dmp master table + lte map + network features)
    :param fg_score_or_train_tag: tag to choose the right dataframe
    :return: select one of the data frames
    """
    assert fg_downsampled_or_train_tag in ("train", "downsampled")
    if fg_downsampled_or_train_tag == "train":
        cols = ["msisdn"]
        fg_master_complete_training = fg_master_complete_training.dropDuplicates(
            cols
        ).withColumn("weekstart", lit(weekstart))
        lte_map = lte_map.dropDuplicates(cols).select(["msisdn", "segment"])
        fg_master_complete_training = fg_master_complete_training.join(
            lte_map, cols, how="inner"
        )
        return fg_master_complete_training
    else:
        return fg_master_complete_downsampled


def pick_a_segment_and_month(spark_table, segment, weekstart) -> pyspark.sql.DataFrame:
    """
    weekstart is the last monday of the desired month
    :param spark_table:
    :param segment: SEGMENT_2, SEGMENT_3, SEGMENT_4,...
    :param weekstart: last monday of the desired month
    :return:
    """
    res = spark_table.filter(spark_table.segment == segment)
    res = res.filter(res.weekstart == weekstart)
    return res


def count_nans(df: pyspark.sql.DataFrame, method, threshold):
    count_all_map = [f.count("weekstart").alias("count")]
    count_nulls = [
        f.count(f.when(df[c].isNull(), c)).alias("nulls__" + c)
        for c, type_ in df.dtypes
        if c != "weekstart"
    ]
    count_zeros = [
        f.count(f.when(df[c] == 0, c)).alias("zeros__" + c)
        for c, type_ in df.dtypes
        if (type_ in ["int", "bigint", "double"]) or ("decimal" in type_)
    ]
    count_minus1 = [
        f.count(f.when(df[c] == -1, c)).alias("minus1__" + c)
        for c, type_ in df.dtypes
        if (type_ in ["int", "bigint", "double"]) or ("decimal" in type_)
    ]
    df_ = df.groupby("weekstart").agg(
        *(count_all_map + count_nulls + count_zeros + count_minus1)
    )
    df_ = df_.select(
        *(
            [
                (df_["nulls__" + c] / df_["count"]).alias("p_nulls__" + c)
                for c, type_ in df.dtypes
                if c != "weekstart"
            ]
            + [
                (df_["zeros__" + c] / df_["count"]).alias("p_zeros__" + c)
                for c, type_ in df.dtypes
                if (type_ in ["int", "bigint", "double"]) or ("decimal" in type_)
            ]
            + [
                (df_["minus1__" + c] / df_["count"]).alias("p_minus1__" + c)
                for c, type_ in df.dtypes
                if (type_ in ["int", "bigint", "double"]) or ("decimal" in type_)
            ]
            + ["weekstart"]
        )
    )

    # to pandas
    df_pandas = pd.DataFrame(df_.collect(), columns=df_.columns)
    # check how many null values each features
    null_cols = [c for c in df_pandas.columns if ("p_nulls" in c)]
    df_null = df_pandas[null_cols]
    df_null = df_null.describe()
    df_null = df_null.transpose()[["mean", "50%"]]
    df_null.columns = ["mean", "median"]
    df_null[f"threshold_{threshold}"] = [
        1 if i > threshold else 0 for i in df_null[method]
    ]
    df_null = df_null.reset_index().rename(columns={"index": "features"})
    df_null["features"] = df_null["features"].str.replace("p_nulls__", "")
    df_null_above = df_null.loc[df_null[f"threshold_{threshold}"] == 1]
    df_null_under = df_null.loc[df_null[f"threshold_{threshold}"] == 0]
    list(df_null_above["features"])
    null_under_threshold = list(df_null_under["features"])
    # check how many zeros values each features
    zeros_cols = [c for c in df_pandas.columns if ("p_zeros" in c)]
    df_zeros = df_pandas[zeros_cols]
    df_zeros = df_zeros.describe()
    df_zeros = df_zeros.transpose()[["mean", "50%"]]
    df_zeros.columns = ["mean", "median"]
    df_zeros[f"threshold_{threshold}"] = [
        1 if i > threshold else 0 for i in df_zeros[method]
    ]
    df_zeros = df_zeros.reset_index().rename(columns={"index": "features"})
    df_zeros["features"] = df_zeros["features"].str.replace("p_zeros__", "")
    df_zeros_above = df_zeros.loc[df_zeros[f"threshold_{threshold}"] == 1]
    df_zeros_under = df_zeros.loc[df_zeros[f"threshold_{threshold}"] == 0]
    list(df_zeros_above["features"])
    zeros_under_threshold = list(df_zeros_under["features"])
    # check how many minus1 values each features
    minus1_cols = [c for c in df_pandas.columns if ("p_minus1" in c)]
    df_minus1 = df_pandas[minus1_cols]
    df_minus1 = df_minus1.describe()
    df_minus1 = df_minus1.transpose()[["mean", "50%"]]
    df_minus1.columns = ["mean", "median"]
    df_minus1[f"threshold_{threshold}"] = [
        1 if i > threshold else 0 for i in df_minus1[method]
    ]
    df_minus1 = df_minus1.reset_index().rename(columns={"index": "features"})
    df_minus1["features"] = df_minus1["features"].str.replace("p_minus1__", "")
    df_minus1_above = df_minus1.loc[df_minus1[f"threshold_{threshold}"] == 1]
    df_minus1_under = df_minus1.loc[df_minus1[f"threshold_{threshold}"] == 0]
    list(df_minus1_above["features"])
    minus1_under_threshold = list(df_minus1_under["features"])

    return null_under_threshold, zeros_under_threshold, minus1_under_threshold


def cast_decimals_to_float(
    spark_table, fg_score_or_train_tag, fg_cols_under_null
) -> pyspark.sql.DataFrame:
    for col, type_ in spark_table.dtypes:
        if "decimal" in type_:
            spark_table = spark_table.withColumn(col, spark_table[col].cast("float"))
    assert fg_score_or_train_tag in ("train", "score")
    fg_cols_under_null = fg_cols_under_null + ["fea_int_usage_4g_kb_data_usage_01m"]
    if fg_score_or_train_tag == "train":
        return spark_table.limit(1000000)
    else:
        return spark_table


def subset_features(
    table: Union[pd.DataFrame, pyspark.sql.DataFrame], feats_to_use: List[int],
) -> Union[pd.DataFrame, pyspark.sql.DataFrame]:

    if isinstance(table, pd.DataFrame):
        res = table.loc[:, feats_to_use]
    elif isinstance(table, SparkDataFrame):
        res = table.select(feats_to_use)
    else:
        raise NotImplementedError()
    return res


def specific_transformations(
    table: Union[pd.DataFrame, pyspark.sql.DataFrame],
) -> Union[pd.DataFrame, pyspark.sql.DataFrame]:
    """
    - transforming `fea_handset_imeis` (list of handsets) to integer (length of the list of handsets)
    :param table: master table
    :return:
    """
    col_name = "fea_handset_imeis"
    if col_name in table.columns:
        if isinstance(table, pd.DataFrame):
            table.loc[:, col_name] = [
                0 if l is None else len(l) for l in table[col_name]
            ]
        elif isinstance(table, SparkDataFrame):
            table = table.withColumn(col_name, size(col_name))
            table = table.withColumn(
                col_name, when(col(col_name) == -1, 0).otherwise(col(col_name))
            )
        return table
    else:
        return table


def to_pandas(spark_table: pyspark.sql.DataFrame) -> pd.DataFrame:
    res = pd.DataFrame(spark_table.collect(), columns=spark_table.columns)
    return res


def remove_rows_with_positive_4g_usage(df: pd.DataFrame) -> pd.DataFrame:
    for col in [c for c in df.columns if "int_usage_4g_kb_data_usage_01m" in c]:
        res = df.loc[~(df[col] > 0), :]
    res = res.drop("fea_int_usage_4g_kb_data_usage_01m", axis=1).reset_index(drop=True)
    return res


def remove_unit_of_analysis(
    pandas_table: pd.DataFrame, cols_to_drop: List[str]
) -> pd.DataFrame:

    """
    drop columns refering to the unit of analysis
    :param pandas_table:
    :param cols_to_drop: columns refering to the unit of analysis
    :return:
    """
    res = pandas_table.drop(cols_to_drop, axis=1)

    return res


def transforming_columns(
    table: pd.DataFrame,
    columns_to_log: List[str],
    imputation_criteria: str,
    limit_manufacture: int,
    fg_score_or_train_tag,
) -> pd.DataFrame:
    # imputation
    for cols in table.loc[
        :, [col for col, ty in table.dtypes.items() if ty == "O"]
    ].columns.tolist():
        table.loc[:, cols] = table.loc[:, cols].fillna("unknown").astype("str")
    imputation_criteria = {"zero": lambda df: 0, "mean": lambda df: df.mean()}[
        imputation_criteria
    ]
    table = table.fillna(imputation_criteria(table))
    assert fg_score_or_train_tag in ("train", "score")
    if fg_score_or_train_tag == "train":
        return table
    else:
        # logging columns
        table.loc[:, columns_to_log] = np.log(1 + table.loc[:, columns_to_log])
        return table
    # limiting categories of the "manufacture" feature
    if limit_manufacture > 0:
        manuf_col_name = ""
        if "manufacture" in table.columns:
            manuf_col_name = "manufacture"
            return manuf_col_name
        elif "manufacturer" in table.columns:
            manuf_col_name = "manufacturer"
            return manuf_col_name
        elif manuf_col_name == "":
            return table
        else:
            raise NotImplementedError()
        manuf_count = Counter(table[manuf_col_name]).most_common(limit_of_categories)
        many_have_it = set([manuf for manuf, count in manuf_count])
        table.manufacture = table.manufacture.map(
            dict(
                (el, el) if el in many_have_it else (el, "RARE_DEVICE")
                for el in table[manuf_col_name]
            )
        )
        return table
    else:
        raise NotImplementedError()

    return table


def plot_histogram(table: pd.DataFrame, path_to_save_hist: str) -> None:
    table.hist(figsize=(45, 45))
    plt.savefig(path_to_save_hist)
