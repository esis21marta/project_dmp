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

from typing import List

import pyspark
import pyspark.sql.functions as f
from pyspark.sql.types import LongType

from src.dmp.pipelines.dmp._06_qa.metrics.metrics_helper import (
    convert_array_type_column_to_array_length,
    convert_percentiles_list_into_separate_columns,
    get_column_name_of_type,
    melt_qa_result,
)

_null_count = lambda x: f.count(f.when(f.col(x).isNull(), 1))
_negative_count = lambda x: f.count(f.when(f.col(x) < 0, 1))


def get_monthly_unique_msisdn(df: pyspark.sql.DataFrame):
    """
    Calculate monthly unique msisdn
    Args:
        df (pyspark.sql.DataFrame): df with msisdn and weekstart

    Returns:
        df with month and unique_msisdn_cnt column
    """
    df = df.withColumn("month", f.trunc("weekstart", "month"))
    df = (
        df.groupBy("month")
        .agg(f.countDistinct("msisdn").alias("unique_msisdn_cnt"))
        .orderBy("month")
    )
    return df


def get_outlier(
    df: pyspark.sql.DataFrame, df_metrics: pyspark.sql.DataFrame, columns: list = []
):
    """
    Returns a melted dataframe containing higher and lower outlier counts using IQR method for values in that weekstart.
    Args:
        df: Raw dataframe.
        df_metrics: Metrics dataframe.
        columns: Filtered columns

    Returns:
        Dataframe with columns of count
    """
    # If columns are not passed then all columns except msisdn and weekstart are considered columns
    if not columns:
        columns = list(set(df.columns) - {"msisdn", "weekstart"})

    # Select only columns and msisdn and weekstart
    df = df.select(columns + ["msisdn", "weekstart"])

    # If Array exists in df, convert array to array length
    df_with_array_length = convert_array_type_column_to_array_length(df)

    numeric_columns = set(
        get_column_name_of_type(df_with_array_length, required_types=["numeric"])
    ) - {"msisdn", "weekstart"}

    if len(numeric_columns) == 0:
        return df_metrics.select(
            f.col("weekstart"),
            f.col("columns"),
            f.lit(None).cast(LongType()).alias("count_higher_outlier"),
            f.lit(None).cast(LongType()).alias("count_lower_outlier"),
        )

    df_metrics = (
        df_metrics.select("weekstart", "columns", "percentile_0_25", "percentile_0_75")
        .groupBy("weekstart")
        .pivot("columns")
        .agg(
            f.first(f.col("percentile_0_25")).alias("q1"),
            f.first(f.col("percentile_0_75")).alias("q3"),
            (
                f.first(f.col("percentile_0_75")) - f.first(f.col("percentile_0_25"))
            ).alias("iqr"),
        )
    )

    joined_df = df_with_array_length.join(
        f.broadcast(df_metrics), on="weekstart", how="inner"
    )

    outlier_results = joined_df.groupBy(["weekstart"]).agg(
        *[
            f.count(
                f.when(
                    f.col(num_col)
                    < (f.col(f"{num_col}_q1") - (1.5 * (f.col(f"{num_col}_iqr")))),
                    1,
                )
            )
            .cast(LongType())
            .alias(f"{num_col}__count_lower_outlier")
            for num_col in numeric_columns
        ],
        *[
            f.count(
                f.when(
                    f.col(num_col)
                    > (f.col(f"{num_col}_q3") + (1.5 * (f.col(f"{num_col}_iqr")))),
                    1,
                )
            )
            .cast(LongType())
            .alias(f"{num_col}__count_higher_outlier")
            for num_col in numeric_columns
        ],
    )

    outlier_results_melted = melt_qa_result(outlier_results)
    return outlier_results_melted.repartition(1)


def get_accuracy_completeness_metrics(
    df: pyspark.sql.DataFrame,
    percentiles: List[float],
    percentiles_accuracy: float,
    columns: List[str] = None,
) -> pyspark.sql.DataFrame:
    """
    Get qa checks for every week and for every columns

    Args:
        df (pyspark.sql.DataFrame): df on which we have to apply the aggregates
        columns (list): columns on which we want to run this metrics. Default it will run on all the columns
        percentiles (list): Percentiles which we want to calculate. Default it will not calculate percentiles
        percentiles_accuracy (float): Accuracy for percentiles. Delault 1000
    Return:
        df: df with all the aggregates
    """

    # If columns are not passed then all columns except msisdn and weekstart are considered columns
    if columns is None:
        columns = list(set(df.columns) - set(("msisdn", "weekstart")))

    # Select only columns and msisdn and weekstart
    df = df.select(columns + ["msisdn", "weekstart"])

    # If Array exists in df, convert array to array length
    df_with_array_length = convert_array_type_column_to_array_length(df)

    numeric_columns = set(
        get_column_name_of_type(df_with_array_length, required_types=["numeric"])
    ) - set(("msisdn", "weekstart"))

    qa_result = df_with_array_length.groupby(["weekstart"]).agg(
        *[
            f.max(col).alias(f"{col}__max") for col in numeric_columns
        ],  # MAX applicable only on numeric data type
        *[
            f.min(col).alias(f"{col}__min") for col in numeric_columns
        ],  # MIN applicable only on numeric data type
        *[
            f.round(f.mean(col), 4).alias(f"{col}__mean") for col in numeric_columns
        ],  # MEAN applicable only on numeric data type
        *[
            _null_count(col).alias(f"{col}__null_count") for col in columns
        ],  # NULL COUNT applicable for all data types
        f.count("msisdn").alias("row_count"),  # COUNT applicable only for MSISDN
        *[
            _negative_count(col).alias(f"{col}__negative_count")
            for col in numeric_columns
        ],  # NEGATIVE COUNT applicable only for numeric data type
        *[
            f.expr(
                f"percentile_approx({col}, array({','.join(map(str, percentiles))}), {percentiles_accuracy})"
            ).alias(f"{col}__percentiles")
            for col in numeric_columns  # PERCENTILES applicable only for numeric data type
        ],
    )

    qa_result_melted = melt_qa_result(qa_result)
    qa_result_melted = qa_result_melted.join(
        qa_result.select(["weekstart", "row_count"]), how="inner", on=["weekstart"]
    )
    qa_result_melted = qa_result_melted.withColumn(
        "null_percentage", f.col("null_count") / f.col("row_count")
    ).drop("null_count")

    if "negative_count" in qa_result_melted.columns:
        qa_result_melted = qa_result_melted.withColumn(
            "negative_value_percentage", f.col("negative_count") / f.col("row_count")
        ).drop("negative_count")

    if "percentiles" in qa_result_melted.columns:
        qa_result_melted = convert_percentiles_list_into_separate_columns(
            qa_result_melted, percentiles
        )

    return qa_result_melted.repartition(5)
