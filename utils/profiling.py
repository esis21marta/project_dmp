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

import re
from functools import reduce

from pyspark import SparkContext, sql
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType, StructField, StructType

from .helpers import melt


def _build_qc_df_from_func(agg_function, df, label, column_dtype=None):
    """
    build quality funtion

    Args:
        agg_function: function aggregation need to apply
        df: dataframe which need to build quality data
        label:
        column_dtype:

    Returns:

    """
    ss = SparkSession.builder.getOrCreate()
    sc = SparkContext.getOrCreate()
    columns = []
    schema = [(x.name, str(x.dataType)) for x in df.schema.fields]

    if not column_dtype:
        columns = df.columns
    elif "string" in column_dtype:
        columns = columns + [x[0] for x in schema if x[1] in ["StringType"]]
    elif "numeric" in column_dtype:
        columns = columns + [
            x[0]
            for x in schema
            if re.match(
                "|".join(
                    [
                        "DecimalType",
                        "DoubleType",
                        "FloatType",
                        "IntegerType",
                        "LongType",
                        "ShortType",
                    ]
                ),
                x[1],
            )
        ]
    elif "date" in column_dtype:
        columns = columns + [
            x[0] for x in schema if x[1] in ["DateType", "TimestampType"]
        ]
    elif "bool" in column_dtype:
        columns = columns + [x[0] for x in schema if x[1] in ["BooleanType"]]
    elif "qty" in column_dtype:
        columns = columns + [x[0] for x in schema if "qty" in x[0]]
    else:
        raise ValueError("unsupported column_dtype argument: {}".format(column_dtype))

    if len(columns) == 0:
        output = ss.createDataFrame(
            sc.emptyRDD(),
            StructType(
                [StructField("field", StringType()), StructField(label, StringType())]
            ),
        )
    else:
        col_batch_list = [columns[x : x + 10] for x in range(0, len(columns), 10)]
        df_list = [
            df.agg(*[agg_function(x).alias(x) for x in column_batch])
            for column_batch in col_batch_list
        ]
        wrking_df = reduce(lambda x, y: x.crossJoin(y), df_list).withColumn(
            "temp", f.lit("DISCARD")
        )
        melted_df = (
            melt(wrking_df, ["temp"], columns)
            .drop("temp")
            .withColumnRenamed("value", label)
            .withColumnRenamed("variable", "field")
        )
        output = melted_df
    return output


def _generate_qc_summary_table(wrk_df: sql.DataFrame) -> sql.DataFrame:
    """
    function generate quality control of dataframe

    Args:
        wrk_df:dataframe which need to get the qc summary

    Returns:
        dataframe with qc summary
    """
    ss = SparkSession.builder.getOrCreate()
    aggregate_stats_pandas = [
        _build_qc_df_from_func(lambda x: f.count(f.col(x)), df=wrk_df, label="n"),
        _build_qc_df_from_func(
            lambda x: f.countDistinct(f.col(x)), df=wrk_df, label="n_distinct"
        ),
        _build_qc_df_from_func(
            lambda x: f.sum(f.when(f.col(x).isNull(), 1).otherwise(0)),
            df=wrk_df,
            label="is_null_cnt",
        ),
        _build_qc_df_from_func(
            lambda x: f.sum((f.col(x).isNotNull().cast("integer"))),
            df=wrk_df,
            label="is_not_null_cnt",
        ),
        _build_qc_df_from_func(
            lambda x: f.sum(f.col(x)).cast("string"),
            df=wrk_df,
            label="sum",
            column_dtype=["numeric"],
        ),
        _build_qc_df_from_func(
            lambda x: f.avg(f.col(x)).cast("string"),
            df=wrk_df,
            label="mean_val",
            column_dtype=["numeric"],
        ),
        _build_qc_df_from_func(
            lambda x: f.max(f.col(x)).cast("string"),
            df=wrk_df,
            label="max_val",
            column_dtype=["numeric", "date"],
        ),
        _build_qc_df_from_func(
            lambda x: f.min(f.col(x)).cast("string"),
            df=wrk_df,
            label="min_val",
            column_dtype=["numeric", "date"],
        ),
        _build_qc_df_from_func(
            lambda x: f.sum((f.col(x) == f.lit("")).cast("integer")),
            df=wrk_df,
            label="is_blank_count",
            column_dtype=["string"],
        ),
    ]
    total_rows = wrk_df.count()
    schema = [(x.name, str(x.dataType)) for x in wrk_df.schema.fields]
    dtypes_df = ss.createDataFrame(schema, ["field", "type"])
    aggregation_results = reduce(
        lambda x, y: x.join(y, "field", "outer"), aggregate_stats_pandas
    )
    reduced_df = dtypes_df.join(aggregation_results, "field", "left")

    missing_data_cols = ["is_null_cnt", "is_blank_count"]
    results_df = (
        reduced_df.withColumn(
            "overall_missing_values",
            reduce(
                lambda x, y: f.coalesce(f.col(x), f.lit(0))
                + f.coalesce(f.col(y), f.lit(0)),
                missing_data_cols,
            ),
        )
        .withColumn("total_rows", f.lit(total_rows))
        .withColumn(
            "overall_missing_pct",
            f.round((f.col("overall_missing_values") / f.col("total_rows")) * 100, 2),
        )
    )

    results_df = results_df.select(
        "field",
        f.col("total_rows").alias("tot_rows"),
        f.col("n_distinct").alias("distinct_vals"),
        "sum",
        f.col("mean_val").alias("mean"),
        f.col("max_val").alias("max"),
        f.col("min_val").alias("min"),
        f.col("overall_missing_values").alias("tot_missing"),
        f.col("overall_missing_pct").alias("perc_missing"),
    )

    return results_df


def generate_qc_summaries(args):
    """
    function which call the function generate quality control of dataframe

    Args:
        args:list of dataframe which need to get the qc summary

    Returns:
        dataframe with qc summary
    """
    return [_generate_qc_summary_table(df) for df in list(args)]


def count_nans(df):
    """
    Counts percentages of nulls, zeros, -1 per each week in 'weekstart'

    Args:
        df:dataframe which need calculated the nan values

    Returns:
        dataframe with calculated the nan values
    """
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

    return df_
