import numpy as np
import pandas as pd
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from src.dmp.pipelines.cvm._4g_stimulation._2_dim_reduction_and_clustering._1_data_cleaning import (
    cast_decimals_to_float,
    subset_features,
    to_pandas,
    transforming_columns,
)

conf = (
    SparkConf()
    .setMaster("local")
    .setAppName("dmp-unit-test")
    .set("spark.default.parallelism", "1")
    .set("spark.sql.shuffle.partitions", "1")
    .set("spark.shuffle.service.enabled", "false")
    .set("spark.sql.catalogImplementation", "hive")
)
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)

example_raw_df_list = [
    ["23456478", "201912", "apple", 10001, 1000222.0, 1],
    ["06333335", "201912", "apple", 10004, None, 2],
    ["+3323445", "201912", "samsung", 10001, float("nan"), 3],
    ["+3323445", "201912", "apple", None, 10002.0, 4],
]

example_raw_df = spark.createDataFrame(
    example_raw_df_list, ["msisdn", "month", "manufacture", "a", "b", "c"]
)


def test_preprocessing():
    df_pandas = to_pandas(example_raw_df)

    assert df_pandas.equals(
        pd.DataFrame(
            [
                ["23456478", "201912", "apple", 10001.0, 1000222.0, 1],
                ["06333335", "201912", "apple", 10004.0, np.nan, 2],
                ["+3323445", "201912", "samsung", 10001.0, np.nan, 3],
                ["+3323445", "201912", "apple", np.nan, 10002.0, 4],
            ],
            columns=["msisdn", "month", "manufacture", "a", "b", "c"],
        )
    )

    df_subset_feat = subset_features(df_pandas, ["manufacture", "a", "b"])

    assert df_subset_feat.equals(
        pd.DataFrame(
            [
                ["apple", 10001.0, 1000222.0],
                ["apple", 10004.0, np.nan],
                ["samsung", 10001.0, np.nan],
                ["apple", np.nan, 10002.0],
            ],
            columns=["manufacture", "a", "b"],
        )
    )

    df_transformed, num_cols, cat_cols = transforming_columns(
        df_subset_feat,
        columns_to_log=["b"],
        imputation_criteria="zero",
        limit_manufacture=1,
    )
    assert df_transformed.equals(
        pd.DataFrame(
            [
                ["apple", 10001.0, 13.81573353310347],
                ["apple", 10004.0, 0.0],
                ["RARE_DEVICE", 10001.0, 0.0],
                ["apple", 0.0, 9.21064032698518],
            ],
            columns=["manufacture", "a", "b"],
        )
    )

    assert cat_cols == ["manufacture"]
    assert num_cols == ["a", "b"]


def test_converting_types_to_numerical():
    df = example_raw_df
    for col in ["a", "b", "c"]:
        df = df.withColumn(col, df[col].cast("decimal"))
    # df.show()
    df = cast_decimals_to_float(df)
    # df.show()
    df_expected = pd.DataFrame(
        [
            ["23456478", "201912", "apple", 10001.0, 1000222.0, 1.0],
            ["06333335", "201912", "apple", 10004.0, np.nan, 2.0],
            ["+3323445", "201912", "samsung", 10001.0, np.nan, 3.0],
            ["+3323445", "201912", "apple", np.nan, 10002.0, 4.0],
        ],
        columns=["msisdn", "month", "manufacture", "a", "b", "c"],
    )
    assert ((df.toPandas() == df_expected) | df.toPandas().isnull()).all(axis=None)
