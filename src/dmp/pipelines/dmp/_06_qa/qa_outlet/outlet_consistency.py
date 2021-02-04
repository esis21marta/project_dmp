from typing import List

import pyspark
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType

from src.dmp.pipelines.dmp._06_qa.metrics.metrics_helper import melt_qa_result
from utils import add_prefix_suffix_to_df_columns

_is_same = lambda x: (
    (f.isnull(f.col(f"{x}_old")) & f.isnull(f.col(f"{x}_new")))
    | (f.col(f"{x}_old") == f.col(f"{x}_new"))
).cast(IntegerType())


def get_same_percent(
    df_new: pyspark.sql.DataFrame,
    df_old: pyspark.sql.DataFrame,
    non_feature_columns: List[str],
    aggregate_columns: List[str],
) -> pyspark.sql.DataFrame:
    """
    Count number of matching values
    Args:
        df_new (pyspark.sql.DataFrame): Old dataframe
        df_old (pyspark.sql.DataFrame): New dataframe

    Returns:
        df (pyspark.sql.DataFrame)
    """

    # Get same columns in old and new
    df_new_cols = df_new.columns
    df_old_cols = df_old.columns
    columns_same_in_old_to_new = set(df_new_cols).intersection(set(df_old_cols))

    # If none of the columns are same throw error
    if len(columns_same_in_old_to_new) == 0:
        raise ValueError("No matching columns between old and new master table")

    # Select common columns
    common_columns = list(columns_same_in_old_to_new)
    df_new = df_new.select(common_columns)
    df_old = df_old.select(common_columns)

    feature_columns = list(columns_same_in_old_to_new - set(non_feature_columns))

    # Add old and new suffix to columns and join for same msisdn and weekstart
    df_new = add_prefix_suffix_to_df_columns(
        df_new, suffix="_new", columns=feature_columns
    )
    df_old = add_prefix_suffix_to_df_columns(
        df_old, suffix="_old", columns=feature_columns
    )
    df_old_new_merged = df_new.join(df_old, on=non_feature_columns, how="inner")

    # Check if old and new value are same or null
    for column in feature_columns:
        df_old_new_merged = df_old_new_merged.withColumn(
            f"{column}_is_eq", _is_same(column)
        )

    # Select only non_feature_columns and _is_eq columns
    df_old_new_merged = df_old_new_merged.select(
        non_feature_columns
        + [
            f.col(col).alias(col.replace("_is_eq", ""))
            for col in df_old_new_merged.columns
            if col.endswith("_is_eq")
        ]
    )

    df_old_new_merged = df_old_new_merged.na.fill(0)

    # Count the number or same records for each column
    df_same_percent = df_old_new_merged.groupby(aggregate_columns).agg(
        *[f.mean(col).alias(f"{col}__same_percent") for col in feature_columns]
    )

    return melt_qa_result(df_same_percent)
