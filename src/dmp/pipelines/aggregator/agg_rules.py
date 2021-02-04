from typing import List, Tuple

import pyspark
from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType


def apply_rule_msisdn(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    The function adds a validation column by checking msisdn in the following format the file records
    from database and converts it into spark data frames using kedro data catalog api.

    df (DataFrame): A spark DataFrame with msisdn column

    Returns:
        DataFrame
    """
    msisdn = f.col("msisdn")
    rule_msisdn = f.when(
        msisdn.startswith("+62"), f.regexp_replace(msisdn, "[+]+62", "62")
    ).otherwise(
        f.when(msisdn.startswith("+"), f.regexp_replace(msisdn, "[+]", "")).otherwise(
            f.when(
                msisdn.startswith("0062"), f.regexp_replace(msisdn, "^0062", "62")
            ).otherwise(
                f.when(
                    (msisdn.startswith("8"))
                    & (f.length(msisdn) >= 9)
                    & (f.length(msisdn) <= 11),
                    f.concat(f.lit("62"), msisdn),
                ).otherwise(
                    f.when(
                        (msisdn.startswith("0"))
                        & (~msisdn.startswith("00"))
                        & (f.length(msisdn) >= 10)
                        & (f.length(msisdn) <= 12),
                        f.regexp_replace(msisdn, "^0", "62"),
                    ).otherwise(msisdn)
                )
            )
        )
    )
    return df.withColumn("msisdn", rule_msisdn)


def apply_rule_criteria(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    The function converts criteria columns to correct format

    df (DataFrame): A spark DataFrame with criteria columns

    Returns:
        DataFrame
    """
    df = (
        df.withColumn(
            "criteria_1",
            f.when(
                (f.trim(f.col("criteria_1")) == "") | f.col("criteria_1").isNull(), 0
            )
            .otherwise(f.col("criteria_1"))
            .cast(DoubleType()),
        )
        .withColumn(
            "criteria_2",
            f.when(
                (f.trim(f.col("criteria_2")) == "") | f.col("criteria_2").isNull(), 0
            )
            .otherwise(f.col("criteria_2"))
            .cast(DoubleType()),
        )
        .withColumn(
            "criteria_3",
            f.when(
                (f.trim(f.col("criteria_3")) == "") | f.col("criteria_3").isNull(), 0
            )
            .otherwise(f.col("criteria_3"))
            .cast(DoubleType()),
        )
        .withColumn(
            "criteria_4",
            f.when(
                (f.trim(f.col("criteria_4")) == "") | f.col("criteria_4").isNull(), 0
            )
            .otherwise(f.col("criteria_4"))
            .cast(DoubleType()),
        )
        .withColumn(
            "criteria_5",
            f.when(
                (f.trim(f.col("criteria_5")) == "") | f.col("criteria_5").isNull(), 0
            )
            .otherwise(f.col("criteria_5"))
            .cast(DoubleType()),
        )
    )
    return df


def apply_rule_valid_criteria(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    The function checks if the criteria columns are valid

    df (DataFrame): A spark DataFrame with criteria columns

    Returns:
        DataFrame
    """
    df = df.withColumn(
        "valid_criteria",
        f.when(
            (f.col("criteria_1") < 0)
            | (f.col("criteria_2") < 0)
            | (f.col("criteria_3") < 0)
            | (f.col("criteria_4") < 0)
            | (f.col("criteria_5") < 0),
            "No",
        ).otherwise("Yes"),
    )
    return df


def apply_rule_valid_msisdn(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    The function checks if the msisdn column is valid

    df (DataFrame): A spark DataFrame with msisdn column

    Returns:
        DataFrame
    """
    df = df.withColumn(
        "valid_msisdn",
        f.when(
            (f.col("msisdn").startswith("62"))
            & (f.length(f.col("msisdn")) >= 11)
            & (f.length(f.col("msisdn")) <= 13),
            "Yes",
        ).otherwise("No"),
    )
    return df


def apply_rule_valid_push_channel(
    df: pyspark.sql.DataFrame, push_channels: List[str]
) -> pyspark.sql.DataFrame:
    """
    The function adds a validation column if the push_channel is valid

    df (DataFrame): A spark DataFrame with push_channel column
    push_channels (List[str]): A list of valid push channels from parameters.yml

    Returns:
        DataFrame
    """
    df = df.withColumn(
        "valid_push_channel",
        f.when(f.col("push_channel").isin(push_channels), "Yes").otherwise("No"),
    )
    return df


def apply_schema_validation_checks(
    dfs: List[pyspark.sql.DataFrame], record_ids: List[int], columns: List[str]
) -> Tuple[List[pyspark.sql.DataFrame], List[int]]:
    """
    The function checks the schema of loaded datasets with rules below :
    - count of dataset should be > 0
    - all columns specified must be present
    - Data type of msisdn must not be DoubleType()

    Args:
        dfs (List[DataFrame]): List of datasets
        record_ids (List[int]): List of file records
        columns (List[str]): List of columns that must be present in input datasets from parameters.yml

    Returns:
        Tuple[List[DataFrame], List[int]]
    """
    valid_datasets = []
    valid_record_ids = []

    if len(dfs) > 0:
        for index, record_id in zip(range(0, len(dfs)), record_ids):
            loaded_df = dfs[index]
            non_empty = loaded_df.count() > 0

            has_all_columns = len(set(columns) - set(loaded_df.columns)) == 0

            is_msisdn_type_not_double = (
                loaded_df.select("msisdn").dtypes[0][1] != "double"
            )

            if non_empty & has_all_columns & is_msisdn_type_not_double:
                valid_datasets.append(loaded_df.select(*columns))
                valid_record_ids.append(record_id)

    return valid_datasets, valid_record_ids
