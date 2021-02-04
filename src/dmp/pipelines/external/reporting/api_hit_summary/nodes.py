import calendar
from datetime import date, datetime
from typing import List

import pyspark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def filter_partner_and_period(
    df: pyspark.sql.DataFrame, partner_id: str
) -> pyspark.sql.DataFrame:
    """
    Filter the table based on the partner ID and the current months

    Args:
        df: API Requests Postgres dump
        partner_id: Partner ID to be filtered
        first_period: First period of the filtered date
        last_period: Last period of the filtered date

    Returns:
    The filtered dataset

    """

    df = df.filter(f.col("event_date") == datetime.now().strftime("%Y-%m-%d"))

    today = datetime.now()
    today_year = today.year
    today_month = today.month
    _, num_days = calendar.monthrange(today_year, today_month)

    first_period = date(today_year, today_month, 1).strftime("%Y-%m-%d")
    last_period = date(today_year, today_month, num_days).strftime("%Y-%m-%d")

    df = df.filter(
        (f.col("requested_at").between(first_period, last_period))
        & (f.col("partner_id") == partner_id)
        & (f.col("http_status") == "200")  # only filter successful requests
    )
    return df


def get_clean_response_format(
    df: pyspark.sql.DataFrame, columns_to_include: List
) -> pyspark.sql.DataFrame:
    """
    Get the clean version of the report to be sent

    Args:
        df: API Requests Postgres dump
        columns_to_include: Final columns to be included

    Returns:
    The clean version of the report
    """
    spark = SparkSession.builder.getOrCreate()
    # json schema
    json_request_meta = spark.read.json(
        df.rdd.map(lambda row: row["request_meta"])
    ).schema
    json_response_meta = spark.read.json(
        df.rdd.map(lambda row: row["response_meta"])
    ).schema

    df = (
        df.withColumn(
            "request_meta_json",
            f.from_json(f.col("request_meta"), schema=json_request_meta),
        )
        .withColumn(
            "response_meta_json",
            f.from_json(f.col("response_meta"), schema=json_response_meta),
        )
        .withColumn("msisdn", f.col("request_meta_json")["msisdn"])
        .withColumn("model_id", f.col("response_meta_json")["modelID"])
        .withColumn("refresh_date", f.col("response_meta_json")["refreshDate"])
        .withColumn(
            "default_probability_score_scv", f.col("response_meta_json")["score"]
        )
    )

    # formatting of the date columns
    df = df.withColumn(
        "refresh_date", f.substring(f.col("refresh_date"), 1, 10)
    ).withColumn("requested_at", f.substring(f.col("requested_at"), 1, 10))

    # select the necessary columns only
    df = (
        df.select(columns_to_include)
        .groupby(["partner_id", "requested_at", "refresh_date"])
        .count()
    )

    df = df.sort(f.col("requested_at"))

    return df
