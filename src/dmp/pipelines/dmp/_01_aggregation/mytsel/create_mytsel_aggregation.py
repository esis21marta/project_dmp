import logging
from datetime import timedelta

import pyspark
import pyspark.sql.functions as f
from pyspark.sql.types import StringType

from utils import (
    get_config_parameters,
    get_end_date,
    get_start_date,
    next_week_start_day,
)
from utils.spark_data_set_helper import get_file_path

log = logging.getLogger(__name__)


def _create_mytsel_weekly_aggregation(
    df_mytsel_daily_user: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    df_mytsel_weekly = (
        df_mytsel_daily_user.withColumn(
            "trx_date", f.to_date(f.col("trx_date").cast(StringType()), "yyyy-MM-dd")
        )
        .withColumn("weekstart", next_week_start_day(f.col("trx_date")))
        .groupby("msisdn", "weekstart")
        .agg(f.count(f.col("trx_date")).alias("mytsel_login_count_days"))
    )
    return df_mytsel_weekly


def create_mytsel_weekly_aggregation(
    df_mytsel_daily_user: pyspark.sql.DataFrame,
) -> None:
    """
    Creates Weekly Aggregation for MyTSel

    Args:
        df_mytsel_daily: Daily MyTSel Data.

    """

    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date()
    end_date = get_end_date()

    weekly_agg_catalog = conf_catalog["l2_mytsel_weekly"]

    load_args = conf_catalog["l1_mck_mytsel_daily_user"]["load_args"]
    save_args = weekly_agg_catalog["save_args"]
    save_args.pop("partitionBy", None)
    file_path = get_file_path(filepath=weekly_agg_catalog["filepath"])
    file_format = weekly_agg_catalog["file_format"]
    partitions = int(weekly_agg_catalog["partitions"])
    log.info(
        "Starting Weekly Aggregation for WeekStart {start_date} to {end_date}".format(
            start_date=(start_date + timedelta(days=7)).strftime("%Y-%m-%d"),
            end_date=(end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
        )
    )
    log.info(f"File Path: {file_path}")
    log.info(f"File Format: {file_format}")
    log.info(f"Load Args: {load_args}")
    log.info(f"Save Args: {save_args}")
    log.info(f"Partitions: {partitions}")

    while start_date < end_date:
        week_start = (start_date + timedelta(days=7)).strftime("%Y-%m-%d")
        sdate = start_date.strftime(load_args["partition_date_format"])
        edate = (start_date + timedelta(days=6)).strftime(
            load_args["partition_date_format"]
        )

        log.info("Starting Weekly Aggregation for WeekStart: {}".format(week_start))

        df_data = df_mytsel_daily_user.filter(
            f.col(load_args["partition_column"]).between(sdate, edate)
        )

        df = _create_mytsel_weekly_aggregation(df_data).drop(f.col("weekstart"))

        partition_file_path = "{file_path}/weekstart={weekstart}".format(
            file_path=file_path, weekstart=week_start
        )

        df.repartition(numPartitions=partitions).write.save(
            partition_file_path, file_format, **save_args
        )
        log.info(
            "Completed Weekly Aggregation for WeekStart: {}".format(
                (start_date + timedelta(days=7)).strftime("%Y-%m-%d")
            )
        )

        start_date += timedelta(days=7)
