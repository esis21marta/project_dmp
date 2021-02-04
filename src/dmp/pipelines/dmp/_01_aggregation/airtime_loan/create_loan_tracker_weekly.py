import logging
from datetime import timedelta

import pyspark
import pyspark.sql.functions as f
import pyspark.sql.types as t

from utils import (
    get_config_parameters,
    get_end_date,
    get_start_date,
    next_week_start_day,
)
from utils.spark_data_set_helper import get_file_path

log = logging.getLogger(__name__)


def _weekly_aggregation(df_cms: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Creates Weekly Aggregation for Loan Amount

    Args:
        df_cms: Daily CMS Data.

    Returns:
        Weekly aggregated loan amount data.
    """

    # df_cms = df_cms.filter(f.col("streamtype") == "FULFILLED")
    # df_cms = df_cms.filter(df_cms.campaignid.contains("-LOAN-"))

    df_cms = df_cms.withColumn(
        "type",
        f.when(f.col("campaignid").contains("-LOAN-"), "LOAN")
        .when(f.col("campaignid").contains("-DEDUCT-"), "DEDUCT")
        .otherwise("OTHERS"),
    )

    df_cms = (
        df_cms.withColumn(
            "loan_amount",
            f.when(
                (f.col("type") == "LOAN") & (f.col("streamtype") == "FULFILLED"),
                f.substring_index(
                    f.substring_index(f.col("campaignid"), "-", 3), "-", -1
                ),
            ),
        )
        .withColumn(
            "loan_amount",
            f.regexp_replace(f.upper(f.col("loan_amount")), "K", "000").cast(
                t.LongType()
            ),
        )
        .withColumn(
            "eligible_loan_amount",
            f.when(
                (f.col("type") == "LOAN") & (f.col("streamtype") == "ELIGIBLE"),
                f.substring_index(
                    f.substring_index(f.col("campaignid"), "-", 3), "-", -1
                ),
            ),
        )
        .withColumn(
            "eligible_loan_amount",
            f.regexp_replace(f.upper(f.col("eligible_loan_amount")), "K", "000").cast(
                t.LongType()
            ),
        )
        .withColumn(
            "event_date",
            f.to_date(f.col("event_date").cast(t.StringType()), "yyyy-MM-dd"),
        )
        .withColumn("weekstart", next_week_start_day(f.col("event_date")))
    )

    df_cms = df_cms.groupBy("msisdn", "weekstart").agg(
        f.sum(f.col("loan_amount")).alias("loan_amount"),
        f.collect_list(
            f.when(
                (f.col("type") == "LOAN") & (f.col("streamtype") == "FULFILLED"),
                f.col("event_date"),
            )
        ).alias("loan_date"),
        f.collect_list(
            f.when(
                (f.col("type") == "DEDUCT") & (f.col("streamtype") == "FULFILLED"),
                f.col("event_date"),
            )
        ).alias("loan_repayment_date"),
        f.collect_list(
            f.when(
                (f.col("type") == "LOAN") & (f.col("streamtype") == "FULFILLED"),
                f.col("campaignid"),
            )
        ).alias("loan_offers_accepted"),
        f.collect_list(
            f.when(
                (f.col("type") == "LOAN") & (f.col("streamtype") == "FULFILLED"),
                f.struct(f.col("event_date"), f.col("campaignid")),
            )
        ).alias("loan_offers_accepted_with_dates"),
        f.collect_list(
            f.when(
                (f.col("type") == "LOAN") & (f.col("streamtype") == "REMINDED"),
                f.col("campaignid"),
            )
        ).alias("loan_offers_sent"),
        f.collect_list(
            f.when(
                (f.col("type") == "LOAN") & (f.col("streamtype") == "REMINDED"),
                f.struct(f.col("event_date"), f.col("campaignid")),
            )
        ).alias("loan_offers_sent_with_dates"),
        f.count(
            f.when(
                (f.col("type") == "LOAN") & (f.col("streamtype") == "REMINDED"),
                f.col("campaignid"),
            )
        ).alias("loan_offers_notifications"),
        f.countDistinct(
            f.when(
                (f.col("type") == "DEDUCT") & (f.col("streamtype") == "FULFILLED"),
                f.col("campaignid"),
            )
        ).alias("loan_offers_repaid"),
        f.countDistinct(
            f.when(
                (f.col("type") == "LOAN") & (f.col("streamtype") == "ELIGIBLE"),
                f.col("campaignid"),
            )
        ).alias("loan_offers_eligible"),
        f.avg(f.col("eligible_loan_amount")).alias("avg_eligible_loan_amount"),
    )

    return df_cms


def create_loan_tracker_weekly(df_cms: pyspark.sql.DataFrame) -> None:
    """
    Creates Weekly Aggregation for Loan Amount

    Args:
        df_cms: Daily CMS Data.

    """

    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date()
    end_date = get_end_date()

    weekly_agg_catalog = conf_catalog["l2_airtime_loan_weekly"]

    load_args = conf_catalog["l1_cms_data"]["load_args"]
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
    log.info(f"Load Args: {load_args}")
    log.info(f"File Path: {file_path}")
    log.info(f"File Format: {file_format}")
    log.info(f"Save Args: {save_args}")
    log.info(f"Partitions: {partitions}")

    while start_date < end_date:
        week_start = (start_date + timedelta(days=7)).strftime("%Y-%m-%d")
        sdate = start_date.strftime(load_args["partition_date_format"])
        edate = (start_date + timedelta(days=6)).strftime(
            load_args["partition_date_format"]
        )

        log.info("Starting Weekly Aggregation for WeekStart: {}".format(week_start))

        df_data = df_cms.filter(
            f.col(load_args["partition_column"]).between(sdate, edate)
        )

        df = _weekly_aggregation(df_cms=df_data).drop(f.col("weekstart"))

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
