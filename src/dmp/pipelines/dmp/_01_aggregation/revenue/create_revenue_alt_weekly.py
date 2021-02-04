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


def _weekly_aggregation(
    df_rev: pyspark.sql.DataFrame, df_con: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    """
    Creates Weekly Aggregation for Revenue

    Args:
        df_rev: Daily Revenue Data.
        df_con: Loan Content IDs

    Returns:
        Weekly aggregated revenue data.
    """

    # df_rev = df_rev.filter(f.col("source").isin("CHG_GOOD", "CHG_REJECT"))

    df_con = (
        df_con.select("content_id")
        .distinct()
        .withColumn("loan_repayment_flag", f.lit(1))
    )

    df_rev = (
        df_rev.join(df_con, ["content_id"], how="left")
        .withColumn(
            "event_date",
            f.to_date(f.col("event_date").cast(t.StringType()), "yyyy-MM-dd"),
        )
        .withColumn("is_weekend", f.dayofweek("event_date").isin([1, 7]).cast("int"))
        .withColumn("weekstart", next_week_start_day(f.col("event_date")))
        .withColumn(
            "loan_repayment_flag", f.coalesce(f.col("loan_repayment_flag"), f.lit(0))
        )
    )

    df_weekly = df_rev.groupBy("msisdn", "weekstart").agg(
        f.sum(f.coalesce(f.col("rev"), f.lit(0))).alias("rev_alt_total"),
        f.sum(f.when(f.col("loan_repayment_flag") == 0, f.col("rev"))).alias(
            "non_loan_revenue"
        ),
        f.sum(f.when(f.col("loan_repayment_flag") == 1, f.col("rev"))).alias(
            "loan_revenue"
        ),
        f.collect_set(
            f.when(f.col("loan_repayment_flag") == 1, f.col("event_date"))
        ).alias("loan_repayment_date"),
        (
            f.coalesce(
                f.count(
                    f.when(
                        (f.col("loan_repayment_flag") == 1)
                        & (f.col("rev") > 0)
                        & (f.col("credit_debit_code") == "D"),
                        f.col("rev"),
                    )
                ),
                f.lit(0),
            )
            - f.coalesce(
                f.count(
                    f.when(
                        (f.col("loan_repayment_flag") == 1)
                        & (f.col("rev") < 0)
                        & (f.col("credit_debit_code") == "C"),
                        f.col("rev"),
                    )
                ),
                f.lit(0),
            )
        ).alias("loan_repayment_transactions"),
        f.sum(
            f.when(
                (f.col("l1_name") == "Voice P2P"), f.coalesce(f.col("rev"), f.lit(0)),
            )
        ).alias("rev_alt_voice"),
        f.sum(
            f.when(
                (f.col("l1_name") == "Voice P2P") & (f.col("is_weekend") == 1),
                f.coalesce(f.col("rev"), f.lit(0)),
            )
        ).alias("rev_alt_voice_weekend"),
        f.sum(
            f.when(
                (
                    (f.col("l1_name") == "Voice P2P")
                    & (f.col("l2_name") == "Voice Package")
                ),
                f.coalesce(f.col("rev"), f.lit(0)),
            )
        ).alias("rev_alt_voice_pkg"),
        f.sum(
            f.when(
                (f.col("l1_name") == "Broadband"), f.coalesce(f.col("rev"), f.lit(0)),
            )
        ).alias("rev_alt_data"),
        f.sum(
            f.when(
                (f.col("l1_name") == "Broadband") & (f.col("is_weekend") == 1),
                f.coalesce(f.col("rev"), f.lit(0)),
            )
        ).alias("rev_alt_data_weekend"),
        f.sum(
            f.when(
                ((f.col("l1_name") == "Broadband") & (f.col("l2_name") != "APN/PAYU")),
                f.coalesce(f.col("rev"), f.lit(0)),
            )
        ).alias("rev_alt_data_pkg"),
        f.sum(
            f.when((f.col("l1_name") == "SMS P2P"), f.coalesce(f.col("rev"), f.lit(0)))
        ).alias("rev_alt_sms"),
        f.sum(
            f.when(
                (f.col("l1_name") == "SMS P2P") & (f.col("is_weekend") == 1),
                f.coalesce(f.col("rev"), f.lit(0)),
            )
        ).alias("rev_alt_sms_weekend"),
        f.sum(
            f.when(
                ((f.col("l1_name") == "SMS P2P") & (f.col("l2_name") == "SMS Package")),
                f.coalesce(f.col("rev"), f.lit(0)),
            )
        ).alias("rev_alt_sms_pkg"),
        f.sum(
            f.when(
                (f.col("l1_name") == "Digital Services"),
                f.coalesce(f.col("rev"), f.lit(0)),
            )
        ).alias("rev_alt_digital"),
        f.sum(
            f.when(
                (
                    (f.col("l1_name") == "Digital Services")
                    & (f.col("l2_name") == "SMS Non P2P")
                ),
                f.coalesce(f.col("rev"), f.lit(0)),
            )
        ).alias("rev_alt_digital_sms_nonp2p"),
        f.sum(
            f.when(
                (
                    (f.col("l1_name") == "Digital Services")
                    & (f.col("l2_name") == "Voice Non P2P")
                ),
                f.coalesce(f.col("rev"), f.lit(0)),
            )
        ).alias("rev_alt_digital_voice_nonp2p"),
        f.sum(
            f.when(
                (
                    (f.col("l1_name") == "Digital Services")
                    & (f.col("l2_name") == "Ring Back Tone")
                ),
                f.coalesce(f.col("rev"), f.lit(0)),
            )
        ).alias("rev_alt_digital_rbt"),
        f.sum(
            f.when(
                (
                    (f.col("l1_name") == "Digital Services")
                    & (f.col("l2_name") == "MMS P2P")
                ),
                f.coalesce(f.col("rev"), f.lit(0)),
            )
        ).alias("rev_alt_digital_mms_p2p"),
        f.sum(
            f.when(
                (
                    (f.col("l1_name") == "Digital Services")
                    & (f.col("l2_name") == "MMS Content")
                ),
                f.coalesce(f.col("rev"), f.lit(0)),
            )
        ).alias("rev_alt_digital_mms_cnt"),
        f.sum(
            f.when(
                (
                    (f.col("l1_name") == "Digital Services")
                    & (f.col("l2_name") == "Transfer Pulsa")
                ),
                f.coalesce(f.col("rev"), f.lit(0)),
            )
        ).alias("rev_alt_digital_tp"),
        f.sum(
            f.when(
                (
                    (f.col("l1_name") == "Digital Services")
                    & (f.col("l2_name") == "USSD")
                ),
                f.coalesce(f.col("rev"), f.lit(0)),
            )
        ).alias("rev_alt_digital_ussd"),
        f.sum(
            f.when(
                (
                    (f.col("l1_name") == "Digital Services")
                    & (f.col("l2_name") == "Other VAS Service")
                ),
                f.coalesce(f.col("rev"), f.lit(0)),
            )
        ).alias("rev_alt_digital_other_vas"),
        f.sum(
            f.when(
                (
                    (f.col("l1_name") == "Digital Services")
                    & (f.col("l2_name") == "Video Call")
                ),
                f.coalesce(f.col("rev"), f.lit(0)),
            )
        ).alias("rev_alt_digital_videocall"),
        f.sum(
            f.when(
                (
                    (f.col("l1_name") == "Digital Services")
                    & (f.col("l2_name") == "M Music")
                ),
                f.coalesce(f.col("rev"), f.lit(0)),
            )
        ).alias("rev_alt_digital_music"),
        f.sum(
            f.when(
                (f.col("l1_name") == "International Roaming"),
                f.coalesce(f.col("rev"), f.lit(0)),
            )
        ).alias("rev_alt_roam"),
        f.sum(
            f.when(
                (
                    (f.col("l1_name") == "International Roaming")
                    & (
                        f.col("l3_name").isin(
                            [
                                "Data Roaming Package",
                                "International Roaming Package",
                                "SMS Roaming Package",
                                "BB Roaming Package",
                            ]
                        )
                    )
                ),
                f.coalesce(f.col("rev"), f.lit(0)),
            )
        ).alias("rev_alt_roam_pkg"),
        f.countDistinct(
            f.when(
                (f.col("l1_name") == "Voice P2P")
                & (f.col("rev") >= 1000)
                & (f.col("rev") < 5000),
                "event_date",
            )
        ).alias("rev_alt_days_with_voice_above_999_below_5000"),
        f.countDistinct(
            f.when(
                (f.col("l1_name") == "Voice P2P")
                & (f.col("rev") >= 5000)
                & (f.col("rev") < 10000),
                "event_date",
            )
        ).alias("rev_alt_days_with_voice_above_4999_below_10000"),
        f.countDistinct(
            f.when(
                (f.col("l1_name") == "Voice P2P")
                & (f.col("rev") >= 10000)
                & (f.col("rev") < 50000),
                "event_date",
            )
        ).alias("rev_alt_days_with_voice_above_9999_below_50000"),
        f.countDistinct(
            f.when(
                (f.col("l1_name") == "Voice P2P") & (f.col("rev") >= 50000),
                "event_date",
            )
        ).alias("rev_alt_days_with_voice_above_49999"),
        f.countDistinct(
            f.when(
                (f.col("l1_name") == "Broadband")
                & (f.col("rev") >= 1000)
                & (f.col("rev") < 5000),
                "event_date",
            )
        ).alias("rev_alt_days_with_data_above_999_below_5000"),
        f.countDistinct(
            f.when(
                (f.col("l1_name") == "Broadband")
                & (f.col("rev") >= 5000)
                & (f.col("rev") < 10000),
                "event_date",
            )
        ).alias("rev_alt_days_with_data_above_4999_below_10000"),
        f.countDistinct(
            f.when(
                (f.col("l1_name") == "Broadband")
                & (f.col("rev") >= 10000)
                & (f.col("rev") < 50000),
                "event_date",
            )
        ).alias("rev_alt_days_with_data_above_9999_below_50000"),
        f.countDistinct(
            f.when(
                (f.col("l1_name") == "Broadband") & (f.col("rev") >= 50000),
                "event_date",
            )
        ).alias("rev_alt_days_with_data_above_49999"),
        f.countDistinct(
            f.when(
                (f.col("l1_name") == "Digital Services")
                & (f.col("rev") >= 1000)
                & (f.col("rev") < 5000),
                "event_date",
            )
        ).alias("rev_alt_days_with_digital_above_999_below_5000"),
        f.countDistinct(
            f.when(
                (f.col("l1_name") == "Digital Services")
                & (f.col("rev") >= 5000)
                & (f.col("rev") < 10000),
                "event_date",
            )
        ).alias("rev_alt_days_with_digital_above_4999_below_10000"),
        f.countDistinct(
            f.when(
                (f.col("l1_name") == "Digital Services")
                & (f.col("rev") >= 10000)
                & (f.col("rev") < 50000),
                "event_date",
            )
        ).alias("rev_alt_days_with_digital_above_9999_below_50000"),
        f.countDistinct(
            f.when(
                (f.col("l1_name") == "Digital Services") & (f.col("rev") >= 50000),
                "event_date",
            )
        ).alias("rev_alt_days_with_digital_above_49999"),
    )

    return df_weekly


def create_revenue_alt_weekly(
    df_rev: pyspark.sql.DataFrame, df_con: pyspark.sql.DataFrame
) -> None:
    """
    Creates Weekly Aggregation for Revenue

    Args:
        df_rev: Daily Revenue Data.
        df_con: Loan Content IDs

    """

    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date()
    end_date = get_end_date()

    weekly_agg_catalog = conf_catalog["l2_revenue_alt_weekly_data"]

    load_args = conf_catalog["l1_merge_revenue_dd"]["load_args"]
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

        df_data = df_rev.filter(
            f.col(load_args["partition_column"]).between(sdate, edate)
        ).repartition(1000)

        df = _weekly_aggregation(df_rev=df_data, df_con=df_con).drop(f.col("weekstart"))

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
