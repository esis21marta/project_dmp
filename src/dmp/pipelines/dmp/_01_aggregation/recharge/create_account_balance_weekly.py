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

import logging
from datetime import timedelta

import pyspark
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.window import Window

from utils import (
    get_config_parameters,
    get_custom_window,
    get_end_date,
    get_fixed_window_rows_between,
    get_start_date,
    next_week_start_day,
)
from utils.spark_data_set_helper import get_file_path

log = logging.getLogger(__name__)


def _weekly_aggregation(
    df_ocs_bal: pyspark.sql.DataFrame,
    df_cb_pre_dd: pyspark.sql.DataFrame,
    df_abt_rech_daily: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Returns Dataframe with the aggregation for each MSISDNs on weekly basis.

    Args:
        df_ocs_bal: Balance dataframe containing balance at daily level.
        df_cb_pre_dd: Balance Dataframe containing balance at daily level.
        df_abt_rech_daily: Recharge dataframe containing recharge at daily level.

    Returns:
        Dataframe with the aggregation for each MSISDNs on weekly basis.
    """

    df_ocs_bal = df_ocs_bal.filter(f.col("event_date") >= "2019-01-07")
    df_cb_pre_dd = df_cb_pre_dd.filter(f.col("event_date") >= "2019-01-07")
    df_cb_pre_dd = df_cb_pre_dd.withColumnRenamed("paychannel", "pay_channel")
    df_abt_rech_daily = (
        df_abt_rech_daily.select("msisdn", "event_date")
        .distinct()
        .withColumn("recharge_flag", f.lit(1))
    )

    df_cb_pre_rech = df_cb_pre_dd.join(
        df_abt_rech_daily, ["msisdn", "event_date"], how="left"
    )
    df_joined = df_ocs_bal.join(
        df_cb_pre_rech, ["pay_channel", "event_date"], how="inner"
    )

    df_daily = (
        df_joined.withColumn("weekstart", next_week_start_day("event_date"))
        .withColumn(
            "is_zero_or_neg", f.when((f.col("account_balance") <= 0), 1).otherwise(0)
        )
        .withColumn(
            "rank_daily",
            f.row_number().over(
                get_custom_window(
                    partition_by_cols=["msisdn", "event_date"],
                    order_by_col="timestamp_1",
                    asc=False,
                )
            ),
        )
        .withColumn(
            "account_balance_min",
            f.min("account_balance").over(Window.partitionBy("msisdn", "weekstart")),
        )
        .withColumn(
            "account_balance_avg",
            f.avg("account_balance").over(Window.partitionBy("msisdn", "weekstart")),
        )
        .withColumn(
            "account_balance_stddev",
            f.stddev("account_balance").over(Window.partitionBy("msisdn", "weekstart")),
        )
        .withColumn(
            "bal_below_500",
            f.sum(f.when((f.col("account_balance") < 500), 1).otherwise(0)).over(
                Window.partitionBy("msisdn", "weekstart")
            ),
        )
        .withColumn(
            "current_balance",
            f.first(f.col("account_balance"), True).over(
                get_fixed_window_rows_between(
                    start=0,
                    end=Window.unboundedFollowing,
                    key="msisdn",
                    optional_keys=["weekstart"],
                    oby="timestamp_1",
                    asc=False,
                )
            ),
        )
        .withColumn(
            "zero_or_neg_count",
            f.sum("is_zero_or_neg")
            .over(Window.partitionBy("msisdn", "weekstart"))
            .cast(t.LongType()),
        )
    )
    df_daily_latest = (
        df_daily.filter(f.col("rank_daily") == 1)
        .withColumn(
            "bal_more_than_5000",
            f.when((f.col("account_balance") > 5000), 1).otherwise(0),
        )
        .withColumn(
            "bal_more_than_10000",
            f.when((f.col("account_balance") > 10000), 1).otherwise(0),
        )
        .withColumn(
            "bal_more_than_20000",
            f.when((f.col("account_balance") > 20000), 1).otherwise(0),
        )
        .withColumn(
            "bal_more_than_50000",
            f.when((f.col("account_balance") > 50000), 1).otherwise(0),
        )
        .withColumn(
            "bal_below_500_latest",
            f.when((f.col("account_balance") < 500), 1).otherwise(0),
        )
        .withColumn(
            "bal_more_than_20000_below_40000",
            f.when((f.col("account_balance").between(20000, 40000)), 1).otherwise(0),
        )
        .withColumn(
            "bal_more_than_40000_below_60000",
            f.when((f.col("account_balance").between(40000, 60000)), 1).otherwise(0),
        )
        .withColumn(
            "bal_more_than_20000_below_60000",
            f.when((f.col("account_balance").between(20000, 60000)), 1).otherwise(0),
        )
        .withColumn(
            "prev_day_balance",
            f.lag(f.col("account_balance")).over(
                get_custom_window(
                    partition_by_cols=["msisdn"], order_by_col="event_date"
                )
            ),
        )
    )

    df_agg = df_daily_latest.groupBy("msisdn", "weekstart").agg(
        f.first("zero_or_neg_count").alias("zero_or_neg_count"),
        f.first("account_balance_min").alias("account_balance_min"),
        f.first("account_balance_avg").alias("account_balance_avg"),
        f.first("account_balance_stddev").alias("account_balance_stddev"),
        f.sum("bal_below_500").alias("account_balance_below_500_count"),
        f.sum("bal_more_than_5000").alias("num_days_bal_more_than_5000"),
        f.sum("bal_more_than_10000").alias("num_days_bal_more_than_10000"),
        f.sum("bal_more_than_20000").alias("num_days_bal_more_than_20000"),
        f.sum("bal_more_than_50000").alias("num_days_bal_more_than_50000"),
        f.sum("bal_below_500_latest").alias("num_days_bal_below_500"),
        f.sum("bal_more_than_20000_below_40000").alias(
            "num_days_bal_more_than_20000_below_40000"
        ),
        f.sum("bal_more_than_40000_below_60000").alias(
            "num_days_bal_more_than_40000_below_60000"
        ),
        f.sum("bal_more_than_20000_below_60000").alias(
            "num_days_bal_more_than_20000_below_60000"
        ),
        f.first(f.col("current_balance")).alias("current_balance"),
        f.max(f.when(f.col("bal_more_than_5000") == 1, f.col("event_date"))).alias(
            "max_date_with_bal_more_than_5000"
        ),
        f.max(f.when(f.col("bal_more_than_10000") == 1, f.col("event_date"))).alias(
            "max_date_with_bal_more_than_10000"
        ),
        f.max(f.when(f.col("bal_more_than_20000") == 1, f.col("event_date"))).alias(
            "max_date_with_bal_more_than_20000"
        ),
        f.max(f.when(f.col("bal_more_than_50000") == 1, f.col("event_date"))).alias(
            "max_date_with_bal_more_than_50000"
        ),
        f.max(f.when(f.col("bal_more_than_5000") != 1, f.col("event_date"))).alias(
            "max_date_with_bal_5000_or_less"
        ),
        f.max(f.when(f.col("bal_more_than_10000") != 1, f.col("event_date"))).alias(
            "max_date_with_bal_10000_or_less"
        ),
        f.max(f.when(f.col("bal_more_than_20000") != 1, f.col("event_date"))).alias(
            "max_date_with_bal_20000_or_less"
        ),
        f.max(f.when(f.col("bal_more_than_50000") != 1, f.col("event_date"))).alias(
            "max_date_with_bal_50000_or_less"
        ),
        f.avg(f.when(f.col("recharge_flag") > 0, f.col("prev_day_balance"))).alias(
            "avg_last_bal_before_rech"
        ),
        f.collect_list(f.when(f.col("recharge_flag") > 0, f.col("event_date"))).alias(
            "recharge_date"
        ),
    )

    return df_agg


def create_account_balance_weekly(
    df_ocs_bal: pyspark.sql.DataFrame,
    df_cb_pre_dd: pyspark.sql.DataFrame,
    df_abt_rech_daily: pyspark.sql.DataFrame,
) -> None:
    """
    Returns Dataframe with the aggregation for each MSISDNs on weekly basis.

    Args:
        df_ocs_bal: Balance dataframe containing balance at daily level.
        df_cb_pre_dd: Balance Dataframe containing balance at daily level.
        df_abt_rech_daily: Recharge dataframe containing recharge at daily level.

    """

    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date()
    end_date = get_end_date()

    weekly_agg_catalog = conf_catalog["l2_account_balance_weekly"]

    load_args_1 = conf_catalog["l1_account_balance"]["load_args"]
    load_args_2 = conf_catalog["l1_prepaid_customers_data"]["load_args"]
    load_args_3 = conf_catalog["l1_abt_rech_daily_abt_dd"]["load_args"]

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
    log.info(f"Load Args 1: {load_args_1}")
    log.info(f"Load Args 2: {load_args_2}")
    log.info(f"Load Args 3: {load_args_3}")
    log.info(f"File Path: {file_path}")
    log.info(f"File Format: {file_format}")
    log.info(f"Save Args: {save_args}")
    log.info(f"Partitions: {partitions}")

    while start_date < end_date:
        week_start = (start_date + timedelta(days=7)).strftime("%Y-%m-%d")
        # we use day-1 to get sunday data from prev week
        sdate_1 = (start_date - timedelta(days=1)).strftime(
            load_args_1["partition_date_format"]
        )
        edate_1 = (start_date + timedelta(days=6)).strftime(
            load_args_1["partition_date_format"]
        )
        # we use day-1 to get sunday data from prev week
        sdate_2 = (start_date - timedelta(days=1)).strftime(
            load_args_2["partition_date_format"]
        )
        edate_2 = (start_date + timedelta(days=6)).strftime(
            load_args_2["partition_date_format"]
        )

        sdate_3 = start_date.strftime(load_args_3["partition_date_format"])
        edate_3 = (start_date + timedelta(days=6)).strftime(
            load_args_3["partition_date_format"]
        )

        log.info("Starting Weekly Aggregation for WeekStart: {}".format(week_start))

        df_data_1 = df_ocs_bal.filter(
            f.col(load_args_1["partition_column"]).between(sdate_1, edate_1)
        )

        df_data_2 = df_cb_pre_dd.filter(
            f.col(load_args_2["partition_column"]).between(sdate_2, edate_2)
        )

        df_data_3 = df_abt_rech_daily.filter(
            f.col(load_args_3["partition_column"]).between(sdate_3, edate_3)
        )

        df = _weekly_aggregation(
            df_ocs_bal=df_data_1, df_cb_pre_dd=df_data_2, df_abt_rech_daily=df_data_3
        )
        df = df.filter(f.col("weekstart") == week_start).drop(f.col("weekstart"))

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
