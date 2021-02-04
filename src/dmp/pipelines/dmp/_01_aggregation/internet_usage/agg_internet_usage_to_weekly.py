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
from functools import reduce

import pyspark
import pyspark.sql.functions as f

from utils import (
    get_config_parameters,
    get_end_date,
    get_start_date,
    next_week_start_day,
)
from utils.spark_data_set_helper import get_file_path

log = logging.getLogger(__name__)

weekdays = ["Mon", "Tue", "Wed", "Thu", "Fri"]
weekends = ["Sat", "Sun"]


def _daily_aggregation(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Perform aggregations to convert granularity to msisdn and event_date.
    Args:
        df: ABT dataframe for internet usage.
    Returns:
        Dataframe aggregated to msisdn and event_date.
    """

    grouped = df.groupBy("msisdn", "event_date").agg(
        f.collect_set(f.col("city")).alias("sub_district_list"),
        f.sum(f.col("tot_kb")).alias("tot_kb"),
        f.sum(f.col("tot_trx")).alias("tot_trx"),
        f.sum(f.col("vol_data_2g_kb")).alias("vol_data_2g_kb"),
        f.sum(f.col("vol_data_3g_kb")).alias("vol_data_3g_kb"),
        f.sum(f.col("vol_data_4g_kb")).alias("vol_data_4g_kb"),
        f.sum(f.col("kb_day")).alias("kb_day"),
        f.sum(f.col("trx_day")).alias("trx_day"),
        f.sum(f.col("cnt_session_day")).alias("cnt_session_day"),
        f.sum(f.col("kb_night")).alias("kb_night"),
        f.sum(f.col("trx_night")).alias("trx_night"),
        f.sum(f.col("cnt_session_night")).alias("cnt_session_night"),
    )

    output = (
        grouped.withColumn(
            "tot_kb_weekday",
            f.when(
                f.date_format(f.col("event_date"), "E").isin(weekdays), f.col("tot_kb")
            ).otherwise(f.lit(0)),
        )
        .withColumn(
            "tot_kb_weekend",
            f.when(
                f.date_format(f.col("event_date"), "E").isin(weekends), f.col("tot_kb")
            ).otherwise(f.lit(0)),
        )
        .withColumn(
            "usage_flag",
            f.when((f.col("tot_kb") > 0) | (f.col("tot_trx") > 0), 1).otherwise(0),
        )
        .withColumn("weekstart", next_week_start_day("event_date"))
    )

    return output


def _weekly_aggregation(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Perform aggregations to convert granularity to msisdn and event_date
    Args:
        df: Dataframe which is aggregated to msisdn and event_date.
    Returns:
        Dataframe which is aggregated to msisdn and weekstart.
    """

    weekday_weekend_agg_exprs = [
        f.sum(
            f.when(
                f.date_format(f.col("event_date"), "E").isin(weekdays),
                f.col("vol_data_2g_kb"),
            ).otherwise(f.lit(0))
        ).alias("vol_data_2g_kb_weekday"),
        f.sum(
            f.when(
                f.date_format(f.col("event_date"), "E").isin(weekdays),
                f.col("vol_data_3g_kb"),
            ).otherwise(f.lit(0))
        ).alias("vol_data_3g_kb_weekday"),
        f.sum(
            f.when(
                f.date_format(f.col("event_date"), "E").isin(weekdays),
                f.col("vol_data_4g_kb"),
            ).otherwise(f.lit(0))
        ).alias("vol_data_4g_kb_weekday"),
        f.sum(
            f.when(
                f.date_format(f.col("event_date"), "E").isin(weekends),
                f.col("vol_data_2g_kb"),
            ).otherwise(f.lit(0))
        ).alias("vol_data_2g_kb_weekend"),
        f.sum(
            f.when(
                f.date_format(f.col("event_date"), "E").isin(weekends),
                f.col("vol_data_3g_kb"),
            ).otherwise(f.lit(0))
        ).alias("vol_data_3g_kb_weekend"),
        f.sum(
            f.when(
                f.date_format(f.col("event_date"), "E").isin(weekends),
                f.col("vol_data_4g_kb"),
            ).otherwise(f.lit(0))
        ).alias("vol_data_4g_kb_weekend"),
    ]

    payload_agg_exprs = [
        f.sum(f.when(f.col("tot_kb") >= 2048, 1).otherwise(f.lit(0))).alias(
            "days_with_payload_total_above_2mb"
        ),
        f.sum(f.when(f.col("tot_kb") >= 51200, 1).otherwise(f.lit(0))).alias(
            "days_with_payload_total_above_50mb"
        ),
        f.sum(f.when(f.col("tot_kb") >= 512000, 1).otherwise(f.lit(0))).alias(
            "days_with_payload_total_above_500mb"
        ),
        f.sum(f.when(f.col("vol_data_4g_kb") >= 2048, 1).otherwise(f.lit(0))).alias(
            "days_with_payload_4g_above_2mb"
        ),
        f.sum(f.when(f.col("vol_data_4g_kb") >= 51200, 1).otherwise(f.lit(0))).alias(
            "days_with_payload_4g_above_50mb"
        ),
        f.sum(f.when(f.col("vol_data_4g_kb") >= 512000, 1).otherwise(f.lit(0))).alias(
            "days_with_payload_4g_above_500mb"
        ),
    ]

    simple_agg_exprs = [
        f.sum(f.col("tot_kb")).alias("tot_kb"),
        f.sum(f.col("tot_trx")).alias("tot_trx"),
        f.sum(f.col("tot_kb_weekday")).alias("tot_kb_weekday"),
        f.sum(f.col("tot_kb_weekend")).alias("tot_kb_weekend"),
        f.sum(f.col("vol_data_2g_kb")).alias("vol_data_2g_kb"),
        f.sum(f.col("vol_data_3g_kb")).alias("vol_data_3g_kb"),
        f.sum(f.col("vol_data_4g_kb")).alias("vol_data_4g_kb"),
        f.min(f.col("vol_data_4g_kb")).alias("min_vol_data_4g_kb_day"),
        f.expr("percentile_approx(vol_data_4g_kb,0.5)").alias("med_vol_data_4g_kb_day"),
        f.max(f.col("vol_data_4g_kb")).alias("max_vol_data_4g_kb_day"),
        f.min(f.col("tot_kb")).alias("min_vol_data_tot_kb_day"),
        f.expr("percentile_approx(tot_kb,0.5)").alias("med_vol_data_tot_kb_day"),
        f.max(f.col("tot_kb")).alias("max_vol_data_tot_kb_day"),
        f.array_distinct(f.flatten(f.collect_set("sub_district_list"))).alias(
            "sub_district_list_data_usage"
        ),
        f.stddev(f.col("tot_kb")).alias("stddev_total_data_usage_day"),
        f.stddev(f.col("vol_data_4g_kb")).alias("stddev_4g_data_usage_day"),
        f.sum(f.col("usage_flag")).alias("count_non_zero_usage_days"),
        f.sum(f.col("kb_day")).alias("tot_kb_day"),
        f.sum(f.col("trx_day")).alias("tot_trx_day"),
        f.sum(f.col("cnt_session_day")).alias("count_day_sessions"),
        f.sum(f.col("kb_night")).alias("tot_kb_night"),
        f.sum(f.col("trx_night")).alias("tot_trx_night"),
        f.sum(f.col("cnt_session_night")).alias("count_night_sessions"),
    ]

    agg_exprs = [weekday_weekend_agg_exprs, payload_agg_exprs, simple_agg_exprs]
    agg_exprs_flattened = reduce(lambda x, y: x + y, agg_exprs)

    week_grouped = (
        df.groupBy("msisdn", "weekstart")
        .agg(*agg_exprs_flattened)
        .withColumn("count_zero_usage_days", 7 - f.col("count_non_zero_usage_days"))
    )

    return week_grouped


def create_weekly_internet_usage_table(
    df_internet_usage: pyspark.sql.DataFrame,
) -> None:
    """
    Aggregates df_internet_usage table to msisdn, weekstart and prepares features for downstream layers.

    Args:
        df_internet_usage: Internet usage (consumption) table.

    """

    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date()
    end_date = get_end_date()

    weekly_agg_catalog = conf_catalog["l1_internet_usage_weekly"]

    load_args = conf_catalog["l1_usage_upcc_dd"]["load_args"]
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

        df_data = df_internet_usage.filter(
            f.col(load_args["partition_column"]).between(sdate, edate)
        )

        df = _daily_aggregation(df=df_data)
        df = _weekly_aggregation(df=df).drop(f.col("weekstart"))

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
