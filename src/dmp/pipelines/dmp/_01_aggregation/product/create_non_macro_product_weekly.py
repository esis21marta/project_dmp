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

from utils import (
    get_config_parameters,
    get_end_date,
    get_start_date,
    next_week_start_day,
)
from utils.spark_data_set_helper import get_file_path

log = logging.getLogger(__name__)


def _weekly_aggregation(
    df_usage_ocs_chg_dd: pyspark.sql.DataFrame,
    df_sku_bucket_pivot_dd: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Creates weekly product table by:
        Left join usage_ocs_chg_dd and sku_bucket_pivot

    Args:
        df_usage_ocs_chg_dd: usage_ocs_chg_dd daily level table
        df_sku_bucket_pivot_dd: sku_bucket_pivot transaction level table

    Returns:
        df_agg: Weekly aggregated product table
    """
    df_usage_ocs_chg_dd = (
        df_usage_ocs_chg_dd.filter(
            (
                f.lower(f.col("cp_name")).like("%reflex%")
                & (f.lower(f.col("service_filter")) == "vas")
            )
            | f.lower(f.col("cp_name")).like("digicore")
        )
        .withColumn("bid", f.col("content_id"))
        .withColumn("sku", f.col("pack_id"))
        .select("msisdn", "event_date", "bid", "sku", "rev")
    )

    # cast to long type except for 'bid', 'sku', '*subscription*', and '*data*'
    castLong = [
        f.col(col).cast(t.LongType()).alias(col)
        for col in df_sku_bucket_pivot_dd.columns
        if (
            (col != "bid")
            & (col != "sku")
            & ("subscription" not in col)
            & ("data" not in col)
        )
    ]

    # for every 'subscription' features, they are binary variables but with 0-> yes, null-> no
    replaceZero = [
        f.when(f.col(col) == 0, 1).cast(t.LongType()).alias(col)
        for col in df_sku_bucket_pivot_dd.columns
        if ("subscription" in col)
    ]

    # there are some ‘data’ features who have different unit (not in GB like the others)
    # they can be identified by filtering out any value higher than 1024
    convertGb = [
        f.when(f.col(col) >= 1024, f.col(col) / 1024)
        .otherwise(f.col(col))
        .cast(t.LongType())
        .alias(col)
        for col in df_sku_bucket_pivot_dd.columns
        if ("data" in col)
    ]

    df_sku_bucket_pivot_dd = df_sku_bucket_pivot_dd.select(
        ["bid", "sku"] + castLong + replaceZero + convertGb
    ).na.fill(0)

    df_join = df_usage_ocs_chg_dd.join(
        df_sku_bucket_pivot_dd, ["bid", "sku"], how="left"
    ).withColumn("weekstart", next_week_start_day(f.col("event_date")))

    df_weekly_agg = df_join.groupBy("msisdn", "weekstart").agg(
        f.sum("rev").cast(t.LongType()).alias("revenue"),
        f.sum("tcash_balance").cast(t.LongType()).alias("tcash_balance"),  # 15
        f.sum("voice_offnet").cast(t.LongType()).alias("voice_offnet"),  # 21
        f.sum("voice_allnet").cast(t.LongType()).alias("voice_allnet"),  # 22
        f.sum("voice_allopr").cast(t.LongType()).alias("voice_allopr"),  # 23
        f.sum("voice_onnet_malam").cast(t.LongType()).alias("voice_onnet_malam"),  # 24
        f.sum("voice_onnet_siang").cast(t.LongType()).alias("voice_onnet_siang"),  # 25
        f.sum("voice_roaming_mo").cast(t.LongType()).alias("voice_roaming_mo"),  # 26
        f.sum("voice_onnet").cast(t.LongType()).alias("voice_onnet"),  # 27
        f.sum("voice_idd").cast(t.LongType()).alias("voice_idd"),  # 28
        f.sum("voice_roaming").cast(t.LongType()).alias("voice_roaming"),  # 29
        f.sum("data_games").cast(t.LongType()).alias("data_games"),  # 30
        f.sum("data_video").cast(t.LongType()).alias("data_video"),  # 31
        f.sum("data_dpi").cast(t.LongType()).alias("data_dpi"),  # 32
        f.sum("data_4g_omg").cast(t.LongType()).alias("data_4g_omg"),  # 33
        f.sum("data_wifi").cast(t.LongType()).alias("data_wifi"),  # 34
        f.sum("data_roaming").cast(t.LongType()).alias("data_roaming"),  # 35
        f.sum("data_allnet").cast(t.LongType()).alias("data_allnet"),  # 36
        f.sum("data_4g_mds").cast(t.LongType()).alias("data_4g_mds"),  # 37
        f.sum("data_allnet_local").cast(t.LongType()).alias("data_allnet_local"),  # 38
        f.sum("unlimited_data").cast(t.LongType()).alias("unlimited_data"),  # 39
        f.sum("data_allnet_siang").cast(t.LongType()).alias("data_allnet_siang"),  # 40
        f.sum("data_music").cast(t.LongType()).alias("data_music"),  # 41
        f.sum("4g_data_pool").cast(t.LongType()).alias("fg_data_pool"),  # 42
        f.sum("data_onnet").cast(t.LongType()).alias("data_onnet"),  # 43
        f.sum("data_4g").cast(t.LongType()).alias("data_4g"),  # 44
        f.sum("data_mds").cast(t.LongType()).alias("data_mds"),  # 45
        f.sum("sms_allnet").cast(t.LongType()).alias("sms_allnet"),  # 46
        f.sum("sms_allopr").cast(t.LongType()).alias("sms_allopr"),  # 47
        f.sum("sms_offnet").cast(t.LongType()).alias("sms_offnet"),  # 48
        f.sum("sms_roaming").cast(t.LongType()).alias("sms_roaming"),  # 49
        f.sum("sms_onnet").cast(t.LongType()).alias("sms_onnet"),  # 50
        f.sum("sms_onnet_siang").cast(t.LongType()).alias("sms_onnet_siang"),  # 51
        f.sum("subscription_tribe")
        .cast(t.LongType())
        .alias("subscription_tribe"),  # 52
        f.sum("subscription_hooq").cast(t.LongType()).alias("subscription_hooq"),  # 53
        f.sum("subscription_viu").cast(t.LongType()).alias("subscription_viu"),  # 54
        f.sum("subscription_bein").cast(t.LongType()).alias("subscription_bein"),  # 55
        f.sum("subscription_music")
        .cast(t.LongType())
        .alias("subscription_music"),  # 56
        f.sum("monbal_monbal").cast(t.LongType()).alias("monbal_monbal"),  # 57
        f.sum("monbal_siang").cast(t.LongType()).alias("monbal_siang"),  # 58
        f.sum("monbal_onnet").cast(t.LongType()).alias("monbal_onnet"),  # 59
    )

    return df_weekly_agg


def create_non_macro_product_weekly(
    df_usage_ocs_chg_dd: pyspark.sql.DataFrame,
    df_sku_bucket_pivot_dd: pyspark.sql.DataFrame,
) -> None:
    """
    Creates weekly product table by:
        Left join usage_ocs_chg_dd and sku_bucket_pivot

    Args:
        df_usage_ocs_chg_dd: usage_ocs_chg_dd daily level table
        df_sku_bucket_pivot_dd: sku_bucket_pivot transaction level table

    Returns:
        df_agg: Weekly aggregated product table
    """

    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date()
    end_date = get_end_date()

    weekly_agg_catalog = conf_catalog["l1_nonmacro_product_weekly_data"]

    load_args = conf_catalog["l1_smy_usage_ocs_chg_dd"]["load_args"]
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

    # Caching df_sku_bucket_pivot_dd, So that Spark won't end up reading this DataFrame from HDFS in each iteration
    df_sku_bucket_pivot_dd.cache()

    while start_date < end_date:
        week_start = (start_date + timedelta(days=7)).strftime("%Y-%m-%d")
        sdate = start_date.strftime(load_args["partition_date_format"])
        edate = (start_date + timedelta(days=6)).strftime(
            load_args["partition_date_format"]
        )

        log.info("Starting Weekly Aggregation for WeekStart: {}".format(week_start))

        df_data = df_usage_ocs_chg_dd.filter(
            f.col(load_args["partition_column"]).between(sdate, edate)
        )

        df = _weekly_aggregation(
            df_usage_ocs_chg_dd=df_data, df_sku_bucket_pivot_dd=df_sku_bucket_pivot_dd
        ).drop(f.col("weekstart"))

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
    df_sku_bucket_pivot_dd.unpersist()
