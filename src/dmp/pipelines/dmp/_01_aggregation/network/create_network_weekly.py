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

from .network_aggregation import network_aggregation

log = logging.getLogger(__name__)


def _weekly_aggregation(
    mck_sales_dnkm_dd_df: pyspark.sql.DataFrame,
    cb_pre_dd: pyspark.sql.DataFrame,
    cb_post_dd: pyspark.sql.DataFrame,
) -> [pyspark.sql.DataFrame]:
    df = (
        mck_sales_dnkm_dd_df.withColumn(
            "event_date",
            f.to_date(f.col("event_date").cast(t.StringType()), "yyyy-MM-dd"),
        )
        .withColumn("weekstart", next_week_start_day(f.col("event_date")))
        .withColumn("lac_ci", f.regexp_replace("lac_ci", '"', ""))
        .withColumn("lac", f.split(f.col("lac_ci"), "-").getItem(0))
    )

    cb_pre_dd = (
        cb_pre_dd.filter(
            (f.col("msisdn").isNotNull())
            & (f.col("lac").isNotNull())
            & (f.col("ci").isNotNull())
        )
        .withColumn("lac_ci", f.concat(f.col("lac"), f.lit("-"), f.col("ci")))
        .select("msisdn", "event_date", "lac_ci")
    )

    cb_post_dd = (
        cb_post_dd.filter(
            (f.col("msisdn").isNotNull())
            & (f.col("lac").isNotNull())
            & (f.col("ci").isNotNull())
        )
        .withColumn("lac_ci", f.concat(f.col("lac"), f.lit("-"), f.col("ci")))
        .select("msisdn", "event_date", "lac_ci")
    )

    cb_prepost = cb_pre_dd.union(cb_post_dd).distinct()

    df = cb_prepost.join(df, ["lac_ci", "event_date"], how="inner")

    df = network_aggregation(df, "msisdn")

    return df


def create_network_weekly(
    mck_sales_dnkm_dd_df: pyspark.sql.DataFrame,
    cb_pre_dd: pyspark.sql.DataFrame,
    cb_post_dd: pyspark.sql.DataFrame,
) -> None:
    """
    Creates weekly aggregation for Network Data.

    """

    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date()
    end_date = get_end_date()

    weekly_agg_catalog = conf_catalog["l1_network_msisdn_weekly_aggregated"]

    load_args_1 = conf_catalog["l1_mck_sales_dnkm_dd"]["load_args"]
    load_args_2 = conf_catalog["l1_prepaid_customers_data"]["load_args"]
    load_args_3 = conf_catalog["l1_postpaid_customers_data"]["load_args"]

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
    log.info(f"File Path 1: {file_path}")
    log.info(f"File Format 1: {file_format}")
    log.info(f"Save Args 1: {save_args}")
    log.info(f"Partitions 1: {partitions}")

    while start_date < end_date:
        week_start = (start_date + timedelta(days=7)).strftime("%Y-%m-%d")
        sdate_1 = start_date.strftime(load_args_1["partition_date_format"])
        edate_1 = (start_date + timedelta(days=6)).strftime(
            load_args_1["partition_date_format"]
        )

        sdate_2 = start_date.strftime(load_args_2["partition_date_format"])
        edate_2 = (start_date + timedelta(days=6)).strftime(
            load_args_1["partition_date_format"]
        )

        sdate_3 = start_date.strftime(load_args_3["partition_date_format"])
        edate_3 = (start_date + timedelta(days=6)).strftime(
            load_args_1["partition_date_format"]
        )

        log.info("Starting Weekly Aggregation for WeekStart: {}".format(week_start))

        df_data_1 = mck_sales_dnkm_dd_df.filter(
            f.col(load_args_1["partition_column"]).between(sdate_1, edate_1)
        )

        df_data_2 = cb_pre_dd.filter(
            f.col(load_args_1["partition_column"]).between(sdate_2, edate_2)
        )

        df_data_3 = cb_post_dd.filter(
            f.col(load_args_1["partition_column"]).between(sdate_3, edate_3)
        )

        df = _weekly_aggregation(
            mck_sales_dnkm_dd_df=df_data_1, cb_pre_dd=df_data_2, cb_post_dd=df_data_3
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
