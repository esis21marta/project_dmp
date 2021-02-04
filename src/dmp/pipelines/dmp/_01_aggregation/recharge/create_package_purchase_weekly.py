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
    df_package_purchase_daily: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Weekly Aggregation of package purchase by msisdn

    Args:
        df_package_purchase_daily: Dataframe for package purchase (daily data)

    Returns:
        SMS, Data & Roaming Recharge Weekly Aggregated Data
    """

    df_pkg_purchase = (
        df_package_purchase_daily.withColumn(
            "event_date",
            f.to_date(f.col("event_date").cast(t.StringType()), "yyyy-MM-dd"),
        )
        .withColumn("weekstart", next_week_start_day(f.col("event_date")))
        .select(
            "msisdn",
            "weekstart",
            "trx_pkg_prchse",
            "rev_pkg_prchse",
            "trx_voice_pkg_prchs",
            "trx_sms_pkg_prchs",
            "trx_data_pkg_prchs",
            "trx_roam_pkg_prchs",
            "rev_voice_pkg_prchs",
            "rev_sms_pkg_prchs",
            "rev_data_pkg_prchs",
            "rev_roam_pkg_prchs",
        )
    )

    df_pkg_purchase_weekly = df_pkg_purchase.groupBy("msisdn", "weekstart").agg(
        f.sum("trx_pkg_prchse").cast(t.LongType()).alias("trx_pkg_prchse"),
        f.sum("rev_pkg_prchse").cast(t.LongType()).alias("rev_pkg_prchse"),
        f.sum("trx_voice_pkg_prchs").cast(t.LongType()).alias("trx_voice_pkg_prchs"),
        f.sum("trx_sms_pkg_prchs").cast(t.LongType()).alias("trx_sms_pkg_prchs"),
        f.sum("trx_data_pkg_prchs").cast(t.LongType()).alias("trx_data_pkg_prchs"),
        f.sum("trx_roam_pkg_prchs").cast(t.LongType()).alias("trx_roam_pkg_prchs"),
        f.sum("rev_voice_pkg_prchs").cast(t.LongType()).alias("rev_voice_pkg_prchs"),
        f.sum("rev_sms_pkg_prchs").cast(t.LongType()).alias("rev_sms_pkg_prchs"),
        f.sum("rev_data_pkg_prchs").cast(t.LongType()).alias("rev_data_pkg_prchs"),
        f.sum("rev_roam_pkg_prchs").cast(t.LongType()).alias("rev_roam_pkg_prchs"),
    )

    return df_pkg_purchase_weekly


def create_package_purchase_weekly(
    df_package_purchase_daily: pyspark.sql.DataFrame,
) -> None:
    """
    Weekly Aggregation of package purchase by msisdn

    Args:
        df_package_purchase_daily: Dataframe for package purchase (daily data)

    """

    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date()
    end_date = get_end_date()

    weekly_agg_catalog = conf_catalog["l1_chg_pkg_prchse_weekly"]

    load_args = conf_catalog["l1_itdaml_dd_cdr_pkg_prchse_all"]["load_args"]
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

        df_data = df_package_purchase_daily.filter(
            f.col(load_args["partition_column"]).between(sdate, edate)
        )

        df = _weekly_aggregation(df_package_purchase_daily=df_data).drop(
            f.col("weekstart")
        )

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
