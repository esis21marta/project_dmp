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
from pyspark.sql.types import StringType

from utils import (
    get_config_parameters,
    get_end_date,
    get_start_date,
    next_week_start_day,
)
from utils.spark_data_set_helper import get_file_path

log = logging.getLogger(__name__)


def _weekly_aggregation(
    df_handset_dd: pyspark.sql.DataFrame, df_handset_lookup: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Aggregates device_dd table to msisdn, weekstart.

    Args:
        df_handset_dd: Handset daily level data.
        df_handset_lookup: Lookup static table for each IMEIs.

    Returns:
        Dataframe with weekly aggregation of df_handset_dd.
    """
    df_handset_dd = (
        df_handset_dd.join(
            df_handset_lookup,
            df_handset_dd["imei"].substr(1, 8) == df_handset_lookup["tac"],
            how="inner",
        )
        .withColumn(
            "trx_date", f.to_date(f.col("trx_date").cast(StringType()), "yyyy-MM-dd")
        )
        .withColumn("weekstart", next_week_start_day(f.col("trx_date")))
    )

    df_handset_weekly = df_handset_dd.groupBy("msisdn", "weekstart").agg(
        f.collect_set("manufacturer").alias("manufacturer"),
        f.collect_set("imei").alias("imeis"),
        f.collect_set("device_type").alias("device_types"),
        f.collect_set("market_name").alias("market_names"),
    )

    first_weekstart = get_start_date(partition_column="weekstart")
    last_weekstart = get_end_date(partition_column="weekstart")

    return df_handset_weekly.filter(
        f.col("weekstart").between(first_weekstart, last_weekstart)
    )


def create_handset_weekly_data(
    df_handset_dd: pyspark.sql.DataFrame, df_handset_lookup: pyspark.sql.DataFrame,
) -> None:
    """
    Aggregates device_dd table to msisdn, weekstart.

    Args:
        df_handset_dd: Handset daily level data.
        df_handset_lookup: Lookup static table for each IMEIs.
    """

    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date()
    end_date = get_end_date()

    # Data before  Jan 2019  is not correct
    df_handset_dd = df_handset_dd.filter(f.col("event_date") >= "2019-01-01")

    weekly_agg_catalog = conf_catalog["l2_handset_weekly_aggregated"]

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
    log.info(f"Save Args: {save_args}")
    log.info(f"Partitions: {partitions}")

    while start_date < end_date:
        week_start = (start_date + timedelta(days=7)).strftime("%Y-%m-%d")
        sdate = start_date.strftime("%Y-%m-%d")
        edate = (start_date + timedelta(days=6)).strftime("%Y-%m-%d")

        log.info("Starting Weekly Aggregation for WeekStart: {}".format(week_start))

        # Only sunday data is required
        df_data = df_handset_dd.filter(f.col("trx_date").between(sdate, edate))
        df = _weekly_aggregation(
            df_handset_dd=df_data, df_handset_lookup=df_handset_lookup
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
