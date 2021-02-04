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
from pyspark.sql.window import Window

from utils import (
    get_config_parameters,
    get_end_date,
    get_start_date,
    next_week_start_day,
)
from utils.spark_data_set_helper import get_file_path

log = logging.getLogger(__name__)


def pick_one_record_from_dbi_imei_handset(
    dbi_imei_handset: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Filters any one handset per IMEI

    Args:
        dbi_imei_handset: Lookup static table for each IMEIS

    Returns:
        Dataframe with Filters any one handset per IMEI
    """
    df_distinct = dbi_imei_handset.select(
        "tac",
        "manufacture",
        "design_type",
        "device_type",
        "os",
        "network",
        "volte",
        "multisim",
    ).distinct()

    df_handset_dim = df_distinct.withColumn(
        "rn", f.row_number().over(Window.partitionBy("tac").orderBy("device_type"))
    )
    df_handset_dim = df_handset_dim.filter(f.col("rn") == 1)

    return df_handset_dim


def _weekly_aggregation(
    df_handset_dd: pyspark.sql.DataFrame, df_handset_dim: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Aggregates device_dd table to msisdn, weekstart

    Args:
        df_handset_dd: Handset daily level data
        df_handset_dim: Lookup static table for each IMEI

    Returns:
        df_handset_weekly: Dataframe with Weekly aggregation of internal handset device
    """

    start_date = get_start_date().strftime("%Y-%m-%d")
    end_date = get_end_date().strftime("%Y-%m-%d")

    df_handset_dd = df_handset_dd.filter(f.col("event_date") >= "2019-01-01").filter(
        f.col("trx_date").between(start_date, end_date)
    )

    df_handset_dd = df_handset_dd.join(
        df_handset_dim,
        df_handset_dd["imei"].substr(1, 8) == df_handset_dim["tac"],
        how="inner",
    )

    df_handset_dd = (
        df_handset_dd.withColumn(
            "trx_date", f.to_date(f.col("trx_date").cast(StringType()), "yyyy-MM-dd")
        )
        .withColumn("weekday", f.date_format(f.col("trx_date"), "EEE"))
        .filter(f.col("weekday") == "Sun")
    )

    df_handset_weekly = (
        df_handset_dd.withColumn("weekstart", next_week_start_day(f.col("trx_date")))
        .groupBy("msisdn", "weekstart")
        .agg(
            f.first(f.col("design_type")).alias("design_type"),
            f.first(f.col("device_type")).alias("device_type"),
            f.first(f.col("os")).alias("os"),
            f.first(f.col("network")).alias("network"),
            f.first(f.col("volte")).alias("volte"),
            f.first(f.col("multisim")).alias("multisim"),
            f.first(f.col("manufacture")).alias("manufacture"),
            f.first(f.col("tac")).alias("tac"),
            f.first(f.col("card_type")).alias("card_type"),
        )
    )

    first_weekstart = get_start_date(partition_column="weekstart")
    last_weekstart = get_end_date(partition_column="weekstart")

    return df_handset_weekly.filter(
        f.col("weekstart").between(first_weekstart, last_weekstart)
    )


def create_handset_weekly(
    df_handset_dd: pyspark.sql.DataFrame, df_handset_dim: pyspark.sql.DataFrame,
) -> None:
    """
    Aggregates device_dd table to msisdn, weekstart

    Args:
        df_handset_dd: Handset daily level data
        df_handset_dim: Lookup static table for each IMEI
    """

    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date()
    end_date = get_end_date()

    # Data before  Jan 2019  is not correct
    df_handset_dd = df_handset_dd.filter(f.col("event_date") >= "2019-01-01")

    weekly_agg_catalog = conf_catalog["l2_device_internal_weekly_aggregated"]

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
            df_handset_dd=df_data, df_handset_dim=df_handset_dim
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
