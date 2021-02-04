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
from typing import List

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
    df_rech_mytsel: pyspark.sql.DataFrame, col_group_by: List[str] = ["msisdn"]
) -> pyspark.sql.DataFrame:
    """
    Creates Weekly Aggregation for MyTsel Recharge

    Args:
        df_rech_mytsel: Daily Recharge from Mytsel channel.
        col_group_by: list of column need to grouped by

    Returns:
        Weekly aggregated mytsel recharge table.
    """

    df_rech_mytsel = df_rech_mytsel.withColumn(
        "trx_date", f.to_date(f.col("trx_date").cast(t.StringType()), "yyyy-MM-dd")
    ).withColumn("weekstart", next_week_start_day(f.col("trx_date")))

    col_group_by.append("weekstart")

    df_rech_mytsel_weekly = df_rech_mytsel.groupBy(col_group_by).agg(
        f.sum("trx").cast(t.LongType()).alias("tot_trx_sum"),
        f.sum("rech").cast(t.LongType()).alias("tot_amt_sum"),
    )

    return df_rech_mytsel_weekly


def create_mytsel_recharge_weekly(df_rech_mytsel: pyspark.sql.DataFrame) -> None:
    """
    Creates Weekly Aggregation for MyTsel Recharge

    Args:
        df_rech_mytsel: Daily Recharge Mytsel.

    """
    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date()
    end_date = get_end_date()

    weekly_agg_catalog = conf_catalog["l1_mytsel_rech_weekly"]

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

        df_data = df_rech_mytsel.filter(f.col("trx_date").between(sdate, edate))

        df = _weekly_aggregation(
            df_rech_mytsel=df_data, col_group_by=["msisdn", "store_location"]
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
