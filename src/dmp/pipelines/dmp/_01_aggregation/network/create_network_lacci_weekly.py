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
) -> [pyspark.sql.DataFrame]:
    """
    Creates weekly aggregation for Network Lac & Lac-ci Data.

    """
    df = (
        mck_sales_dnkm_dd_df.withColumn(
            "event_date",
            f.to_date(f.col("event_date").cast(t.StringType()), "yyyy-MM-dd"),
        )
        .withColumn("weekstart", next_week_start_day(f.col("event_date")))
        .withColumn("lac_ci", f.regexp_replace("lac_ci", '"', ""))
        .withColumn("lac", f.split(f.col("lac_ci"), "-").getItem(0))
    )

    df.cache()

    df_lacci = network_aggregation(df, "lac_ci")
    df_lac = network_aggregation(df, "lac")

    return df_lacci, df_lac


def create_network_lacci_weekly(mck_sales_dnkm_dd_df: pyspark.sql.DataFrame,) -> None:
    """
    Creates weekly aggregation for Network Lac & Lac-ci Data.

    """

    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date()
    end_date = get_end_date()

    weekly_agg_catalog_1 = conf_catalog["l1_network_lacci_weekly_aggregated"]
    weekly_agg_catalog_2 = conf_catalog["l1_network_lac_weekly_aggregated"]

    load_args = conf_catalog["l1_mck_sales_dnkm_dd"]["load_args"]

    save_args_1 = weekly_agg_catalog_1["save_args"]
    save_args_1.pop("partitionBy", None)
    file_path_1 = get_file_path(filepath=weekly_agg_catalog_1["filepath"])
    file_format_1 = weekly_agg_catalog_1["file_format"]
    partitions_1 = int(weekly_agg_catalog_1["partitions"])

    save_args_2 = weekly_agg_catalog_2["save_args"]
    save_args_2.pop("partitionBy", None)
    file_path_2 = get_file_path(filepath=weekly_agg_catalog_2["filepath"])
    file_format_2 = weekly_agg_catalog_2["file_format"]
    partitions_2 = int(weekly_agg_catalog_2["partitions"])

    log.info(
        "Starting Weekly Aggregation for WeekStart {start_date} to {end_date}".format(
            start_date=(start_date + timedelta(days=7)).strftime("%Y-%m-%d"),
            end_date=(end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
        )
    )
    log.info(f"Load Args 1: {load_args}")
    log.info(f"File Path 1: {file_path_1}")
    log.info(f"File Format 1: {file_format_1}")
    log.info(f"Save Args 1: {save_args_1}")
    log.info(f"Partitions 1: {partitions_1}")
    log.info(f"File Path 2: {file_path_2}")
    log.info(f"File Format 2: {file_format_2}")
    log.info(f"Save Args 2: {save_args_2}")
    log.info(f"Partitions 2: {partitions_2}")

    while start_date < end_date:
        week_start = (start_date + timedelta(days=7)).strftime("%Y-%m-%d")
        sdate = start_date.strftime(load_args["partition_date_format"])
        edate = (start_date + timedelta(days=6)).strftime(
            load_args["partition_date_format"]
        )

        log.info("Starting Weekly Aggregation for WeekStart: {}".format(week_start))

        df_data = mck_sales_dnkm_dd_df.filter(
            f.col(load_args["partition_column"]).between(sdate, edate)
        )

        df_1, df_2 = _weekly_aggregation(mck_sales_dnkm_dd_df=df_data)

        df_1 = df_1.drop(f.col("weekstart"))
        df_2 = df_2.drop(f.col("weekstart"))

        partition_file_path_1 = "{file_path}/weekstart={weekstart}".format(
            file_path=file_path_1, weekstart=week_start
        )

        df_1.repartition(numPartitions=partitions_1).write.save(
            partition_file_path_1, file_format_1, **save_args_1
        )

        partition_file_path_2 = "{file_path}/weekstart={weekstart}".format(
            file_path=file_path_2, weekstart=week_start
        )

        df_2.repartition(numPartitions=partitions_2).write.save(
            partition_file_path_2, file_format_2, **save_args_2
        )

        log.info(
            "Completed Weekly Aggregation for WeekStart: {}".format(
                (start_date + timedelta(days=7)).strftime("%Y-%m-%d")
            )
        )

        start_date += timedelta(days=7)
