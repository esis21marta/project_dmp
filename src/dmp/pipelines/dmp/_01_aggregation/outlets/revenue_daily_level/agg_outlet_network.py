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

from utils import get_config_parameters, get_end_date, get_start_date
from utils.spark_data_set_helper import get_file_path

log = logging.getLogger(__name__)


def partition_dnkm(df_dnkm: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    return df_dnkm


def _agg_outlet_network_dnkm_to_daily(
    df_dnkm: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Aggregating network table by lac_ci, trx_date
    :param df_dnkm: Network raw table
    :return: Aggregated variables by lac_ci, trx_date
    """
    df_dnkm = df_dnkm.withColumn("lac_ci", f.regexp_replace("lac_ci", "-", "|"))
    cast_columns_to_double = [
        "hsupa_mean_user",
        "hsdpa_accesability",
        "hsdpa_accesability_denum_sum",
        "hsdpa_accesability_num_sum",
        "hsdpa_retainability_denum_sum",
        "hsdpa_retainability_num_sum",
        "hsupa_accesability",
        "hsupa_accesability_denum_sum",
        "hsupa_accesability_num_sum",
        "hsupa_retainability_denum_sum",
        "hsupa_retainability_num_sum",
        "hsdpa_retainability",
        "hsupa_retainability",
        "3g_throughput_kbps",
        "payload_hspa_mbyte",
        "payload_psr99_mbyte",
    ]
    for col in cast_columns_to_double:
        df_dnkm = df_dnkm.withColumn(col, f.col(col).cast("double"))

    df_dnkm_agg = df_dnkm.groupBy("lac_ci", "trx_date").agg(
        f.avg("hsupa_mean_user").alias("hsupa_mean_user"),
        f.avg("hsdpa_accesability").alias("hsdpa_accesability"),
        f.sum("hsdpa_accesability_denum_sum").alias("hsdpa_accesability_denum_sum"),
        f.sum("hsdpa_accesability_num_sum").alias("hsdpa_accesability_num_sum"),
        f.sum("hsdpa_retainability_denum_sum").alias("hsdpa_retainability_denum_sum"),
        f.sum("hsdpa_retainability_num_sum").alias("hsdpa_retainability_num_sum"),
        f.avg("hsupa_accesability").alias("hsupa_accesability"),
        f.sum("hsupa_accesability_denum_sum").alias("hsupa_accesability_denum_sum"),
        f.sum("hsupa_accesability_num_sum").alias("hsupa_accesability_num_sum"),
        f.sum("hsupa_retainability_denum_sum").alias("hsupa_retainability_denum_sum"),
        f.sum("hsupa_retainability_num_sum").alias("hsupa_retainability_num_sum"),
        f.avg("hsdpa_retainability").alias("hsdpa_retainability"),
        f.avg("hsupa_retainability").alias("hsupa_retainability"),
        f.avg("3g_throughput_kbps").alias("3g_throughput_kbps"),
        f.avg("payload_hspa_mbyte").alias("payload_hspa_mbyte"),
        f.avg("payload_psr99_mbyte").alias("payload_psr99_mbyte"),
    )

    return df_dnkm_agg


def agg_outlet_network_dnkm(df_dnkm: pyspark.sql.DataFrame) -> None:
    conf_catalog = get_config_parameters(config="catalog")
    start_date = get_start_date(period="1cm")
    end_date = get_end_date(period="1cm")

    # ------------------------------- Input Catalog Entries ------------------------------------ #
    load_args = conf_catalog["l1_dnkm_paritioned_by_trx_date"]["load_args"]

    # ------------------------------- Output Catalog Entries ------------------------------------ #
    daily_agg_catalog = conf_catalog["l1_dnkm_agg_by_lacci"]

    save_args = daily_agg_catalog["save_args"]
    save_args.pop("partitionBy", None)

    file_path = get_file_path(filepath=daily_agg_catalog["filepath"])
    file_format = daily_agg_catalog["file_format"]
    partitions = int(daily_agg_catalog["partitions"])

    log.info(
        "Starting Daily Aggregation for Dates {start_date} to {end_date}".format(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
        )
    )

    log.info(f"File Path: {file_path}")
    log.info(f"File Format: {file_format}")
    log.info(f"Load Args: {load_args}")
    log.info(f"Save Args: {save_args}")
    log.info(f"Partitions: {partitions}")

    while start_date <= end_date:
        event_date = start_date.strftime("%Y-%m-%d")
        current_date = start_date.strftime(load_args["partition_date_format"])

        log.info("Starting Daily Aggregation for Day: {}".format(event_date))

        DATE_COLUMN = "trx_date"
        df_dnkm_filtered = df_dnkm.filter(f.col(DATE_COLUMN) == current_date)

        df = _agg_outlet_network_dnkm_to_daily(df_dnkm_filtered).drop("trx_date")

        partition_file_path = "{file_path}/trx_date={event_date}".format(
            file_path=file_path, event_date=event_date
        )
        df.repartition(numPartitions=partitions).write.save(
            partition_file_path, file_format, **save_args
        )

        log.info("Completed Daily Aggregation for Day: {}".format(event_date))
        start_date += timedelta(days=1)
