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


def _weekly_aggregation(tutela_df: pyspark.sql.DataFrame,) -> [pyspark.sql.DataFrame]:
    df = tutela_df.withColumn(
        "date_end", f.to_date(f.col("date_end").cast(t.StringType()), "yyyy-MM-dd")
    ).withColumn("weekstart", next_week_start_day(f.col("date_end")))

    df = df.filter(f.lower(f.col("level")) == "kecamatan")

    df = df.withColumn(
        "operator",
        f.when(f.lower(f.col("operator")) == "telkomsel", "tsel").otherwise(
            "competitor"
        ),
    )
    df = df.withColumn("node", f.lower(f.col("node")))
    df = df.withColumn(
        "node", f.when(f.col("node") == "others", "2g_3g").otherwise(f.col("node")),
    )
    df = df.withColumn(
        "operator_node", f.concat(f.col("operator"), f.lit("_"), f.col("node"))
    )

    df = (
        df.groupBy("location", "weekstart")
        .pivot("operator_node")
        .agg(
            f.max("download_p10").alias("download_p10"),
            f.max("download_p25").alias("download_p25"),
            f.max("avg_download_throughput").alias("avg_download_throughput"),
            f.max("download_p75").alias("download_p75"),
            f.max("download_p90").alias("download_p90"),
            f.max("upload_p10").alias("upload_p10"),
            f.max("upload_p25").alias("upload_p25"),
            f.max("avg_upload_throughput").alias("avg_upload_throughput"),
            f.max("upload_p75").alias("upload_p75"),
            f.max("upload_p90").alias("upload_p90"),
            f.max("latency_p10").alias("latency_p10"),
            f.max("latency_p25").alias("latency_p25"),
            f.max("avg_latency").alias("avg_latency"),
            f.max("latency_p75").alias("latency_p75"),
            f.max("latency_p90").alias("latency_p90"),
            f.max("coverage_cell_bandwidth").alias("coverage_cell_bandwidth"),
            f.max("coverage_km2").alias("coverage_km2"),
            f.max("device_share").alias("device_share"),
            f.max("enodeb_share").alias("enodeb_share"),
            f.avg("excellent_quality").alias("avg_excellent_quality"),
            f.avg("hd_quality").alias("avg_hd_quality"),
            f.avg("good_quality").alias("avg_good_quality"),
            f.avg("game_parameter").alias("avg_game_parameter"),
            f.avg("video_score").alias("avg_video_score"),
            f.sum("signal_good").alias("signal_good"),
            f.sum("signal_fair").alias("signal_fair"),
            f.sum("signal_bad").alias("signal_bad"),
            f.sum("sample").alias("sample"),
        )
    )

    df = df.withColumn(
        "4g_device_share_gap",
        f.col("tsel_4g_device_share") - f.col("competitor_4g_device_share"),
    )
    df = df.withColumn(
        "4g_enodeb_share_gap",
        f.col("tsel_4g_enodeb_share") - f.col("competitor_4g_enodeb_share"),
    )
    df = df.withColumn(
        "4g_coverage_km2_gap",
        f.col("tsel_4g_coverage_km2") - f.col("competitor_4g_coverage_km2"),
    )
    df = df.withColumn(
        "4g_coverage_cell_bandwidth_gap",
        f.col("tsel_4g_coverage_cell_bandwidth")
        - f.col("competitor_4g_coverage_cell_bandwidth"),
    )
    df = df.withColumn(
        "4g_avg_download_throughput_gap",
        f.col("tsel_4g_avg_download_throughput")
        - f.col("competitor_4g_avg_download_throughput"),
    )
    df = df.withColumn(
        "4g_avg_latency_gap",
        f.col("tsel_4g_avg_latency") - f.col("competitor_4g_avg_latency"),
    )
    df = df.withColumn(
        "4g_avg_upload_throughput_gap",
        f.col("tsel_4g_avg_upload_throughput")
        - f.col("competitor_4g_avg_upload_throughput"),
    )

    df = df.withColumn(
        "2g_3g_device_share_gap",
        f.col("tsel_2g_3g_device_share") - f.col("competitor_2g_3g_device_share"),
    )

    df = df.withColumn(
        "2g_3g_coverage_km2_gap",
        f.col("tsel_2g_3g_coverage_km2") - f.col("competitor_2g_3g_coverage_km2"),
    )

    df = df.withColumn(
        "2g_3g_coverage_cell_bandwidth_gap",
        f.col("tsel_2g_3g_coverage_cell_bandwidth")
        - f.col("competitor_2g_3g_coverage_cell_bandwidth"),
    )

    df = df.withColumn(
        "2g_3g_avg_download_throughput_gap",
        f.col("tsel_2g_3g_avg_download_throughput")
        - f.col("competitor_2g_3g_avg_download_throughput"),
    )

    df = df.withColumn(
        "2g_3g_avg_upload_throughput_gap",
        f.col("tsel_2g_3g_avg_upload_throughput")
        - f.col("competitor_2g_3g_avg_upload_throughput"),
    )
    df = df.withColumn(
        "2g_3g_avg_latency_gap",
        f.col("tsel_2g_3g_avg_latency") - f.col("competitor_2g_3g_avg_latency"),
    )
    return df


def create_tutela_aggregation(tutela_df: pyspark.sql.DataFrame,) -> None:
    """
    Creates weekly aggregation for Network Data.

    """

    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date(context=None, partition_column="yearweek_partition")
    end_date = get_end_date(context=None, partition_column="yearweek_partition")

    weekly_agg_catalog = conf_catalog["l2_network_tutela_location_weekly_aggregated"]

    load_args = conf_catalog["l1_network_tutela_weekly"]["load_args"]
    save_args = weekly_agg_catalog["save_args"]
    save_args.pop("partitionBy", None)
    file_path = get_file_path(filepath=weekly_agg_catalog["filepath"])
    file_format = weekly_agg_catalog["file_format"]
    partitions = int(weekly_agg_catalog["partitions"])
    log.info(
        "Starting Weekly Aggregation for WeekStart {start_date} to {end_date}".format(
            start_date=(start_date).strftime("%Y-%m-%d"),
            end_date=(end_date).strftime("%Y-%m-%d"),
        )
    )
    log.info(f"File Path: {file_path}")
    log.info(f"File Format: {file_format}")
    log.info(f"Load Args: {load_args}")
    log.info(f"Save Args: {save_args}")
    log.info(f"Partitions: {partitions}")
    first_day = start_date.replace(month=1, day=1)
    first_monday = first_day + timedelta(days=7 - first_day.weekday())
    prev_end_date = start_date - timedelta(days=7)

    # get first prev week
    sd = start_date - timedelta(days=7)
    fd = sd.replace(month=1, day=1)
    fm = fd + timedelta(days=7 - first_day.weekday())
    prev_week = int((sd - fm).days / 7)

    while start_date <= end_date:
        week_start = start_date.strftime("%Y-%m-%d")
        log.info("Starting Weekly Aggregation for WeekStart: {}".format(week_start))

        year = start_date.year
        if start_date.year > prev_end_date.year:
            first_monday = start_date
            year = prev_end_date.year

        week = int((start_date - first_monday).days / 7)

        if week == 0:
            week = prev_week + 1

        week = f"{week:02}"
        yearweek = str(year) + str(week)

        log.info("Loading tutela yearweek partition: {}".format(yearweek))

        df_data = tutela_df.filter(f.col(load_args["partition_column"]) == yearweek)

        df = _weekly_aggregation(tutela_df=df_data).drop(f.col("weekstart"))

        partition_file_path = "{file_path}/weekstart={weekstart}".format(
            file_path=file_path, weekstart=week_start
        )

        df.repartition(numPartitions=partitions).write.save(
            partition_file_path, file_format, **save_args
        )
        log.info(
            "Completed Weekly Aggregation for WeekStart: {}".format(
                (start_date).strftime("%Y-%m-%d")
            )
        )
        prev_week = week
        prev_end_date = start_date
        start_date += timedelta(days=7)
