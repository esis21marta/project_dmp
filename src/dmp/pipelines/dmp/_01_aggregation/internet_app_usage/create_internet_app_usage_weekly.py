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
    df_bcp_usage: pyspark.sql.DataFrame, df_app_feature_mapping: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Creates weekly aggregation for smy BCP usage table.

    Args:
        df_bcp_usage: BCP Daily Usage DataFrame.
        df_app_feature_mapping: Application to Feature Mapping Mapping.

    Returns:
        DataFrame with Internet Usage for App usage weekly aggregated.
    """
    df_app_usage = df_bcp_usage.join(
        df_app_feature_mapping,
        how="inner",
        on=[
            (df_bcp_usage["accessed_app"] == df_app_feature_mapping["accessed_app"])
            & (
                (df_bcp_usage["component"] == df_app_feature_mapping["component"])
                | (df_app_feature_mapping["component"].isNull())
            )
        ],
    ).select(
        "msisdn",
        "trx_date",
        df_bcp_usage["accessed_app"],
        "category",
        "rat",
        "volume_in",
        "volume_out",
        "internal_latency",
        "external_latency",
        "duration",
    )

    df_app_usage = (
        df_app_usage.withColumn(
            "trx_date", f.to_date(f.col("trx_date").cast(t.StringType()), "yyyyMMdd"),
        )
        .withColumn("weekstart", next_week_start_day(f.col("trx_date")))
        .withColumn(
            "weekend",
            f.when(f.date_format(f.col("trx_date"), "u") > 5, True).otherwise(False),
        )
    )

    # Aggregating weekly
    df_app_usage_weekly = df_app_usage.groupBy("msisdn", "weekstart", "category").agg(
        f.coalesce(f.sum("volume_in"), f.lit(0)).alias("volume_in"),
        f.coalesce(f.sum("volume_out"), f.lit(0)).alias("volume_out"),
        f.collect_set(f.col("accessed_app")).alias("accessed_app"),
        f.coalesce(
            f.sum(
                f.when(
                    (~f.col("weekend")) & (f.col("rat") == "E-UTRAN"),
                    f.col("volume_in").cast(t.LongType()),
                )
            ),
            f.lit(0),
        ).alias("4g_volume_in_weekday"),
        f.coalesce(
            f.sum(
                f.when(
                    f.col("weekend") & (f.col("rat") == "E-UTRAN"),
                    f.col("volume_in").cast(t.LongType()),
                )
            ),
            f.lit(0),
        ).alias("4g_volume_in_weekend"),
        f.coalesce(
            f.sum(
                f.when(
                    (~f.col("weekend")) & (f.col("rat") == "E-UTRAN"),
                    f.col("volume_out").cast(t.LongType()),
                )
            ),
            f.lit(0),
        ).alias("4g_volume_out_weekday"),
        f.coalesce(
            f.sum(
                f.when(
                    f.col("weekend") & (f.col("rat") == "E-UTRAN"),
                    f.col("volume_out").cast(t.LongType()),
                )
            ),
            f.lit(0),
        ).alias("4g_volume_out_weekend"),
        f.coalesce(
            f.sum(
                f.when(
                    (~f.col("weekend")) & (f.col("rat") == "E-UTRAN"),
                    f.col("duration").cast(t.LongType()),
                )
            ),
            f.lit(0),
        ).alias("4g_duration_weekday"),
        f.coalesce(
            f.sum(
                f.when(
                    f.col("weekend") & (f.col("rat") == "E-UTRAN"),
                    f.col("duration").cast(t.LongType()),
                )
            ),
            f.lit(0),
        ).alias("4g_duration_weekend"),
        f.coalesce(
            f.sum(
                f.when(
                    (~f.col("weekend")) & (f.col("rat") == "E-UTRAN"),
                    f.col("internal_latency"),
                )
            ),
            f.lit(0),
        ).alias("4g_internal_latency_weekday"),
        f.coalesce(
            f.count(
                f.when(
                    (~f.col("weekend")) & (f.col("rat") == "E-UTRAN"),
                    f.col("internal_latency"),
                )
            ),
            f.lit(0),
        ).alias("4g_internal_latency_count_weekday"),
        f.coalesce(
            f.sum(
                f.when(
                    f.col("weekend") & (f.col("rat") == "E-UTRAN"),
                    f.col("internal_latency"),
                )
            ),
            f.lit(0),
        ).alias("4g_internal_latency_weekend"),
        f.coalesce(
            f.count(
                f.when(
                    f.col("weekend") & (f.col("rat") == "E-UTRAN"),
                    f.col("internal_latency"),
                )
            ),
            f.lit(0),
        ).alias("4g_internal_latency_count_weekend"),
        f.coalesce(
            f.sum(
                f.when(
                    (~f.col("weekend")) & (f.col("rat") == "E-UTRAN"),
                    f.col("external_latency"),
                )
            ),
            f.lit(0),
        ).alias("4g_external_latency_weekday"),
        f.coalesce(
            f.count(
                f.when(
                    (~f.col("weekend")) & (f.col("rat") == "E-UTRAN"),
                    f.col("external_latency"),
                )
            ),
            f.lit(0),
        ).alias("4g_external_latency_count_weekday"),
        f.coalesce(
            f.sum(
                f.when(
                    f.col("weekend") & (f.col("rat") == "E-UTRAN"),
                    f.col("external_latency"),
                )
            ),
            f.lit(0),
        ).alias("4g_external_latency_weekend"),
        f.coalesce(
            f.count(
                f.when(
                    f.col("weekend") & (f.col("rat") == "E-UTRAN"),
                    f.col("external_latency"),
                )
            ),
            f.lit(0),
        ).alias("4g_external_latency_count_weekend"),
        f.min("trx_date").alias("min_trx_date"),
        f.max("trx_date").alias("max_trx_date"),
    )

    return df_app_usage_weekly


def create_internet_app_usage_weekly(
    df_bcp_usage: pyspark.sql.DataFrame, df_app_feature_mapping: pyspark.sql.DataFrame,
) -> None:
    """
    Creates weekly aggregation for smy BCP usage table.

    Args:
        df_bcp_usage: BCP Daily Usage DataFrame.
        df_app_feature_mapping: Application to Feature Mapping Mapping.

    """

    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date()
    end_date = get_end_date()

    weekly_agg_catalog = conf_catalog["l2_internet_app_usage_weekly"]

    load_args = conf_catalog["l1_bcp_usage_daily"]["load_args"]
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

    # Caching df_app_feature_mapping, So that Spark won't end up reading this DataFrame from HDFS in each iteration
    df_app_feature_mapping.cache()

    while start_date < end_date:
        week_start = (start_date + timedelta(days=7)).strftime("%Y-%m-%d")
        sdate = start_date.strftime(load_args["partition_date_format"])
        edate = (start_date + timedelta(days=6)).strftime(
            load_args["partition_date_format"]
        )

        log.info("Starting Weekly Aggregation for WeekStart: {}".format(week_start))

        df_data = df_bcp_usage.filter(
            f.col(load_args["partition_column"]).between(sdate, edate)
        )

        df = _weekly_aggregation(
            df_bcp_usage=df_data, df_app_feature_mapping=df_app_feature_mapping,
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
    df_app_feature_mapping.unpersist()
