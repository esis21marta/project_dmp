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

import re

import pyspark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
from pyspark.sql.window import Window

from utils import (
    add_prefix_suffix_to_df_columns,
    get_end_date,
    get_required_output_columns,
    get_start_date,
)


def rename_feature_df(fea_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Returns the dataframe with renamed column

    Args:
        fea_df: dataframe which contain features

    Returns:
        dataframe which contain renamed column features
    """
    columns = fea_df.columns
    for col in ["msisdn", "weekstart"]:
        columns.remove(col)
    df = add_prefix_suffix_to_df_columns(
        fea_df, prefix="fea_int_app_usage_", columns=columns
    )
    return df


def get_rolling_window(num_days: int, key: str = "msisdn") -> pyspark.sql.window:
    """
    Returns a rolling window

    Args:
        num_days: Number of days for the rolling window
        key: partitionBy Key

    Returns:
        w: Rolling window with given number of weeks partitioned by msisdn, ordered by weekstart
    """
    days = lambda i: ((i - 1) * 86400)
    w = (
        Window()
        .partitionBy(f.col(key), f.col("category"))
        .orderBy(f.col("weekstart").cast("timestamp").cast("long"))
        .rangeBetween(-days(num_days), 0)
    )
    return w


def fea_internet_app_usage(
    df_app_usage_weekly: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: list,
    shuffle_partitions: int,
) -> pyspark.sql.DataFrame:
    """
    Calculates the internet app usage for each msisdn:

    Args:
        df_app_usage_weekly: Internet App Weekly Aggregated data (cadence: msisdn, weekstart, feature)
        feature_mode: Feature Mode [All OR List]
        required_output_features: Feature List

    Returns:
        fea_df: DataFrame with features, app internet usage
            - msisdn: Unique Id
            - weekstart: Weekly observation point
            - fea_int_app_usage_*_data_vol_*d: Data used in the window
            - fea_int_app_usage_*_accessed_apps_*d : App count used in the window
            - fea_int_app_usage_*_days_since_first_usage: Days since first usage
            - fea_int_app_usage_*_days_since_lsst_usage: Days since last usage
    """

    # Get All App Category which are required for the feature generations from required_output_features
    req_app_category = set()
    if "feature_list" == feature_mode:
        required_output_features = [
            fea
            for fea in required_output_features
            if fea.startswith("fea_int_app_usage_")
            and (
                ("_data_vol_" in fea)
                or ("_accessed_apps_" in fea)
                or ("_days_since_first_usage" in fea)
                or ("_days_since_last_usage" in fea)
                or ("_latency_4g_weekdays_" in fea)
                or ("_latency_4g_weekends_" in fea)
                or ("_speed_4g_weekdays_" in fea)
                or ("_speed_4g_weekends_" in fea)
                or ("_duration_" in fea)
            )
        ]
        for fea in required_output_features:
            for fea_type in [
                "_data_vol_",
                "_accessed_apps_",
                "_days_since_first_usage",
                "_days_since_last_usage",
                "_latency_4g_weekdays_",
                "_latency_4g_weekends_",
                "_speed_4g_weekdays_",
                "_speed_4g_weekends_",
                "_duration_",
            ]:
                if fea_type in fea:
                    req_app_category.add(
                        re.search(f"fea_int_app_usage_(.*){fea_type}", fea).group(1)
                    )

        # These 4 categories are required in Social Media Features
        req_app_category.add("facebook")
        req_app_category.add("instagram")
        req_app_category.add("linkedin")
        req_app_category.add("youtube")

        df_app_usage_weekly = df_app_usage_weekly.filter(
            f.col("category").isin(req_app_category)
        )

    # Updated Shuffle Partitions
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)

    # Aggregating over Features
    df_app_usage_weekly = (
        df_app_usage_weekly.withColumn(
            "data_vol_01w", f.col("volume_in") + f.col("volume_out")
        )
        .withColumn("category", f.trim(f.lower(f.col("category"))))
        .withColumn(
            "data_vol_weekend_01w",
            f.col("4g_volume_in_weekend") + f.col("4g_volume_out_weekend"),
        )
        .withColumn(
            "data_vol_weekday_01w",
            f.col("4g_volume_in_weekday") + f.col("4g_volume_out_weekday"),
        )
    )

    # Calculating First Usage for Each Feature Category
    df_first_usage = (
        df_app_usage_weekly.groupBy(f.col("msisdn"), f.col("category"))
        .agg(f.min(f.col("min_trx_date")).alias("first_usage"))
        .select("msisdn", "category", "first_usage")
    )

    # Calculating Last Usage for Each Feature Category over weekly trend
    w = (
        Window()
        .partitionBy(f.col("msisdn"), f.col("category"))
        .orderBy(f.col("weekstart"))
    )
    df_last_usage = df_app_usage_weekly.withColumn(
        "last_usage", f.max(f.col("max_trx_date")).over(window=w)
    ).select("msisdn", "weekstart", "category", "last_usage")

    # Joining First Usage & Last Usage
    df_app_usage_weekly = df_app_usage_weekly.join(
        df_first_usage, ["msisdn", "category"], how="left"
    ).join(df_last_usage, ["msisdn", "weekstart", "category"], how="left")

    # Adding accessed_apps_num_weeks
    df_app_usage_weekly = df_app_usage_weekly.withColumn(
        "accessed_apps_01w",
        f.when(
            f.col("accessed_app").isNotNull(), f.size(f.col("accessed_app"))
        ).otherwise(0),
    ).withColumn(
        "accessed_apps_num_weeks_01w",
        f.when(f.col("accessed_apps_01w") > 0, 1).otherwise(0).cast(LongType()),
    )
    # Calculating Features
    fea_df = (
        df_app_usage_weekly.withColumn("data_vol_01w", f.col("data_vol_01w"))
        .withColumn(
            "data_vol_02w", f.sum(f.col("data_vol_01w")).over(get_rolling_window(14)),
        )
        .withColumn(
            "data_vol_03w", f.sum(f.col("data_vol_01w")).over(get_rolling_window(21)),
        )
        .withColumn(
            "data_vol_01m", f.sum(f.col("data_vol_01w")).over(get_rolling_window(28)),
        )
        .withColumn(
            "data_vol_02m", f.sum(f.col("data_vol_01w")).over(get_rolling_window(56)),
        )
        .withColumn(
            "data_vol_03m", f.sum(f.col("data_vol_01w")).over(get_rolling_window(91)),
        )
        .withColumn(
            "data_vol_weekend_02w",
            f.sum(f.col("data_vol_weekend_01w")).over(get_rolling_window(14)),
        )
        .withColumn(
            "data_vol_weekend_03w",
            f.sum(f.col("data_vol_weekend_01w")).over(get_rolling_window(21)),
        )
        .withColumn(
            "data_vol_weekend_01m",
            f.sum(f.col("data_vol_weekend_01w")).over(get_rolling_window(28)),
        )
        .withColumn(
            "data_vol_weekend_02m",
            f.sum(f.col("data_vol_weekend_01w")).over(get_rolling_window(56)),
        )
        .withColumn(
            "data_vol_weekend_03m",
            f.sum(f.col("data_vol_weekend_01w")).over(get_rolling_window(91)),
        )
        .withColumn(
            "data_vol_weekday_02w",
            f.sum(f.col("data_vol_weekday_01w")).over(get_rolling_window(14)),
        )
        .withColumn(
            "data_vol_weekday_03w",
            f.sum(f.col("data_vol_weekday_01w")).over(get_rolling_window(21)),
        )
        .withColumn(
            "data_vol_weekday_01m",
            f.sum(f.col("data_vol_weekday_01w")).over(get_rolling_window(28)),
        )
        .withColumn(
            "data_vol_weekday_02m",
            f.sum(f.col("data_vol_weekday_01w")).over(get_rolling_window(56)),
        )
        .withColumn(
            "data_vol_weekday_03m",
            f.sum(f.col("data_vol_weekday_01w")).over(get_rolling_window(91)),
        )
        .withColumn(
            "data_vol_weekday_weekend_ratio_01w",
            f.col("data_vol_weekend_01w")
            / (f.col("data_vol_weekday_01w") + f.col("data_vol_weekend_01w")),
        )
        .withColumn(
            "data_vol_weekday_weekend_ratio_01m",
            f.col("data_vol_weekend_01m")
            / (f.col("data_vol_weekday_01m") + f.col("data_vol_weekend_01m")),
        )
        .withColumn(
            "data_vol_weekday_weekend_ratio_02m",
            f.col("data_vol_weekend_02m")
            / (f.col("data_vol_weekday_02m") + f.col("data_vol_weekend_02m")),
        )
        .withColumn(
            "data_vol_weekday_weekend_ratio_03m",
            f.col("data_vol_weekend_03m")
            / (f.col("data_vol_weekday_03m") + f.col("data_vol_weekend_03m")),
        )
        .withColumn(
            "duration_01w", f.col("4g_duration_weekday") + f.col("4g_duration_weekend"),
        )
        .withColumn(
            "duration_01m", f.sum(f.col("duration_01w")).over(get_rolling_window(28)),
        )
        .withColumn(
            "duration_02m", f.sum(f.col("duration_01w")).over(get_rolling_window(56)),
        )
        .withColumn(
            "duration_03m", f.sum(f.col("duration_01w")).over(get_rolling_window(91)),
        )
        .withColumn(
            "accessed_apps_02w",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("accessed_app")).over(
                            get_rolling_window(14)
                        )
                    )
                )
            ),
        )
        .withColumn(
            "accessed_apps_03w",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("accessed_app")).over(
                            get_rolling_window(21)
                        )
                    )
                )
            ),
        )
        .withColumn(
            "accessed_apps_01m",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("accessed_app")).over(
                            get_rolling_window(28)
                        )
                    )
                )
            ),
        )
        .withColumn(
            "accessed_apps_02m",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("accessed_app")).over(
                            get_rolling_window(56)
                        )
                    )
                )
            ),
        )
        .withColumn(
            "accessed_apps_03m",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("accessed_app")).over(
                            get_rolling_window(91)
                        )
                    )
                )
            ),
        )
        .withColumn(
            "accessed_apps_num_weeks_01m",
            f.sum(f.col("accessed_apps_num_weeks_01w")).over(get_rolling_window(28)),
        )
        .withColumn(
            "accessed_apps_num_weeks_02m",
            f.sum(f.col("accessed_apps_num_weeks_01w")).over(get_rolling_window(56)),
        )
        .withColumn(
            "accessed_apps_num_weeks_03m",
            f.sum(f.col("accessed_apps_num_weeks_01w")).over(get_rolling_window(91)),
        )
        .withColumn("max_days_to_track_first_and_last_usage", f.lit(91))
        .withColumn(
            "days_since_first_usage",
            f.least(
                f.greatest(
                    f.datediff(f.col("weekstart"), f.col("first_usage")), f.lit(-1)
                ),
                f.col("max_days_to_track_first_and_last_usage"),
            ),
        )
        .withColumn(
            "days_since_last_usage",
            f.least(
                f.greatest(
                    f.datediff(f.col("weekstart"), f.col("last_usage")), f.lit(-1)
                ),
                f.col("max_days_to_track_first_and_last_usage"),
            ),
        )
        .withColumn(
            "latency_4g_weekdays_01w",
            (
                f.col("4g_internal_latency_weekday")
                + f.col("4g_external_latency_weekday")
            )
            / (
                f.col("4g_internal_latency_count_weekday")
                + f.col("4g_external_latency_count_weekday")
            ),
        )
        .withColumn(
            "latency_4g_weekdays_01m",
            f.sum(
                f.col("4g_internal_latency_weekday")
                + f.col("4g_external_latency_weekday")
            ).over(get_rolling_window(7 * 4))
            / f.sum(
                f.col("4g_internal_latency_count_weekday")
                + f.col("4g_external_latency_count_weekday")
            ).over(get_rolling_window(7 * 4)),
        )
        .withColumn(
            "latency_4g_weekdays_02m",
            f.sum(
                f.col("4g_internal_latency_weekday")
                + f.col("4g_external_latency_weekday")
            ).over(get_rolling_window(7 * 8))
            / f.sum(
                f.col("4g_internal_latency_count_weekday")
                + f.col("4g_external_latency_count_weekday")
            ).over(get_rolling_window(7 * 8)),
        )
        .withColumn(
            "latency_4g_weekends_01w",
            (
                f.col("4g_internal_latency_weekend")
                + f.col("4g_external_latency_weekend")
            )
            / (
                f.col("4g_internal_latency_count_weekend")
                + f.col("4g_external_latency_count_weekend")
            ),
        )
        .withColumn(
            "latency_4g_weekends_01m",
            f.sum(
                f.col("4g_internal_latency_weekend")
                + f.col("4g_external_latency_weekend")
            ).over(get_rolling_window(7 * 4))
            / f.sum(
                f.col("4g_internal_latency_count_weekend")
                + f.col("4g_external_latency_count_weekend")
            ).over(get_rolling_window(7 * 4)),
        )
        .withColumn(
            "latency_4g_weekends_02m",
            f.sum(
                f.col("4g_internal_latency_weekend")
                + f.col("4g_external_latency_weekend")
            ).over(get_rolling_window(7 * 8))
            / f.sum(
                f.col("4g_internal_latency_count_weekend")
                + f.col("4g_external_latency_count_weekend")
            ).over(get_rolling_window(7 * 8)),
        )
        # "fea_int_app_usage_speed_4g_weekdays_01w"
        .withColumn(
            "speed_4g_weekdays_01w",
            (f.col("4g_volume_in_weekday") + f.col("4g_volume_out_weekday"))
            / (f.col("4g_duration_weekday")),
        )
        .withColumn(
            "speed_4g_weekdays_01m",
            f.sum(f.col("4g_volume_in_weekday") + f.col("4g_volume_out_weekday")).over(
                get_rolling_window(7 * 4)
            )
            / f.sum(f.col("4g_duration_weekday")).over(get_rolling_window(7 * 4)),
        )
        .withColumn(
            "speed_4g_weekdays_02m",
            f.sum(f.col("4g_volume_in_weekday") + f.col("4g_volume_out_weekday")).over(
                get_rolling_window(7 * 8)
            )
            / f.sum(f.col("4g_duration_weekday")).over(get_rolling_window(7 * 8)),
        )
        .withColumn(
            "speed_4g_weekends_01w",
            (f.col("4g_volume_in_weekend") + f.col("4g_volume_out_weekend"))
            / (f.col("4g_duration_weekend")),
        )
        .withColumn(
            "speed_4g_weekends_01m",
            f.sum(f.col("4g_volume_in_weekend") + f.col("4g_volume_out_weekend")).over(
                get_rolling_window(7 * 4)
            )
            / f.sum(f.col("4g_duration_weekend")).over(get_rolling_window(7 * 4)),
        )
        .withColumn(
            "speed_4g_weekends_02m",
            f.sum(f.col("4g_volume_in_weekend") + f.col("4g_volume_out_weekend")).over(
                get_rolling_window(7 * 8)
            )
            / f.sum(f.col("4g_duration_weekend")).over(get_rolling_window(7 * 8)),
        )
        .select(
            "weekstart",
            "msisdn",
            "category",
            "data_vol_01w",
            "data_vol_02w",
            "data_vol_03w",
            "data_vol_01m",
            "data_vol_02m",
            "data_vol_03m",
            "data_vol_weekday_03m",
            "data_vol_weekday_weekend_ratio_01w",
            "data_vol_weekday_weekend_ratio_01m",
            "data_vol_weekday_weekend_ratio_02m",
            "data_vol_weekday_weekend_ratio_03m",
            "duration_01w",
            "duration_01m",
            "duration_02m",
            "duration_03m",
            "accessed_apps_01w",
            "accessed_apps_02w",
            "accessed_apps_03w",
            "accessed_apps_01m",
            "accessed_apps_02m",
            "accessed_apps_03m",
            "accessed_apps_num_weeks_01w",
            "accessed_apps_num_weeks_01m",
            "accessed_apps_num_weeks_02m",
            "accessed_apps_num_weeks_03m",
            "days_since_first_usage",
            "days_since_last_usage",
            "latency_4g_weekdays_01w",
            "latency_4g_weekdays_01m",
            "latency_4g_weekdays_02m",
            "latency_4g_weekends_01w",
            "latency_4g_weekends_01m",
            "latency_4g_weekends_02m",
            "speed_4g_weekdays_01w",
            "speed_4g_weekdays_01m",
            "speed_4g_weekdays_02m",
            "speed_4g_weekends_01w",
            "speed_4g_weekends_01m",
            "speed_4g_weekends_02m",
        )
    )

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return fea_df.filter(f.col("weekstart").between(first_week_start, last_week_start))


def pivot_features(
    fea_df: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: list,
    shuffle_partitions: int,
) -> pyspark.sql.DataFrame:
    """
    :param fea_df: Features dataframe
    :param  feature_mode: Feature Mode [All OR List]
    :param  required_output_features: Feature List
    :return: Pivoted features dataframe
    """

    # Updated Shuffle Partitions
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)

    # Pivoting Features
    fea_df = (
        fea_df.groupBy(f.col("weekstart"), f.col("msisdn"))
        .pivot("category")
        .agg(
            f.coalesce(f.sum(f.col("data_vol_01w")), f.lit(0)).alias("data_vol_01w"),
            f.coalesce(f.sum(f.col("data_vol_02w")), f.lit(0)).alias("data_vol_02w"),
            f.coalesce(f.sum(f.col("data_vol_03w")), f.lit(0)).alias("data_vol_03w"),
            f.coalesce(f.sum(f.col("data_vol_01m")), f.lit(0)).alias("data_vol_01m"),
            f.coalesce(f.sum(f.col("data_vol_02m")), f.lit(0)).alias("data_vol_02m"),
            f.coalesce(f.sum(f.col("data_vol_03m")), f.lit(0)).alias("data_vol_03m"),
            f.coalesce(f.sum(f.col("data_vol_weekday_03m")), f.lit(0)).alias(
                "data_vol_weekday_03m"
            ),
            f.avg(f.col("data_vol_weekday_weekend_ratio_01w")).alias(
                "data_vol_weekday_weekend_ratio_01w"
            ),
            f.avg(f.col("data_vol_weekday_weekend_ratio_01m")).alias(
                "data_vol_weekday_weekend_ratio_01m"
            ),
            f.avg(f.col("data_vol_weekday_weekend_ratio_02m")).alias(
                "data_vol_weekday_weekend_ratio_02m"
            ),
            f.avg(f.col("data_vol_weekday_weekend_ratio_03m")).alias(
                "data_vol_weekday_weekend_ratio_03m"
            ),
            f.coalesce(f.sum(f.col("duration_01w")), f.lit(0)).alias("duration_01w"),
            f.coalesce(f.sum(f.col("duration_01m")), f.lit(0)).alias("duration_01m"),
            f.coalesce(f.sum(f.col("duration_02m")), f.lit(0)).alias("duration_02m"),
            f.coalesce(f.sum(f.col("duration_03m")), f.lit(0)).alias("duration_03m"),
            f.coalesce(f.avg(f.col("latency_4g_weekdays_01w")), f.lit(0)).alias(
                "latency_4g_weekdays_01w"
            ),
            f.coalesce(f.avg(f.col("latency_4g_weekdays_01m")), f.lit(0)).alias(
                "latency_4g_weekdays_01m"
            ),
            f.coalesce(f.avg(f.col("latency_4g_weekdays_02m")), f.lit(0)).alias(
                "latency_4g_weekdays_02m"
            ),
            f.coalesce(f.avg(f.col("latency_4g_weekends_01w")), f.lit(0)).alias(
                "latency_4g_weekends_01w"
            ),
            f.coalesce(f.avg(f.col("latency_4g_weekends_01m")), f.lit(0)).alias(
                "latency_4g_weekends_01m"
            ),
            f.coalesce(f.avg(f.col("latency_4g_weekends_02m")), f.lit(0)).alias(
                "latency_4g_weekends_02m"
            ),
            f.coalesce(f.avg(f.col("speed_4g_weekdays_01w")), f.lit(0)).alias(
                "speed_4g_weekdays_01w"
            ),
            f.coalesce(f.avg(f.col("speed_4g_weekdays_01m")), f.lit(0)).alias(
                "speed_4g_weekdays_01m"
            ),
            f.coalesce(f.avg(f.col("speed_4g_weekdays_02m")), f.lit(0)).alias(
                "speed_4g_weekdays_02m"
            ),
            f.coalesce(f.avg(f.col("speed_4g_weekends_01w")), f.lit(0)).alias(
                "speed_4g_weekends_01w"
            ),
            f.coalesce(f.avg(f.col("speed_4g_weekends_01m")), f.lit(0)).alias(
                "speed_4g_weekends_01m"
            ),
            f.coalesce(f.avg(f.col("speed_4g_weekends_02m")), f.lit(0)).alias(
                "speed_4g_weekends_02m"
            ),
            f.coalesce(f.sum(f.col("accessed_apps_01w")), f.lit(0)).alias(
                "accessed_apps_01w"
            ),
            f.coalesce(f.sum(f.col("accessed_apps_02w")), f.lit(0)).alias(
                "accessed_apps_02w"
            ),
            f.coalesce(f.sum(f.col("accessed_apps_03w")), f.lit(0)).alias(
                "accessed_apps_03w"
            ),
            f.coalesce(f.sum(f.col("accessed_apps_01m")), f.lit(0)).alias(
                "accessed_apps_01m"
            ),
            f.coalesce(f.sum(f.col("accessed_apps_02m")), f.lit(0)).alias(
                "accessed_apps_02m"
            ),
            f.coalesce(f.sum(f.col("accessed_apps_03m")), f.lit(0)).alias(
                "accessed_apps_03m"
            ),
            f.coalesce(f.sum(f.col("accessed_apps_num_weeks_01w")), f.lit(0)).alias(
                "accessed_apps_num_weeks_01w"
            ),
            f.coalesce(f.sum(f.col("accessed_apps_num_weeks_01m")), f.lit(0)).alias(
                "accessed_apps_num_weeks_01m"
            ),
            f.coalesce(f.sum(f.col("accessed_apps_num_weeks_02m")), f.lit(0)).alias(
                "accessed_apps_num_weeks_02m"
            ),
            f.coalesce(f.sum(f.col("accessed_apps_num_weeks_03m")), f.lit(0)).alias(
                "accessed_apps_num_weeks_03m"
            ),
            f.coalesce(f.max(f.col("days_since_first_usage")), f.lit(-1)).alias(
                "days_since_first_usage"
            ),
            f.coalesce(f.min(f.col("days_since_last_usage")), f.lit(-1)).alias(
                "days_since_last_usage"
            ),
        )
    )

    fea_df = rename_feature_df(fea_df)

    extra_columns_to_keep = [
        "msisdn",
        "weekstart",
        "fea_int_app_usage_facebook_data_vol_01w",  # This feature is used in social media features calculation
        "fea_int_app_usage_facebook_data_vol_01m",  # This feature is used in social media features calculation
        "fea_int_app_usage_facebook_data_vol_02m",  # This feature is used in social media features calculation
        "fea_int_app_usage_instagram_data_vol_01w",  # This feature is used in social media features calculation
        "fea_int_app_usage_instagram_data_vol_01m",  # This feature is used in social media features calculation
        "fea_int_app_usage_instagram_data_vol_02m",  # This feature is used in social media features calculation
        "fea_int_app_usage_linkedin_data_vol_01w",  # This feature is used in social media features calculation
        "fea_int_app_usage_linkedin_data_vol_01m",  # This feature is used in social media features calculation
        "fea_int_app_usage_linkedin_data_vol_02m",  # This feature is used in social media features calculation
        "fea_int_app_usage_youtube_data_vol_01w",  # This feature is used in social media features calculation
        "fea_int_app_usage_youtube_data_vol_01m",  # This feature is used in social media features calculation
        "fea_int_app_usage_youtube_data_vol_02m",  # This feature is used in social media features calculation
    ]

    required_output_columns = get_required_output_columns(
        output_features=fea_df.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=extra_columns_to_keep,
    )

    fea_df = fea_df.select(required_output_columns)

    return fea_df
