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

import pyspark
import pyspark.sql.functions as f

from utils import (
    get_rolling_window,
    get_window_pby_msisdn_oby_trx_date,
    get_window_to_pick_last_row,
    next_week_start_day,
)


def get_usage_prep_df(cb_multidim_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    add column from cb multidim for get tsel point

    Args:
        cb_multidim_df: dataframe from cb multidim

    Returns:
        dataframe which add column previous and difference tsel point
    """
    return (
        cb_multidim_df.withColumn("trx_date", f.to_date("trx_date", "yyyy-MM-dd"))
        .withColumn(
            "prev_tsel_poin",
            f.lag(f.col("tsel_poin"), 1).over(get_window_pby_msisdn_oby_trx_date()),
        )
        .withColumn("points_difference", f.col("tsel_poin") - f.col("prev_tsel_poin"))
    )


def get_loyalty_df(
    points_usage_prep_df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    select column from cb multidim for get tsel point

    Args:
        points_usage_prep_df: dataframe which add column previous and difference tsel point

    Returns:
        dataframe which contain tsel loyalty tier and segment
    """

    return points_usage_prep_df.select(
        "msisdn",
        "trx_date",
        next_week_start_day("trx_date").alias("weekstart"),
        "points_difference",
        f.col("loyalty_tier").alias("fea_loyalty_tier"),
        f.col("loyalty_segment").alias("fea_loyalty_segment"),
    )


def get_loyalty_points_usage_df(
    loyalty_df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    add column from dataframe which contain tsel loyalty tier and segment

    Args:
        loyalty_df: dataframe which contain tsel loyalty tier and segment

    Returns:
        dataframe with create rolling windows for point earned
    """

    return (
        loyalty_df.withColumn(
            "points_earned",
            f.when(
                f.col("points_difference") > 0, f.col("points_difference")
            ).otherwise(f.lit(0)),
        )
        .withColumn(
            "points_used",
            f.when(
                f.col("points_difference") < 0, f.abs(f.col("points_difference"))
            ).otherwise(f.lit(0)),
        )
        .withColumn(
            "points_earned_01m", f.sum("points_earned").over(get_rolling_window(30))
        )
        .withColumn(
            "points_earned_03m", f.sum("points_earned").over(get_rolling_window(90))
        )
        .withColumn(
            "points_used_01m", f.sum("points_used").over(get_rolling_window(30))
        )
        .withColumn(
            "points_used_03m", f.sum("points_used").over(get_rolling_window(90))
        )
        .withColumn(
            "fea_avg_points_earned_03m", (f.col("points_earned_03m") / f.lit(90))
        )
        .withColumn("fea_avg_points_used_03m", (f.col("points_used_03m") / f.lit(90)))
        .withColumn(
            "fea_trd_point_earned_03m",
            f.col("points_earned_01m") / f.col("points_earned_03m"),
        )
        .withColumn(
            "fea_trd_point_usage_03m",
            f.col("points_used_01m") / f.col("points_used_03m"),
        )
    )


def feat_customer_loyalty(
    cb_multidim_df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Returns customer loyalty tier and points usage features

    Args:
        cb_multidim_df: Multidim table

    Returns:
        - msisdn: Unique identifier
        - weekstart: week attributed to the month
        - fea_loyalty_tier: loyalty tier of msisdn
        - fea_loyalty_segment: loyalty segment of msisdn
        - points_earned_01m: points earned in last 30 days
        - points_earned_03m: points earned in last 90 days
        - points_used_01m: points used in last 30 days
        - points_used_03m: points used in last 90 days
        - fea_avg_points_earned_03m: average points earned in last 90 days
        - fea_avg_points_used_03m: average points used in last 90 days
        - fea_trd_point_earned_03m: trend of points earned last 30 days versus last 90 days
        - fea_trd_point_usage_03m: trend of points used last 30 days versus last 90 days
    """

    output_columns = [
        "msisdn",
        "weekstart",
        "fea_loyalty_tier",
        "fea_loyalty_segment",
        "points_earned_01m",
        "points_earned_03m",
        "points_used_01m",
        "points_used_03m",
        "fea_avg_points_earned_03m",
        "fea_avg_points_used_03m",
        "fea_trd_point_earned_03m",
        "fea_trd_point_usage_03m",
    ]

    cb_multidim_df = cb_multidim_df.filter(
        f.to_date(f.col("event_date"), "yyyy-MM-dd")
        >= f.to_date(f.lit("2019-06-01"), "yyyy-MM-dd")
    )

    points_usage_prep_df = get_usage_prep_df(cb_multidim_df)
    loyalty_df = get_loyalty_df(points_usage_prep_df)
    loyalty_points_usage_df = get_loyalty_points_usage_df(loyalty_df)

    return (
        loyalty_points_usage_df.withColumn(
            "rank", f.row_number().over(get_window_to_pick_last_row())
        )
        .filter(f.col("rank") == 1)
        .select(output_columns)
        .repartition(1000)
    )
