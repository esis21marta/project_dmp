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
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.window import Window


def mobility_staypoint_covid_monthly_stats(
    staypoint_agg: pyspark.sql.DataFrame, covid_agg: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    stay_point_agg_window = Window.partitionBy(f.col("msisdn"), f.col("mo_id")).orderBy(
        f.col("agg_duration").desc()
    )

    staypoint_pri = (
        staypoint_agg.where(f.col("mo_id") >= "2020-01")
        .withColumn("stay_rank", f.rank().over(stay_point_agg_window))
        .where(f.col("stay_rank") <= 1)
        .groupBy("msisdn", "mo_id", "stay_rank")
        .agg(f.first(f.col("geohash")).alias("geohash"))
    )

    staypoint_covid = staypoint_pri.join(
        (
            covid_agg.selectExpr(
                "mo_id",
                "suspect_avg",
                "positive_avg",
                "probable_avg",
                "dead_avg",
                "suspect_max",
                "positive_max",
                "probable_max",
                "dead_max",
                "geohash",
                "covid_cluster_flag",
            )
        ),
        on=["geohash", "mo_id"],
    ).drop("geohash")

    staypoint_covid_features = (
        staypoint_covid.withColumn(
            "prefix", f.lit("fea_mobility_staypoint").cast(t.StringType())
        )
        .withColumn(
            "stay_rank_renamed",
            f.concat_ws("_", f.col("prefix"), f.col("stay_rank").cast(t.StringType())),
        )
        .groupBy("msisdn", "mo_id")
        .pivot("stay_rank_renamed")
        .agg(
            f.max("covid_cluster_flag").alias("covid_cluster_flag"),
            f.max("suspect_avg").alias("suspect_avg"),
            f.max("positive_avg").alias("positive_avg"),
            f.max("probable_avg").alias("probable_avg"),
            f.max("dead_avg").alias("dead_avg"),
            f.max("suspect_max").alias("suspect_max"),
            f.max("positive_max").alias("positive_max"),
            f.max("probable_max").alias("probable_max"),
            f.max("dead_max").alias("dead_max"),
        )
    )

    return staypoint_covid_features
