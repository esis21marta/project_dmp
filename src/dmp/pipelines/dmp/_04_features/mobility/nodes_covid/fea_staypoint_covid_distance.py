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

from utils import calc_distance_bw_2_lat_long, get_rolling_window, neighbors


def mobility_staypoint_covid_distance_features(
    staypoint: pyspark.sql.DataFrame, covid_agg: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    neighbor_udf = f.udf(lambda x: neighbors(x), t.ArrayType(t.StringType()))

    # rolling windows
    w_2month = get_rolling_window(2, key="msisdn", oby="month_mapped_dt")
    w_3month = get_rolling_window(3, key="msisdn", oby="month_mapped_dt")

    covid_agg_neighbor = (
        covid_agg.where(f.col("covid_cluster_flag") == 1)
        .withColumn("neighbor_geohash", neighbor_udf(f.col("geohash")))
        .withColumn("geohash_neighbor", f.explode(f.col("neighbor_geohash")))
        .selectExpr(
            "geohash as covid_geohash",
            "mo_id as covid_mo_id",
            "lat2",
            "long2",
            "geohash_neighbor",
        )
        .distinct()
    )

    stay_point_agg_window = Window.partitionBy(f.col("msisdn"), f.col("mo_id")).orderBy(
        f.col("agg_duration").desc()
    )

    df_staypoint = (
        staypoint.where(
            f.col("mo_id").isin(
                "2020-01",
                "2020-02",
                "2020-03",
                "2020-04",
                "2020-05",
                "2020-06",
                "2020-07",
                "2020-08",
            )
        )
        .withColumn("stay_rank", f.rank().over(stay_point_agg_window))
        .where(f.col("stay_rank") <= 1)
        .selectExpr(
            "msisdn", "mo_id", "lat1", "long1", "geohash", "month_mapped_dt"
        )  # , "stay_rank")
        .distinct()
    )

    df_staypoint_pre_covid = (
        df_staypoint.where(f.col("mo_id").isin("2020-01", "2020-02"))
        .distinct()
        .join(
            covid_agg_neighbor,
            (
                (f.col("geohash") == f.col("covid_geohash"))
                | (f.col("geohash") == f.col("geohash_neighbor"))
            ),
        )
        .selectExpr(
            "msisdn", "mo_id", "lat1", "long1", "lat2", "long2", "month_mapped_dt"
        )  # , "stay_rank")
        .distinct()
    )

    df_pre_covid_min_distance = (
        calc_distance_bw_2_lat_long(df_staypoint_pre_covid)
        .groupBy("msisdn", "mo_id", "month_mapped_dt")  # , "stay_rank")
        .agg(f.min(f.col("distance")).alias("min_distance"))
        .withColumn("min_distance_jan_feb", f.min(f.col("min_distance")).over(w_2month))
        .withColumn("avg_distance_jan_feb", f.avg(f.col("min_distance")).over(w_2month))
    )

    df_pre_covid_min_distance_latest = (
        df_pre_covid_min_distance.groupBy("msisdn")
        .agg(f.max("mo_id").alias("mo_id"))
        .join(df_pre_covid_min_distance, on=["msisdn", "mo_id"])
        .selectExpr(
            "msisdn",
            "mo_id as mo_id_jan_feb",
            "min_distance_jan_feb as min_distance_jan_feb_final",
            "avg_distance_jan_feb as avg_distance_jan_feb_final",
        )
    )

    df_staypoint_covid = df_staypoint.join(
        covid_agg_neighbor,
        (
            (
                (f.col("mo_id") == f.col("covid_mo_id"))
                & (f.col("geohash") == f.col("covid_geohash"))
            )
            | (
                (f.col("mo_id") == f.col("covid_mo_id"))
                & (f.col("geohash") == f.col("geohash_neighbor"))
            )
        ),
    )

    df_covid_distance = calc_distance_bw_2_lat_long(df_staypoint_covid)

    df_post_covid_mar_apr_may = (
        df_covid_distance.where(f.col("mo_id").isin("2020-03", "2020-04", "2020-05"))
        .groupBy("msisdn", "mo_id", "month_mapped_dt")
        .agg(f.min(f.col("distance")).alias("min_distance"))
        .withColumn(
            "min_distance_mar_apr_may", f.min(f.col("min_distance")).over(w_3month)
        )
        .withColumn(
            "avg_distance_mar_apr_may", f.avg(f.col("min_distance")).over(w_3month)
        )
    )

    df_post_covid_mar_apr_may_latest = (
        df_post_covid_mar_apr_may.groupBy("msisdn")
        .agg(f.max("mo_id").alias("mo_id"))
        .join(df_post_covid_mar_apr_may, on=["msisdn", "mo_id"])
        .selectExpr(
            "msisdn",
            "mo_id as mo_id_mar_apr_may",
            "min_distance_mar_apr_may as min_distance_mar_apr_may_final",
            "avg_distance_mar_apr_may as avg_distance_mar_apr_may_final",
        )
    )

    df_post_covid_jun_jul_aug = (
        df_covid_distance.where(f.col("mo_id").isin("2020-06", "2020-07", "2020-08"))
        .groupBy("msisdn", "mo_id", "month_mapped_dt")
        .agg(f.min(f.col("distance")).alias("min_distance"))
        .withColumn(
            "min_distance_jun_jul_aug", f.min(f.col("min_distance")).over(w_3month)
        )
        .withColumn(
            "avg_distance_jun_jul_aug", f.avg(f.col("min_distance")).over(w_3month)
        )
    )

    df_post_covid_jun_jul_aug_latest = (
        df_post_covid_jun_jul_aug.groupBy("msisdn")
        .agg(f.max("mo_id").alias("mo_id"))
        .join(df_post_covid_jun_jul_aug, on=["msisdn", "mo_id"])
        .selectExpr(
            "msisdn",
            "mo_id as mo_id_jun_jul_aug",
            "min_distance_jun_jul_aug as min_distance_jun_jul_aug_final",
            "avg_distance_jun_jul_aug as avg_distance_jun_jul_aug_final",
        )
    )

    staypoint_covid_distance_final = (
        staypoint.selectExpr("msisdn", "mo_id")  # , "stay_rank")
        .distinct()
        .join(
            df_pre_covid_min_distance, on=["msisdn", "mo_id"], how="left"
        )  # , "stay_rank"]
        .join(
            df_post_covid_mar_apr_may, on=["msisdn", "mo_id"], how="left"
        )  # , "stay_rank"]
        .join(
            df_post_covid_jun_jul_aug, on=["msisdn", "mo_id"], how="left"
        )  # , "stay_rank"]
        .join(df_pre_covid_min_distance_latest, on=["msisdn"], how="left")
        .join(df_post_covid_mar_apr_may_latest, on=["msisdn"], how="left")
        .join(df_post_covid_jun_jul_aug_latest, on=["msisdn"], how="left")
        .withColumn(
            "fea_mobility_staypoint_covid_min_distance_jan_feb",
            f.when(
                f.col("mo_id") <= f.col("mo_id_jan_feb"), f.col("min_distance_jan_feb")
            ).otherwise(f.col("min_distance_jan_feb_final")),
        )
        .withColumn(
            "fea_mobility_staypoint_covid_avg_distance_jan_feb",
            f.when(
                f.col("mo_id") <= f.col("mo_id_jan_feb"), f.col("avg_distance_jan_feb")
            ).otherwise(f.col("avg_distance_jan_feb_final")),
        )
        .withColumn(
            "fea_mobility_staypoint_covid_min_distance_mar_apr_may",
            f.when(
                f.col("mo_id") <= f.col("mo_id_mar_apr_may"),
                f.col("min_distance_mar_apr_may"),
            ).otherwise(f.col("min_distance_mar_apr_may_final")),
        )
        .withColumn(
            "fea_mobility_staypoint_covid_avg_distance_mar_apr_may",
            f.when(
                f.col("mo_id") <= f.col("mo_id_mar_apr_may"),
                f.col("avg_distance_mar_apr_may"),
            ).otherwise(f.col("avg_distance_mar_apr_may_final")),
        )
        .withColumn(
            "fea_mobility_staypoint_covid_min_distance_jun_jul_aug",
            f.when(
                f.col("mo_id") <= f.col("mo_id_jun_jul_aug"),
                f.col("min_distance_jun_jul_aug"),
            ).otherwise(f.col("min_distance_jun_jul_aug_final")),
        )
        .withColumn(
            "fea_mobility_staypoint_covid_avg_distance_jun_jul_aug",
            f.when(
                f.col("mo_id") <= f.col("mo_id_jun_jul_aug"),
                f.col("avg_distance_jun_jul_aug"),
            ).otherwise(f.col("avg_distance_jun_jul_aug_final")),
        )
        .select(
            "msisdn",
            "mo_id",
            "fea_mobility_staypoint_covid_min_distance_jan_feb",
            "fea_mobility_staypoint_covid_avg_distance_jan_feb",
            "fea_mobility_staypoint_covid_min_distance_mar_apr_may",
            "fea_mobility_staypoint_covid_avg_distance_mar_apr_may",
            "fea_mobility_staypoint_covid_min_distance_jun_jul_aug",
            "fea_mobility_staypoint_covid_avg_distance_jun_jul_aug",
        )
    )

    return staypoint_covid_distance_final
