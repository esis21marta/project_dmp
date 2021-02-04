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

from utils import calc_distance_bw_2_lat_long, neighbors


def mobility_staypoint_poi_features(
    stay_point_agg: pyspark.sql.DataFrame, poi_agg: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    neighbor_udf = f.udf(lambda x: neighbors(x), t.ArrayType(t.StringType()))

    stay_point_agg_window = Window.partitionBy(f.col("msisdn"), f.col("mo_id")).orderBy(
        f.col("agg_duration").desc()
    )

    df_stay_point_agg = (
        stay_point_agg.where(f.col("geohash").isNotNull())
        .withColumn("stay_rank", f.rank().over(stay_point_agg_window))
        .where(f.col("stay_rank") <= 1)
        .withColumn("geohash_neighbor", (neighbor_udf(stay_point_agg.geohash)))
        .withColumn("neighbor_geohash", f.explode(f.col("geohash_neighbor")))
        .drop("geohash_neighbor")
    )

    df_join = (
        df_stay_point_agg.join(
            poi_agg,
            ((df_stay_point_agg.neighbor_geohash == poi_agg.poi_neighbor_geohash)),
        )
        .selectExpr(
            "msisdn",
            "mo_id",
            "lat1",
            "long1",
            "latitude as lat2",
            "longitude as long2",
            "geohash",
            "poi_geohash",
            "agg_duration",
            "kelurahan_name",
            "kecamatan_name",
            "kabupaten_name",
            "province_name",
            "category_type_1",
            "category_type_2",
            "stay_rank",
        )
        .distinct()
    )

    df_staypoint_poi = (
        calc_distance_bw_2_lat_long(df_join).where(f.col("distance") <= 2000)
        # .withColumn("poi_100m", f.when(f.col("distance") <= 100, 1).otherwise(0))
        # .withColumn("poi_200m", f.when(f.col("distance") <= 200, 1).otherwise(0))
        # .withColumn("poi_500m", f.when(f.col("distance") <= 500, 1).otherwise(0))
        .withColumn("poi_1km", f.when(f.col("distance") <= 1000, 1).otherwise(0))
        # .withColumn("poi_2km", f.when(f.col("distance") <= 2000, 1).otherwise(0))
    )

    df_cat_1_poi_features = (
        df_staypoint_poi.select(
            "msisdn",
            "mo_id",
            "stay_rank",
            "category_type_1",
            "distance",
            # "poi_100m",
            # "poi_200m",
            # "poi_500m",
            "poi_1km",
            # "poi_2km",
        )
        .where(
            f.col("category_type_1").isin(
                "big_transport",
                "social_place",
                "residential",
                "industry_factory",
                "government",
                "education",
                "commercial",
                "commercial_food",
                "mall",
                "sport",
                "heritage_culture",
            )
        )
        .withColumn("cat_prefix", f.lit("cat_1").cast(t.StringType()))
        .withColumn("geo_prefix", f.lit("fea_mobility_staypoint").cast(t.StringType()))
        .withColumn(
            "category_renamed",
            f.concat_ws(
                "_",
                f.col("geo_prefix"),
                f.col("stay_rank").cast(t.StringType()),
                f.col("cat_prefix"),
                f.col("category_type_1"),
            ),
        )
        .groupBy("msisdn", "mo_id")
        .pivot("category_renamed")
        .agg(
            f.min(f.col("distance")).alias("min_poi_dist"),
            # f.sum(f.col("poi_100m")).alias("poi_100m_num"),
            # f.sum(f.col("poi_200m")).alias("poi_200m_num"),
            # f.sum(f.col("poi_500m")).alias("poi_500m_num"),
            f.sum(f.col("poi_1km")).alias("poi_1km_num"),
            # f.sum(f.col("poi_2km")).alias("poi_2km_num"),
        )
    )

    df_cat_2_poi_features = (
        df_staypoint_poi.select(
            "msisdn",
            "mo_id",
            "stay_rank",
            "category_type_2",
            "distance",
            # "poi_100m",
            # "poi_200m",
            # "poi_500m",
            "poi_1km",
            # "poi_2km",
        )
        .where(
            f.col("category_type_2").isin(
                "big_transport",
                "commercial_travelshop",
                "commercial_store",
                "commercial_entertainment",
                "residential",
                "industry_factory",
                "government",
                "education",
                "commercial",
                "commercial_food",
                "commercial_hotel",
                "sport",
                "heritage_culture",
            )
        )
        .withColumn("cat_prefix", f.lit("cat_2").cast(t.StringType()))
        .withColumn("geo_prefix", f.lit("fea_mobility_staypoint").cast(t.StringType()))
        .withColumn(
            "category_renamed",
            f.concat_ws(
                "_",
                f.col("geo_prefix"),
                f.col("stay_rank").cast(t.StringType()),
                f.col("cat_prefix"),
                f.col("category_type_2"),
            ),
        )
        .groupBy("msisdn", "mo_id")
        .pivot("category_renamed")
        .agg(
            f.min(f.col("distance")).alias("min_poi_dist"),
            # f.sum(f.col("poi_100m")).alias("poi_100m_num"),
            # f.sum(f.col("poi_200m")).alias("poi_200m_num"),
            # f.sum(f.col("poi_500m")).alias("poi_500m_num"),
            f.sum(f.col("poi_1km")).alias("poi_1km_num"),
            # f.sum(f.col("poi_2km")).alias("poi_2km_num"),
        )
    )

    df_final = (
        (
            stay_point_agg.selectExpr(
                "msisdn",
                "mo_id",
                # "kelurahan_name as staypoint_kelurahan_name",
                # "kecamatan_name as staypoint_kecamatan_name",
                # "kabupaten_name as staypoint_kabupaten_name",
                # "province_name as staypoint_province_name",
            ).distinct()
        ).join(df_cat_1_poi_features, on=["msisdn", "mo_id"], how="left")
    ).join(df_cat_2_poi_features, on=["msisdn", "mo_id"], how="left")

    return df_final
