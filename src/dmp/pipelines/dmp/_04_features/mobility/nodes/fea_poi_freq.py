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

from utils import get_rolling_window


def mobility_poi_freq_features(
    staypoint: pyspark.sql.DataFrame, poi_agg: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    col_list = ["msisdn", "mo_id", "geohash", "month_mapped_dt"]

    df_staypoint = (
        staypoint.select(*col_list)
        .groupBy("msisdn", "mo_id", "geohash", "month_mapped_dt")
        .agg(f.count(f.col("msisdn")).alias("staypoint_count"))
    )

    df_staypoint_agg = (
        df_staypoint.withColumn(
            "staypoint_cnt_01m",
            f.sum("staypoint_count").over(
                get_rolling_window(
                    1, key="msisdn", oby="month_mapped_dt", optional_keys=["geohash"],
                )
            ),
        )
        # .withColumn(
        #     "staypoint_cnt_02m",
        #     f.sum("staypoint_count").over(
        #         get_rolling_window(
        #             2, key="msisdn", oby="month_mapped_dt", optional_keys=["geohash"],
        #         )
        #     ),
        # )
        .withColumn(
            "staypoint_cnt_03m",
            f.sum("staypoint_count").over(
                get_rolling_window(
                    3, key="msisdn", oby="month_mapped_dt", optional_keys=["geohash"],
                )
            ),
        ).withColumn(
            "staypoint_cnt_06m",
            f.sum("staypoint_count").over(
                get_rolling_window(
                    6, key="msisdn", oby="month_mapped_dt", optional_keys=["geohash"],
                )
            ),
        )
        # .withColumn(
        #     "staypoint_cnt_09m",
        #     f.sum("staypoint_count").over(
        #         get_rolling_window(
        #             9, key="msisdn", oby="month_mapped_dt", optional_keys=["geohash"],
        #         )
        #     ),
        # )
    )

    df_join = df_staypoint_agg.join(
        (
            poi_agg.selectExpr(
                "category_type_1", "category_type_2", "poi_geohash"
            ).distinct()
        ),
        ((df_staypoint_agg.geohash == poi_agg.poi_geohash)),
    ).selectExpr(
        "msisdn",
        "mo_id",
        "category_type_1",
        "category_type_2",
        "staypoint_cnt_01m",
        # "staypoint_cnt_02m",
        "staypoint_cnt_03m",
        "staypoint_cnt_06m",
        # "staypoint_cnt_09m",
    )

    df_cat_1_poi_features = (
        df_join.where(
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
        .withColumn("cat_prefix", f.lit("fea_mobility_poi_cat_1").cast(t.StringType()))
        .withColumn(
            "category_renamed",
            f.concat_ws("_", f.col("cat_prefix"), f.col("category_type_1"),),
        )
        .groupBy("msisdn", "mo_id")
        .pivot("category_renamed")
        .agg(
            f.sum(f.col("staypoint_cnt_01m")).alias("visit_cnt_01m"),
            # f.sum(f.col("staypoint_cnt_02m")).alias(
            #     "visit_cnt_02m"
            # ),
            f.sum(f.col("staypoint_cnt_03m")).alias("visit_cnt_03m"),
            f.sum(f.col("staypoint_cnt_06m")).alias("visit_cnt_06m"),
            # f.sum(f.col("staypoint_cnt_09m")).alias(
            #     "visit_cnt_09m"
            # ),
        )
    )

    df_cat_2_poi_features = (
        df_join.where(
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
        .withColumn("cat_prefix", f.lit("fea_mobility_poi_cat_2").cast(t.StringType()))
        .withColumn(
            "category_renamed",
            f.concat_ws("_", f.col("cat_prefix"), f.col("category_type_2"),),
        )
        .groupBy("msisdn", "mo_id")
        .pivot("category_renamed")
        .agg(
            f.sum(f.col("staypoint_cnt_01m")).alias("visit_cnt_01m"),
            # f.sum(f.col("staypoint_cnt_02m")).alias(
            #     "visit_cnt_02m"
            # ),
            f.sum(f.col("staypoint_cnt_03m")).alias("visit_cnt_03m"),
            f.sum(f.col("staypoint_cnt_06m")).alias("visit_cnt_06m"),
            # f.sum(f.col("staypoint_cnt_09m")).alias(
            #     "visit_cnt_09m"
            # ),
        )
    )

    df_final = (
        (df_staypoint.select("msisdn", "mo_id",).distinct())
        .join(df_cat_1_poi_features, on=["msisdn", "mo_id"], how="left")
        .join(df_cat_2_poi_features, on=["msisdn", "mo_id"], how="left")
    )

    return df_final
