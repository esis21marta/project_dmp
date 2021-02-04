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

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.dmp.pipelines.dmp._04_features.outlets.geospatial.fea_pois_for_outlets import (
    calc_closest_pois_for_outlets,
    calc_radius_based_features,
)
from utils import calc_distance_bw_2_lat_long


class TestGeospatialFeatures:
    def test_calc_closest_pois_for_outlets(self, spark_session: SparkSession):
        sc = spark_session.sparkContext
        df_test_data = sc.parallelize(
            [
                ("1", "cafe", 10),
                ("1", "restaurant", 20),
                ("1", "cafe", 110),
                ("1", "bar", 20),
                ("2", "mall", 10),
                ("2", "bar", 10),
                ("2", "bar", None),
                ("2", "bar", None),
            ]
        )

        df_with_dist = spark_session.createDataFrame(
            df_test_data.map(
                lambda x: Row(outlet_id=x[0], subcategory=x[1], distance=x[2])
            )
        )
        df_with_dist = df_with_dist.coalesce(1)
        df_result = calc_closest_pois_for_outlets(
            df_with_dist, category_column="subcategory"
        ).orderBy("outlet_id")
        out_list = [[i[0], i[1], i[2], i[3], i[4]] for i in df_result.collect()]
        assert out_list == [
            ["1", 20, 10, None, 20],
            ["2", 10, None, 10, None],
        ]

    def test_calc_distance_between_2_lat_long(self, spark_session: SparkSession):
        sc = spark_session.sparkContext
        df_outlets = sc.parallelize(
            [
                ("70.123", "40.7486", "71.234", "42.123"),
                ("73.123", "39.7486", "61.234", "42.123"),
            ]
        )
        df_outlets = spark_session.createDataFrame(
            df_outlets.map(lambda x: Row(lat1=x[0], long1=x[1], lat2=x[2], long2=x[3]))
        )
        out = calc_distance_bw_2_lat_long(df_outlets)
        out_list = [[i[0]] for i in out.select("distance").collect()]
        assert sorted(out_list) == [[133477.5128442087], [1325700.7907414343]]

    def test_calc_radius_based_features(self, spark_session: SparkSession):
        schema = StructType(
            [
                StructField("outlet_id", StringType(), True),
                StructField("subcategory", StringType(), True),
                StructField("distance", IntegerType(), True),
            ]
        )

        df_outlet_poi = spark_session.createDataFrame(
            [
                ("1100000001", "cafe", 100),
                ("1100000001", "cafe", 600),
                ("1100000001", "cafe", 1200),
                ("1100000001", "cafe", 2600),
                ("1100000001", "cafe", 5800),
                ("1100000001", "bar", 1200),
                ("1100000001", "bar", 2200),
                ("1100000001", "bar", 4200),
            ],
            schema=schema,
        )

        df_outlet_poi = df_outlet_poi.coalesce(1)
        df_result = calc_radius_based_features(
            df_outlet_poi, category_column="subcategory", fea_prefix="fea_outlets_"
        )

        cols_of_interest = [
            "outlet_id",
            "fea_outlets_bar_under_200m",
            "fea_outlets_bar_under_500m",
            "fea_outlets_bar_under_1000m",
            "fea_outlets_bar_under_2500m",
            "fea_outlets_bar_under_5000m",
            "fea_outlets_cafe_under_200m",
            "fea_outlets_cafe_under_500m",
            "fea_outlets_cafe_under_1000m",
            "fea_outlets_cafe_under_2500m",
            "fea_outlets_cafe_under_5000m",
        ]
        out_list = [
            [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10]]
            for i in df_result.select(cols_of_interest).collect()
        ]

        assert out_list == [["1100000001", 0, 0, 0, 2, 3, 1, 1, 2, 3, 4]]
