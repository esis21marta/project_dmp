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

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from src.dmp.pipelines.dmp._01_aggregation.outlets.geospatial.agg_outlet_geospatial import (
    _pick_outlets_with_highest_count_for_lat_log,
    get_all_neighbours_for_fb_points,
    get_most_recent_outlet_classification,
)


class TestOutletsGeospatialAggregation:
    def test_pick_outlets_with_highest_count_for_lat_log(
        self, spark_session: SparkSession
    ):
        schema = StructType(
            [
                StructField("outlet_id", StringType(), True),
                StructField("trx_date", StringType(), True),
                StructField("lattitude", StringType(), True),
                StructField("longitude", StringType(), True),
                StructField("kabupaten", StringType(), True),
                StructField("tipe_outlet", StringType(), True),
                StructField("cluster", StringType(), True),
                StructField("branch", StringType(), True),
                StructField("sub_branch", StringType(), True),
                StructField("kecamatan", StringType(), True),
                StructField("kelurahan", StringType(), True),
            ]
        )

        df_outlet = spark_session.createDataFrame(
            [
                (
                    "11000000010",
                    "2019-01-01",
                    "5.2363256663",
                    "96.2428592041",
                    "PIDIE JAYA",
                    "Warung",
                    "ACEH",
                    "BANDA ACEH",
                    "BANDA ACEH",
                    "MEUREUDU",
                    "DAYAH TIMU",
                ),
                (
                    "11000000010",
                    "2019-01-02",
                    "5.2363256664",
                    "96.2428592042",
                    "JAVA",
                    "Warung",
                    "ACEH",
                    "BANDA ACEH",
                    "BANDA ACEH",
                    "MEUREUDU",
                    "DAYAH TIMU",
                ),
                (
                    "11000000010",
                    "2019-01-03",
                    "5.2363256665",
                    "96.2428592042",
                    "SUMARTRA",
                    "Warung",
                    "ACEH",
                    "BANDA ACEH",
                    "BANDA ACEH",
                    "MEUREUDU",
                    "DAYAH TIMU",
                ),
                (
                    "11000000020",
                    "2019-01-04",
                    "5.4363256663",
                    "96.4428592041",
                    "JAVA",
                    "Warung",
                    "ACEH",
                    "BANDA ACEH",
                    "BANDA ACEH",
                    "MEUREUDU",
                    "DAYAH TIMU",
                ),
            ],
            schema=schema,
        )

        df_outlet = df_outlet.coalesce(1)
        df_result = _pick_outlets_with_highest_count_for_lat_log(df_outlet)
        out_list = [
            [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10]]
            for i in df_result.collect()
        ]

        assert out_list == [
            [
                "11000000020",
                "5.4363256663",
                "96.4428592041",
                "JAVA",
                "Warung",
                "ACEH",
                "w0uwf",
                "BANDA ACEH",
                "BANDA ACEH",
                "MEUREUDU",
                "DAYAH TIMU",
            ]
        ]

    def test_get_most_recent_outlet_classification(self, spark_session: SparkSession):
        schema = StructType(
            [
                StructField("outlet_id", StringType(), True),
                StructField("trx_date", StringType(), True),
                StructField("klasifikasi", StringType(), True),
            ]
        )

        df_outlet_classification = spark_session.createDataFrame(
            [
                ("11000000010", "2019-01-02", "SILVER"),
                ("11000000010", "2019-02-01", "BRONZE"),
                ("11000000080", "2019-01-03", "BRONZE"),
                ("11000000080", "2019-01-04", "PLATINUM"),
            ],
            schema=schema,
        )

        df_outlet_classification = df_outlet_classification.coalesce(1)
        df_result = get_most_recent_outlet_classification(
            df_outlet_classification
        ).orderBy("outlet_id")
        out_list = [[i[0], i[1], i[2], i[3]] for i in df_result.collect()]
        assert out_list == [
            ["11000000010", "2019-02-01", "BRONZE", 1],
            ["11000000080", "2019-01-04", "PLATINUM", 1],
        ]

    def test_get_all_neighbours_for_fb_points(self, spark_session: SparkSession):
        schema = StructType(
            [
                StructField("latitude", StringType(), True),
                StructField("longitude", StringType(), True),
            ]
        )

        df_fb_pop = spark_session.createDataFrame(
            [
                ("-7.710972309112549", "112.17736053466797"),
                ("-7.710972309112549", "112.2459716796875"),
            ],
            schema=schema,
        )

        df_fb_pop = df_fb_pop.coalesce(1)
        df_fb_pop_with_neighbours = get_all_neighbours_for_fb_points(df_fb_pop)
        df_neighbours = (
            df_fb_pop_with_neighbours.groupBy("lat2", "long2")
            .agg(f.array_sort(f.collect_set("neighbours_all")).alias("neighbours"))
            .orderBy("lat2", "long2")
        )

        out_list = [[i[0], i[1], i[2]] for i in df_neighbours.collect()]
        assert out_list == [
            [
                "-7.710972309112549",
                "112.17736053466797",
                [
                    "qqxez",
                    "qqxgb",
                    "qqxgc",
                    "qqxsp",
                    "qqxsr",
                    "qqxu0",
                    "qqxu1",
                    "qqxu2",
                    "qqxu3",
                ],
            ],
            [
                "-7.710972309112549",
                "112.2459716796875",
                [
                    "qqxgc",
                    "qqxgf",
                    "qqxgg",
                    "qqxu1",
                    "qqxu3",
                    "qqxu4",
                    "qqxu5",
                    "qqxu6",
                    "qqxu7",
                ],
            ],
        ]

    def test_get_all_fb_pop_for_an_outlet(self, spark_session: SparkSession):
        schema_outlet = StructType(
            [
                StructField("outlet_id", StringType(), True),
                StructField("lat1", StringType(), True),
                StructField("long1", StringType(), True),
                StructField("neighbours_all", StringType(), True),
            ]
        )

        df_outlet = spark_session.createDataFrame(
            [
                # 1st Outlet - 32010005820
                ("32010005820", "-7.729209899902344", "112.07933044433594", "qqxsn"),
                ("32010005820", "-7.729209899902344", "112.07933044433594", "qqxsj"),
                ("32010005820", "-7.729209899902344", "112.07933044433594", "qqxsp"),
                ("32010005820", "-7.729209899902344", "112.07933044433594", "qqxsm"),
                ("32010005820", "-7.729209899902344", "112.07933044433594", "qqxsq"),
                ("32010005820", "-7.729209899902344", "112.07933044433594", "qqxsr"),
                ("32010005820", "-7.729209899902344", "112.07933044433594", "qqxev"),
                ("32010005820", "-7.729209899902344", "112.07933044433594", "qqxey"),
                ("32010005820", "-7.729209899902344", "112.07933044433594", "qqxez"),
                # 2nd Outlet - 32000192470
                ("32000192470", "-7.628637313842773", "112.18938446044922", "qqxu8"),
                ("32000192470", "-7.628637313842773", "112.18938446044922", "qqxsx"),
                ("32000192470", "-7.628637313842773", "112.18938446044922", "qqxu9"),
                ("32000192470", "-7.628637313842773", "112.18938446044922", "qqxsz"),
                ("32000192470", "-7.628637313842773", "112.18938446044922", "qqxub"),
                ("32000192470", "-7.628637313842773", "112.18938446044922", "qqxuc"),
                ("32000192470", "-7.628637313842773", "112.18938446044922", "qqxsr"),
                ("32000192470", "-7.628637313842773", "112.18938446044922", "qqxu2"),
                ("32000192470", "-7.628637313842773", "112.18938446044922", "qqxu3"),
            ],
            schema=schema_outlet,
        )

        df_outlet_with_neighbours = df_outlet.coalesce(1)

        schema = StructType(
            [
                StructField("latitude", StringType(), True),
                StructField("longitude", StringType(), True),
            ]
        )

        df_fb_pop = spark_session.createDataFrame(
            [
                ("-7.710972309112549", "112.17736053466797"),
                ("-7.710972309112549", "112.2459716796875"),
            ],
            schema=schema,
        )

        df_fb_pop = df_fb_pop.coalesce(1)
        df_fb_pop_with_neighbours = get_all_neighbours_for_fb_points(df_fb_pop)

        df_fbj = df_outlet_with_neighbours.join(
            f.broadcast(df_fb_pop_with_neighbours), ["neighbours_all"], how="inner"
        )
        df_fb_neighbours = (
            df_fbj.groupBy("outlet_id")
            .agg(f.array_sort(f.collect_list("neighbours_all")))
            .orderBy("outlet_id")
        )
        out_list = [[i[0], i[1]] for i in df_fb_neighbours.collect()]
        assert out_list == [
            ["32000192470", ["qqxsr", "qqxu2", "qqxu3", "qqxu3"]],
            ["32010005820", ["qqxez", "qqxsp", "qqxsr"]],
        ]
