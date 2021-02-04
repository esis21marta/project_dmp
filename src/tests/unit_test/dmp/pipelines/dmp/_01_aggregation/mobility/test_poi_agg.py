from unittest import mock

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src.dmp.pipelines.dmp._01_aggregation.mobility.nodes.poi_agg import (
    mobility_poi_agg,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestMobilityPoiStayAgg:
    @mock.patch(
        "src.dmp.pipelines.dmp._01_aggregation.mobility.nodes.poi_agg.encode",
        return_value="qqu5d",
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._01_aggregation.mobility.nodes.poi_agg.neighbors",
        return_value=[
            ["qquh2", "qquh6", "qquh8", "qquh9", "qquhd", "qquh0", "qquh1", "qquh4"]
        ],
        autospec=True,
    )
    def test_mobility_poi_agg(
        self, mock_encode, mock_neighbors, spark_session: SparkSession
    ):
        ext_poi_big_df = spark_session.createDataFrame(
            data=[
                ["Transportation", "Parking", "AR", -6.3892, 106.9882,],
                ["Utilities", "Power House", "AR", -6.3895, 106.9885,],
            ],
            schema=StructType(
                [
                    StructField("category", StringType(), False),
                    StructField("subcategory", StringType(), False),
                    StructField("shp_type", StringType(), False),
                    StructField("latitude", DoubleType(), True),
                    StructField("longitude", DoubleType(), True),
                ]
            ),
        )

        ext_osm_pois_df = spark_session.createDataFrame(
            data=[
                ["supermarket", -6.3892, 106.9882,],
                ["hotel", -6.38925, 106.98825,],
                ["post_office", -6.3892, 106.9882,],
            ],
            schema=StructType(
                [
                    StructField("category", StringType(), False),
                    StructField("latitude", DoubleType(), True),
                    StructField("longitude", DoubleType(), True),
                ]
            ),
        )

        cat_1_dict = {"supermarket": "utility", "parking": "social"}

        cat_2_dict = {"supermarket": "shop", "hotel": "tourism"}

        actual_fea_df = mobility_poi_agg(
            ext_poi_big_df, ext_osm_pois_df, cat_1_dict, cat_2_dict
        )

        actual_fea_df.show(10, 0)

        actual_fea_df.printSchema()

        expected_fea_df = spark_session.createDataFrame(
            data=[
                ["supermarket", 106.9882, -6.3892, "utility", "shop", "qqu5d", "qqu59"],
                ["supermarket", 106.9882, -6.3892, "utility", "shop", "qqu5d", "qqu5e"],
                ["supermarket", 106.9882, -6.3892, "utility", "shop", "qqu5d", "qqu5c"],
                ["supermarket", 106.9882, -6.3892, "utility", "shop", "qqu5d", "qqu5f"],
                ["supermarket", 106.9882, -6.3892, "utility", "shop", "qqu5d", "qqu5g"],
                ["supermarket", 106.9882, -6.3892, "utility", "shop", "qqu5d", "qqu53"],
                ["supermarket", 106.9882, -6.3892, "utility", "shop", "qqu5d", "qqu56"],
                ["supermarket", 106.9882, -6.3892, "utility", "shop", "qqu5d", "qqu57"],
            ],
            schema=StructType(
                [
                    StructField("category", StringType(), False),
                    StructField("longitude", DoubleType(), True),
                    StructField("latitude", DoubleType(), True),
                    StructField("category_type_1", StringType(), True),
                    StructField("category_type_2", StringType(), True),
                    StructField("poi_geohash", StringType(), False),
                    StructField("poi_neighbor_geohash", StringType(), True),
                ]
            ),
        )

        expected_fea_df.show(1, 0)
        expected_fea_df.printSchema()

        assert_df_frame_equal(actual_fea_df, expected_fea_df)
