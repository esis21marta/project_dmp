from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._04_features.mobility.nodes.fea_poi_freq import (
    mobility_poi_freq_features,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestFeaPoiFreq:
    def test_fea_poi_freq(self, spark_session: SparkSession):
        staypoint_df = spark_session.createDataFrame(
            data=[
                [
                    "imsi_1",
                    "2020-01",
                    "qqu5d",
                    7200,
                    "kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    -6.38925,
                    106.98825,
                    1,
                    "2000-01-20",
                    7200,
                    1,
                ],
                [
                    "imsi_1",
                    "2020-02",
                    "qqu5d",
                    14880,
                    "kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    -6.38925,
                    106.98825,
                    1,
                    "2000-01-21",
                    14880,
                    1,
                ],
                [
                    "imsi_1",
                    "2020-04",
                    "qqu5d",
                    11280,
                    "kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    -6.38925,
                    106.98825,
                    1,
                    "2000-01-23",
                    11280,
                    1,
                ],
                [
                    "imsi_1",
                    "2020-06",
                    "qqu5d",
                    29280,
                    "kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    -6.38925,
                    106.98825,
                    1,
                    "2000-01-25",
                    29280,
                    1,
                ],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("mo_id", StringType(), False),
                    StructField("geohash", StringType(), False),
                    StructField("agg_duration", LongType(), True),
                    StructField("kelurahan_name", StringType(), True),
                    StructField("kecamatan_name", StringType(), True),
                    StructField("kabupaten_name", StringType(), True),
                    StructField("province_name", StringType(), True),
                    StructField("lat1", DoubleType(), True),
                    StructField("long1", DoubleType(), True),
                    StructField("village_cnt", LongType(), False),
                    StructField("month_mapped_dt", StringType(), False),
                    StructField("agg_duration_max", LongType(), False),
                    StructField("duration_max_flag", IntegerType(), False),
                ]
            ),
        )

        poi_agg_df = spark_session.createDataFrame(
            data=[
                [
                    "supermarket",
                    106.9882,
                    -6.3892,
                    "commercial",
                    "commercial-store",
                    "qqu57",
                    "qqu59",
                ],
                [
                    "supermarket",
                    106.9882,
                    -6.3892,
                    "commercial",
                    "commercial-store",
                    "qqu57",
                    "qqu5e",
                ],
                [
                    "supermarket",
                    106.9882,
                    -6.3892,
                    "commercial",
                    "commercial-store",
                    "qqu57",
                    "qqu5c",
                ],
                [
                    "supermarket",
                    106.9882,
                    -6.3892,
                    "commercial",
                    "commercial-store",
                    "qqu57",
                    "qqu5f",
                ],
                [
                    "supermarket",
                    106.9882,
                    -6.3892,
                    "commercial",
                    "commercial-store",
                    "qqu57",
                    "qqu5g",
                ],
                [
                    "supermarket",
                    106.9882,
                    -6.3892,
                    "commercial",
                    "commercial-store",
                    "qqu57",
                    "qqu53",
                ],
                [
                    "supermarket",
                    106.9882,
                    -6.3892,
                    "commercial",
                    "commercial-store",
                    "qqu57",
                    "qqu56",
                ],
                [
                    "supermarket",
                    106.9882,
                    -6.3892,
                    "commercial",
                    "commercial-store",
                    "qqu57",
                    "qqu5d",
                ],
                [
                    "mall",
                    106.98825,
                    -6.3892,
                    "commercial",
                    "commercial-store",
                    "qqu5d",
                    "qqu59",
                ],
                [
                    "mall",
                    106.98825,
                    -6.3892,
                    "commercial",
                    "commercial-store",
                    "qqu5d",
                    "qqu5e",
                ],
                [
                    "mall",
                    106.98825,
                    -6.3892,
                    "commercial",
                    "commercial-store",
                    "qqu5d",
                    "qqu5c",
                ],
                [
                    "mall",
                    106.98825,
                    -6.3892,
                    "commercial",
                    "commercial-store",
                    "qqu5d",
                    "qqu5f",
                ],
                [
                    "mall",
                    106.98825,
                    -6.3892,
                    "commercial",
                    "commercial-store",
                    "qqu5d",
                    "qqu5g",
                ],
                [
                    "mall",
                    106.98825,
                    -6.3892,
                    "commercial",
                    "commercial-store",
                    "qqu5d",
                    "qqu53",
                ],
                [
                    "mall",
                    106.98825,
                    -6.3892,
                    "commercial",
                    "commercial-store",
                    "qqu5d",
                    "qqu56",
                ],
                [
                    "mall",
                    106.98825,
                    -6.3892,
                    "commercial",
                    "commercial-store",
                    "qqu5d",
                    "qqu57",
                ],
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

        actual_fea_df = mobility_poi_freq_features(staypoint_df, poi_agg_df)

        actual_fea_df.show(10, 0)
        actual_fea_df.printSchema()

        expected_fea_df = spark_session.createDataFrame(
            data=[
                ["imsi_1", "2020-01", 1, 1, 1,],
                ["imsi_1", "2020-02", 1, 2, 2,],
                ["imsi_1", "2020-04", 1, 2, 3,],
                ["imsi_1", "2020-06", 1, 2, 4,],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("mo_id", StringType(), False),
                    StructField(
                        "fea_mobility_poi_cat_1_commercial_visit_cnt_01m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_poi_cat_1_commercial_visit_cnt_03m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_poi_cat_1_commercial_visit_cnt_06m",
                        LongType(),
                        True,
                    ),
                ]
            ),
        )

        assert_df_frame_equal(actual_fea_df, expected_fea_df)
