from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._04_features.mobility.nodes.fea_staypoint_unique_count import (
    mobility_staypoint_unique_count_features,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestFeaStaypointUniqueCount:
    def test_fea_staypoint_unique_count(self, spark_session: SparkSession):
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
                    1480,
                    "kelurahan_5",
                    "kecamatan_6",
                    "kabu_6",
                    "provinc_7",
                    -6.8925,
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
                    "kelurahan_5",
                    "kecamatan_6",
                    "kabu_6",
                    "provinc_7",
                    -6.8925,
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

        actual_fea_df = mobility_staypoint_unique_count_features(staypoint_df)

        expected_fea_df = spark_session.createDataFrame(
            data=[
                ["imsi_1", "2020-01", 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                ["imsi_1", "2020-02", 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2],
                ["imsi_1", "2020-04", 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2],
                ["imsi_1", "2020-06", 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("mo_id", StringType(), False),
                    StructField(
                        "fea_mobility_staypoint_kelurahan_count_distinct_01m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_staypoint_kecamatan_count_distinct_01m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_staypoint_kabupaten_count_distinct_01m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_staypoint_province_count_distinct_01m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_staypoint_kelurahan_count_distinct_03m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_staypoint_kecamatan_count_distinct_03m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_staypoint_kabupaten_count_distinct_03m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_staypoint_province_count_distinct_03m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_staypoint_kelurahan_count_distinct_06m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_staypoint_kecamatan_count_distinct_06m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_staypoint_kabupaten_count_distinct_06m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_staypoint_province_count_distinct_06m",
                        IntegerType(),
                        True,
                    ),
                ]
            ),
        )

        assert_df_frame_equal(actual_fea_df, expected_fea_df)
