from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._04_features.mobility.nodes.fea_home_unique_count import (
    mobility_home_stay_unique_count_features,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestFeaHomeUniqueCount:
    def test_fea_home_unique_count(self, spark_session: SparkSession):
        home_agg_df = spark_session.createDataFrame(
            data=[
                [
                    "imsi_1",
                    "2020-01",
                    33860.0,
                    "qqu5d",
                    "kelurahan_1",
                    "neighbor_kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    24,
                    33860.0,
                    1,
                    -6.38925,
                    106.98825,
                    1,
                    "2000-01-20",
                ],
                [
                    "imsi_1",
                    "2020-02",
                    33860.0,
                    "qqu5d",
                    "kelurahan_2",
                    "neighbor_kelurahan_2",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    24,
                    33860.0,
                    1,
                    -6.3892,
                    106.9882,
                    1,
                    "2000-01-21",
                ],
                [
                    "imsi_1",
                    "2020-03",
                    33860.0,
                    "qqu5d",
                    "kelurahan_4",
                    "neighbor_kelurahan_5",
                    "kecamatan_2",
                    "kabu_4",
                    "provinc_6",
                    24,
                    33860.0,
                    1,
                    -6.38925,
                    106.985,
                    1,
                    "2000-01-22",
                ],
                [
                    "imsi_1",
                    "2020-04",
                    33860.0,
                    "qqu5d",
                    "kelurahan_2",
                    "neighbor_kelurahan_2",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    24,
                    33860.0,
                    1,
                    -6.3925,
                    106.9825,
                    1,
                    "2000-01-23",
                ],
                [
                    "imsi_1",
                    "2020-05",
                    33860.0,
                    "qqu5d",
                    "kelurahan_5",
                    "neighbor_kelurahan_5",
                    "kecamatan_6",
                    "kabu_3",
                    "provinc_7",
                    24,
                    33860.0,
                    1,
                    -6.385,
                    106.825,
                    1,
                    "2000-01-25",
                ],
                [
                    "imsi_1",
                    "2020-06",
                    33860.0,
                    "qqu5d",
                    "kelurahan_5",
                    "neighbor_kelurahan_5",
                    "kecamatan_6",
                    "kabu_3",
                    "provinc_7",
                    24,
                    33860.0,
                    1,
                    -6.385,
                    106.825,
                    1,
                    "2000-01-26",
                ],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("mo_id", StringType(), False),
                    StructField("home1_duration_max", DoubleType(), True),
                    StructField("geohash", StringType(), False),
                    StructField("home1_kelurahan_name", StringType(), True),
                    StructField("home1_neighboring_kelurahan_name", StringType(), True),
                    StructField("home1_kecamatan_name", StringType(), True),
                    StructField("home1_kabupaten_name", StringType(), True),
                    StructField("home1_province_name", StringType(), True),
                    StructField("home1_days", LongType(), True),
                    StructField("home1_duration", DoubleType(), True),
                    StructField("village_cnt", LongType(), True),
                    StructField("home1_lat", DoubleType(), True),
                    StructField("home1_lon", DoubleType(), True),
                    StructField("duration_max_flag", IntegerType(), False),
                    StructField("month_mapped_dt", StringType(), False),
                ]
            ),
        )

        actual_fea_df = mobility_home_stay_unique_count_features(home_agg_df)

        expected_fea_df = spark_session.createDataFrame(
            data=[
                ["imsi_1", "2020-01", 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                ["imsi_1", "2020-02", 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1],
                ["imsi_1", "2020-03", 1, 1, 1, 1, 3, 2, 1, 1, 3, 2, 1, 1],
                ["imsi_1", "2020-04", 1, 1, 1, 1, 2, 2, 1, 1, 3, 2, 1, 1],
                ["imsi_1", "2020-05", 1, 1, 1, 1, 2, 2, 2, 2, 4, 3, 2, 2],
                ["imsi_1", "2020-06", 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 2, 2],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("mo_id", StringType(), False),
                    StructField(
                        "fea_mobility_kelurahan_home_cnt_distinct_01m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_kecamatan_home_cnt_distinct_01m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_kabupaten_home_cnt_distinct_01m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_province_home_cnt_distinct_01m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_kelurahan_home_cnt_distinct_03m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_kecamatan_home_cnt_distinct_03m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_kabupaten_home_cnt_distinct_03m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_province_home_cnt_distinct_03m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_kelurahan_home_cnt_distinct_06m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_kecamatan_home_cnt_distinct_06m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_kabupaten_home_cnt_distinct_06m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_province_home_cnt_distinct_06m",
                        IntegerType(),
                        True,
                    ),
                ]
            ),
        )

        assert_df_frame_equal(actual_fea_df, expected_fea_df)
