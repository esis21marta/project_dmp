from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._04_features.mobility.nodes_covid.fea_staypoint_covid_monthly_stats import (
    mobility_staypoint_covid_monthly_stats,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestFeaCovidStaypointCovidMonthlyStats:
    def test_fea_covid_staypoint_covid_monthly_stats(
        self, spark_session: SparkSession,
    ):
        covid_agg_df = spark_session.createDataFrame(
            data=[
                [
                    "2020-01",
                    "qquh9",
                    -6.24177,
                    106.933,
                    39.0,
                    16.0,
                    0.0,
                    5.0,
                    39.0,
                    16.0,
                    0.0,
                    5.0,
                    1,
                ],
                [
                    "2020-02",
                    "qquh9",
                    -6.24177,
                    106.933,
                    93.0,
                    10.0,
                    0.0,
                    4.0,
                    93.0,
                    10.0,
                    0.0,
                    4.0,
                    1,
                ],
                [
                    "2020-03",
                    "qquh9",
                    -6.24177,
                    106.933,
                    90.0,
                    1.0,
                    0.0,
                    2.0,
                    90.0,
                    1.0,
                    0.0,
                    2.0,
                    1,
                ],
                [
                    "2020-04",
                    "qquh9",
                    -6.24177,
                    106.933,
                    43.0,
                    6.0,
                    0.0,
                    3.0,
                    43.0,
                    6.0,
                    0.0,
                    3.0,
                    1,
                ],
                [
                    "2020-07",
                    "qquh9",
                    -6.24177,
                    106.933,
                    90.0,
                    1.0,
                    0.0,
                    2.0,
                    90.0,
                    1.0,
                    0.0,
                    2.0,
                    1,
                ],
                [
                    "2020-06",
                    "qquh9",
                    -6.24177,
                    106.933,
                    39.0,
                    16.0,
                    0.0,
                    5.0,
                    39.0,
                    16.0,
                    0.0,
                    5.0,
                    1,
                ],
                [
                    "2020-05",
                    "qquh9",
                    -6.24177,
                    106.933,
                    459.0,
                    106.0,
                    0.0,
                    1.0,
                    459.0,
                    106.0,
                    0.0,
                    1.0,
                    1,
                ],
            ],
            schema=StructType(
                [
                    StructField("mo_id", StringType(), False),
                    StructField("geohash", StringType(), False),
                    StructField("lat2", DoubleType(), True),
                    StructField("long2", DoubleType(), True),
                    StructField("suspect_avg", DoubleType(), True),
                    StructField("positive_avg", DoubleType(), True),
                    StructField("probable_avg", DoubleType(), True),
                    StructField("dead_avg", DoubleType(), True),
                    StructField("suspect_max", DoubleType(), True),
                    StructField("positive_max", DoubleType(), True),
                    StructField("probable_max", DoubleType(), True),
                    StructField("dead_max", DoubleType(), True),
                    StructField("covid_cluster_flag", IntegerType(), False),
                ]
            ),
        )

        staypoint_df = spark_session.createDataFrame(
            data=[
                [
                    "imsi_1",
                    "2020-01",
                    "qquh9",
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
                    "qquh9",
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

        actual_fea_df = mobility_staypoint_covid_monthly_stats(
            staypoint_df, covid_agg_df
        )

        expected_fea_df = spark_session.createDataFrame(
            data=[
                ["2020-01", "imsi_1", 1, 39.0, 16.0, 0.0, 5.0, 39.0, 16.0, 0.0, 5.0],
                ["2020-02", "imsi_1", 1, 93.0, 10.0, 0.0, 4.0, 93.0, 10.0, 0.0, 4.0],
            ],
            schema=StructType(
                [
                    StructField("mo_id", StringType(), False),
                    StructField("msisdn", StringType(), False),
                    StructField(
                        "fea_mobility_staypoint_1_covid_cluster_flag",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_staypoint_1_suspect_avg", DoubleType(), True
                    ),
                    StructField(
                        "fea_mobility_staypoint_1_positive_avg", DoubleType(), True
                    ),
                    StructField(
                        "fea_mobility_staypoint_1_probable_avg", DoubleType(), True
                    ),
                    StructField(
                        "fea_mobility_staypoint_1_dead_avg", DoubleType(), True
                    ),
                    StructField(
                        "fea_mobility_staypoint_1_suspect_max", DoubleType(), True
                    ),
                    StructField(
                        "fea_mobility_staypoint_1_positive_max", DoubleType(), True
                    ),
                    StructField(
                        "fea_mobility_staypoint_1_probable_max", DoubleType(), True
                    ),
                    StructField(
                        "fea_mobility_staypoint_1_dead_max", DoubleType(), True
                    ),
                ]
            ),
        )

        assert_df_frame_equal(actual_fea_df, expected_fea_df)
