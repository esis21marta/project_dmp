from unittest import mock

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._04_features.mobility.nodes_covid.fea_staypoint_covid_distance import (
    mobility_staypoint_covid_distance_features,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestFeaCovidStaypointDistance:
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.mobility.nodes_covid.fea_staypoint_covid_distance.neighbors",
        return_value=[
            ["qquh2", "qquh6", "qquh8", "qquh9", "qquhd", "qquh0", "qquh1", "qquh4"]
        ],
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.mobility.nodes_covid.fea_staypoint_covid_distance.calc_distance_bw_2_lat_long",
    )
    def test_fea_covid_staypoint_distance(
        self,
        mock_calc_distance_bw_2_lat_long,
        mock_neighbors,
        spark_session: SparkSession,
    ):
        mock_calc_distance_bw_2_lat_long.return_value = spark_session.createDataFrame(
            data=[
                [
                    "imsi_1",
                    "2020-01",
                    -6.38925,
                    106.98825,
                    -6.24177,
                    106.933,
                    "2000-01-20",
                    17498.97736351866,
                ],
                [
                    "imsi_1",
                    "2020-02",
                    -6.38925,
                    106.98825,
                    -6.24177,
                    106.933,
                    "2000-01-21",
                    17498.97736351866,
                ],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("mo_id", StringType(), False),
                    StructField("lat1", DoubleType(), True),
                    StructField("long1", DoubleType(), True),
                    StructField("lat2", DoubleType(), True),
                    StructField("long2", DoubleType(), True),
                    StructField("month_mapped_dt", StringType(), True),
                    StructField("distance", DoubleType(), True),
                ]
            ),
        )

        covid_agg_df = spark_session.createDataFrame(
            data=[
                [
                    "2020-01",
                    "qquh3",
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
                    "qquh3",
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
                    "qquh3",
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
                    "qquh3",
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
                    "qquh3",
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
                    "qquh3",
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
                    "qquh3",
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
                    "qquh6",
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
                    "qquh6",
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
                    "qquh6",
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
                    "qquh6",
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

        actual_fea_df = mobility_staypoint_covid_distance_features(
            staypoint_df, covid_agg_df
        )

        expected_fea_df = spark_session.createDataFrame(
            data=[
                [
                    "imsi_1",
                    "2020-01",
                    17498.97736351866,
                    17498.97736351866,
                    None,
                    None,
                    None,
                    None,
                ],
                [
                    "imsi_1",
                    "2020-02",
                    17498.97736351866,
                    17498.97736351866,
                    None,
                    None,
                    None,
                    None,
                ],
                [
                    "imsi_1",
                    "2020-04",
                    17498.97736351866,
                    17498.97736351866,
                    None,
                    None,
                    None,
                    None,
                ],
                [
                    "imsi_1",
                    "2020-06",
                    17498.97736351866,
                    17498.97736351866,
                    None,
                    None,
                    None,
                    None,
                ],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("mo_id", StringType(), False),
                    StructField(
                        "fea_mobility_staypoint_covid_min_distance_jan_feb",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_staypoint_covid_avg_distance_jan_feb",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_staypoint_covid_min_distance_mar_apr_may",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_staypoint_covid_avg_distance_mar_apr_may",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_staypoint_covid_min_distance_jun_jul_aug",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_mobility_staypoint_covid_avg_distance_jun_jul_aug",
                        DoubleType(),
                        True,
                    ),
                ]
            ),
        )

        assert_df_frame_equal(actual_fea_df, expected_fea_df)
