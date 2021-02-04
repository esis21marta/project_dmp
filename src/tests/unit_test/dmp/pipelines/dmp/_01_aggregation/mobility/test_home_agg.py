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

from src.dmp.pipelines.dmp._01_aggregation.mobility.nodes.home_agg import (
    mobility_home_stay_agg,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestMobilityHomeStayAgg:
    @mock.patch(
        "src.dmp.pipelines.dmp._01_aggregation.mobility.nodes.home_agg.encode",
        return_value="qqu5d",
        autospec=True,
    )
    def test_mobility_home_stay_agg(self, mock_encode, spark_session: SparkSession):

        comm_agg_df = spark_session.createDataFrame(
            data=[
                [
                    "imsi_1",
                    -6.38925,
                    106.98825,
                    1,
                    4480.0,
                    "kelurahan_1",
                    "neighbor_kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    -6.38925,
                    106.98825,
                    30,
                    4480.0,
                    "kelurahan_1",
                    "neighbor_kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    -6.38925,
                    106.98825,
                    20,
                    376.0,
                    "kelurahan_1",
                    "neighbor_kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    -6.38925,
                    106.98825,
                    30,
                    3760.0,
                    "kelurahan_1",
                    "neighbor_kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    "2020-01",
                ],
                [
                    "imsi_1",
                    -6.38925,
                    106.98825,
                    20,
                    4000.0,
                    "kelurahan_1",
                    "neighbor_kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    -6.38925,
                    106.98825,
                    30,
                    3760.0,
                    "kelurahan_1",
                    "neighbor_kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    -6.38925,
                    106.98825,
                    20,
                    3760.0,
                    "kelurahan_1",
                    "neighbor_kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    -6.38925,
                    106.98825,
                    20,
                    4480.0,
                    "kelurahan_1",
                    "neighbor_kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    "2020-01",
                ],
                [
                    "imsi_1",
                    -6.38925,
                    106.98825,
                    3,
                    25380.0,
                    "kelurahan_1",
                    "neighbor_kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    -6.38925,
                    106.98825,
                    20,
                    3760.0,
                    "kelurahan_1",
                    "neighbor_kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    -6.38925,
                    106.98825,
                    30,
                    4480.0,
                    "kelurahan_1",
                    "neighbor_kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    -6.38925,
                    106.98825,
                    30,
                    4480.0,
                    "kelurahan_1",
                    "neighbor_kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    "2020-01",
                ],
                [
                    "imsi_1",
                    -6.38925,
                    106.98825,
                    20,
                    3760.0,
                    "kelurahan_1",
                    "neighbor_kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    -6.38925,
                    106.98825,
                    30,
                    3760.0,
                    "kelurahan_1",
                    "neighbor_kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    -6.38925,
                    106.98825,
                    30,
                    3760.0,
                    "kelurahan_1",
                    "neighbor_kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    -6.38925,
                    106.98825,
                    30,
                    4480.0,
                    "kelurahan_1",
                    "neighbor_kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    "2019-06",
                ],
            ],
            schema=StructType(
                [
                    StructField("imsi", StringType(), False),
                    StructField("home1_lat", DoubleType(), True),
                    StructField("home1_lon", DoubleType(), True),
                    StructField("home1_days", LongType(), True),
                    StructField("home1_duration", DoubleType(), True),
                    StructField("home1_kelurahan_name", StringType(), True),
                    StructField("home1_neighboring_kelurahan_name", StringType(), True),
                    StructField("home1_kecamatan_name", StringType(), True),
                    StructField("home1_kabupaten_name", StringType(), True),
                    StructField("home1_province_name", StringType(), True),
                    StructField("home2_lat", DoubleType(), True),
                    StructField("home2_lon", DoubleType(), True),
                    StructField("home2_days", LongType(), True),
                    StructField("home2_duration", DoubleType(), True),
                    StructField("home2_kelurahan_name", StringType(), True),
                    StructField("home2_neighboring_kelurahan_name", StringType(), True),
                    StructField("home2_kecamatan_name", StringType(), True),
                    StructField("home2_kabupaten_name", StringType(), True),
                    StructField("home2_province_name", StringType(), True),
                    StructField("home3_lat", DoubleType(), True),
                    StructField("home3_lon", DoubleType(), True),
                    StructField("home3_days", LongType(), True),
                    StructField("home3_duration", DoubleType(), True),
                    StructField("home3_kelurahan_name", StringType(), True),
                    StructField("home3_neighboring_kelurahan_name", StringType(), True),
                    StructField("home3_kecamatan_name", StringType(), True),
                    StructField("home3_kabupaten_name", StringType(), True),
                    StructField("home3_province_name", StringType(), True),
                    StructField("home4_lat", DoubleType(), True),
                    StructField("home4_lon", DoubleType(), True),
                    StructField("home4_days", LongType(), True),
                    StructField("home4_duration", DoubleType(), True),
                    StructField("home4_kelurahan_name", StringType(), True),
                    StructField("home4_neighboring_kelurahan_name", StringType(), True),
                    StructField("home4_kecamatan_name", StringType(), True),
                    StructField("home4_kabupaten_name", StringType(), True),
                    StructField("home4_province_name", StringType(), True),
                    StructField("mo_id", StringType(), False),
                ]
            ),
        )

        month_dt_dict = {"2020-01": "2000-01-20"}

        actual_fea_df = mobility_home_stay_agg(comm_agg_df, month_dt_dict,)

        actual_fea_df.show(10, 0)

        actual_fea_df.printSchema()

        expected_fea_df = spark_session.createDataFrame(
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
                ]
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

        expected_fea_df.show(1, 0)
        expected_fea_df.printSchema()

        assert_df_frame_equal(actual_fea_df, expected_fea_df)