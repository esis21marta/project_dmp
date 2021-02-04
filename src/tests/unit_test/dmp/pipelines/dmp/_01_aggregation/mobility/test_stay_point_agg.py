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

from src.dmp.pipelines.dmp._01_aggregation.mobility.nodes.stay_point_agg import (
    mobility_stay_point_agg,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestMobilityHomeStayAgg:
    @mock.patch(
        "src.dmp.pipelines.dmp._01_aggregation.mobility.nodes.stay_point_agg.encode",
        return_value="qqu5d",
        autospec=True,
    )
    def test_mobility_home_stay_agg(self, mock_encode, spark_session: SparkSession):
        comm_agg_df = spark_session.createDataFrame(
            data=[
                [
                    "imsi_1",
                    "grid_1",
                    -6.38925,
                    106.98825,
                    "2020-01-01 12:30:00",
                    "2020-01-01 14:30:00",
                    "kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    "2020-01-01",
                ],
                [
                    "imsi_1",
                    "grid_1",
                    -6.38925,
                    106.98825,
                    "2020-02-05 12:30:00",
                    "2020-02-05 16:38:00",
                    "kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    "2020-02-05",
                ],
                [
                    "imsi_1",
                    "grid_1",
                    -6.38925,
                    106.98825,
                    "2020-04-15 09:30:00",
                    "2020-04-15 12:38:00",
                    "kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    "2020-04-15",
                ],
                [
                    "imsi_1",
                    "grid_1",
                    -6.38925,
                    106.98825,
                    "2020-06-25 07:30:00",
                    "2020-06-25 15:38:00",
                    "kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    "2020-06-25",
                ],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("grid_name", StringType(), False),
                    StructField("grid_lat", DoubleType(), True),
                    StructField("grid_lon", DoubleType(), True),
                    StructField("start_date", StringType(), False),
                    StructField("end_date", StringType(), False),
                    StructField("kelurahan_name", StringType(), True),
                    StructField("kecamatan_name", StringType(), True),
                    StructField("kabupaten_name", StringType(), True),
                    StructField("province_name", StringType(), True),
                    StructField("calling_date", StringType(), False),
                ]
            ),
        )

        month_dt_dict = {
            "2020-01": "2000-01-20",
            "2020-02": "2000-01-21",
            "2020-03": "2000-01-22",
            "2020-04": "2000-01-23",
            "2020-05": "2000-01-24",
            "2020-06": "2000-01-25",
        }

        actual_fea_df = mobility_stay_point_agg(comm_agg_df, month_dt_dict,)

        actual_fea_df.show(10, 0)

        actual_fea_df.printSchema()

        expected_fea_df = spark_session.createDataFrame(
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

        expected_fea_df.show(1, 0)
        expected_fea_df.printSchema()

        assert_df_frame_equal(actual_fea_df, expected_fea_df)
