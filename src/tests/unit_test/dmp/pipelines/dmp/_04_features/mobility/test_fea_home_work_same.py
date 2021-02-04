from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._04_features.mobility.nodes.fea_home_work_same import (
    mobility_home_work_same,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestFeaHomeWorkSame:
    def test_fea_home_work_same(self, spark_session: SparkSession):
        home_agg_df = spark_session.createDataFrame(
            data=[
                [
                    "imsi_1",
                    "2020-04",
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
                    "2000-01-23",
                ],
                [
                    "imsi_1",
                    "2020-05",
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
                    "2000-01-24",
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

        work_agg_df = spark_session.createDataFrame(
            data=[
                [
                    "imsi_1",
                    "2020-04",
                    33860.0,
                    "qqd5e",
                    "kelurahan_1",
                    "neighbor_kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    24,
                    33860.0,
                    1,
                    -6.389,
                    106.985,
                    1,
                    "2000-01-23",
                ],
                [
                    "imsi_1",
                    "2020-05",
                    33860.0,
                    "qqu5d",
                    "kelurahan_2",
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
                    "2000-01-24",
                ],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("mo_id", StringType(), False),
                    StructField("work1_duration_max", DoubleType(), True),
                    StructField("geohash", StringType(), False),
                    StructField("work1_kelurahan_name", StringType(), True),
                    StructField("work1_neighboring_kelurahan_name", StringType(), True),
                    StructField("work1_kecamatan_name", StringType(), True),
                    StructField("work1_kabupaten_name", StringType(), True),
                    StructField("work1_province_name", StringType(), True),
                    StructField("work1_days", LongType(), True),
                    StructField("work1_duration", DoubleType(), True),
                    StructField("village_cnt", LongType(), True),
                    StructField("work1_lat", DoubleType(), True),
                    StructField("work1_lon", DoubleType(), True),
                    StructField("duration_max_flag", IntegerType(), False),
                    StructField("month_mapped_dt", StringType(), False),
                ]
            ),
        )

        actual_fea_df = mobility_home_work_same(home_agg_df, work_agg_df)

        expected_fea_df = spark_session.createDataFrame(
            data=[["imsi_1", "2020-04", 0,], ["imsi_1", "2020-05", 1,],],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("mo_id", StringType(), False),
                    StructField(
                        "fea_mobility_home_work_same_flag", IntegerType(), True,
                    ),
                ]
            ),
        )

        assert_df_frame_equal(actual_fea_df, expected_fea_df)
