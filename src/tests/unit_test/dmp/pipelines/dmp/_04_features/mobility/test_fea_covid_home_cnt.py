from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._04_features.mobility.nodes_covid.fea_covid_home_cnt import (
    mobility_covid_home_cnt_features,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestFeaCovidHomeCnt:
    def test_fea_covid_home_cnt(self, spark_session: SparkSession):
        comm_agg_df = spark_session.createDataFrame(
            data=[
                [
                    "imsi_1",
                    "2020-07",
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
                    "2000-01-26",
                ],
                [
                    "imsi_2",
                    "2020-03",
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
                    "2000-01-22",
                ],
                [
                    "imsi_2",
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
                    "imsi_4",
                    "2020-06",
                    33860.0,
                    "qqu5f",
                    "kelurahan_1",
                    "neighbor_kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    24,
                    33860.0,
                    1,
                    -6.38825,
                    106.9825,
                    1,
                    "2000-01-25",
                ],
                [
                    "imsi_4",
                    "2020-05",
                    33860.0,
                    "qqu5e",
                    "kelurahan_1",
                    "neighbor_kelurahan_1",
                    "kecamatan_3",
                    "kabu_4",
                    "provinc_6",
                    24,
                    33860.0,
                    1,
                    -6.3895,
                    106.9885,
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

        actual_fea_df = mobility_covid_home_cnt_features(comm_agg_df)

        expected_fea_df = spark_session.createDataFrame(
            data=[
                ["imsi_1", "2020-07", None, None, 1, None, None, None, None, 1],
                ["imsi_2", "2020-03", 1, None, None, 1, None, None, None, None],
                ["imsi_2", "2020-04", 1, 1, None, 1, 1, None, None, None],
                ["imsi_4", "2020-05", 1, 1, 1, None, None, 1, None, None],
                ["imsi_4", "2020-06", 1, 2, 2, None, None, 1, 1, None],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("mo_id", StringType(), False),
                    StructField(
                        "fea_mobility_covid_home_cnt_mar_apr_may", IntegerType(), True
                    ),
                    StructField(
                        "fea_mobility_covid_home_cnt_apr_may_jun", IntegerType(), True
                    ),
                    StructField(
                        "fea_mobility_covid_home_cnt_may_jun_jul", IntegerType(), True
                    ),
                    StructField("fea_mobility_covid_home_cnt_mar", IntegerType(), True),
                    StructField("fea_mobility_covid_home_cnt_apr", IntegerType(), True),
                    StructField("fea_mobility_covid_home_cnt_may", IntegerType(), True),
                    StructField("fea_mobility_covid_home_cnt_jun", IntegerType(), True),
                    StructField("fea_mobility_covid_home_cnt_jul", IntegerType(), True),
                ]
            ),
        )

        assert_df_frame_equal(actual_fea_df, expected_fea_df)
