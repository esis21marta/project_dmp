import datetime

from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.dmp.pipelines.dmp._06_qa.timeliness.timeliness_score import timeliness_score
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestTimelinessScore:
    def test_check_check_timeliness_score(
        self, spark_session,
    ):

        df_master = spark_session.createDataFrame(
            data=[
                [
                    "61865676780000",  # msisdn
                    datetime.date(2020, 1, 1),  # weekstart
                    "demo",  # fea_monthly_spend
                    1.123,  # fea_int_usage_col_a
                    10,  # fea_int_usage_col_b
                    ["demo_1", "demo_2"],  # fea_rech_col_a
                ],
                [
                    "61865676780000",  # msisdn
                    datetime.date(2020, 1, 1),  # weekstart
                    "demo",  # fea_monthly_spend
                    1.123,  # fea_int_usage_col_a
                    10,  # fea_int_usage_col_b
                    ["demo_1", "demo_2"],  # fea_rech_col_a
                ],
                [
                    "61865676780000",  # msisdn
                    datetime.date(2020, 1, 1),  # weekstart
                    "demo",  # fea_monthly_spend
                    1.123,  # fea_int_usage_col_a
                    10,  # fea_int_usage_col_b
                    ["demo_1", "demo_2"],  # fea_rech_col_a
                ],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("fea_monthly_spend", StringType(), True),
                    StructField("fea_int_usage_col_a", DoubleType(), True),
                    StructField("fea_int_usage_col_b", LongType(), True),
                    StructField("fea_rech_col_a", ArrayType(StringType(), True), True),
                ]
            ),
        )

        df_timeliness = spark_session.createDataFrame(
            data=[
                [
                    "master",  # table_name
                    datetime.datetime(2020, 8, 24),  # run_time
                    datetime.datetime(2020, 8, 31, 10, 44),  # actual
                    datetime.datetime(2020, 9, 1, 0, 0),  # expected
                    0.0,  # delay
                    "master",  # layer
                    "training",  # master_mode
                ],
                [
                    "master",  # table_name
                    datetime.datetime(2020, 8, 31),  # run_time
                    datetime.datetime(2020, 9, 8, 10, 44),  # actual
                    datetime.datetime(2020, 9, 8, 0, 0),  # expected
                    10.733,  # delay
                    "master",  # layer
                    "training",  # master_mode
                ],
            ],
            schema=StructType(
                [
                    StructField("table_name", StringType(), False),
                    StructField("run_time", DateType(), False),
                    StructField("actual", TimestampType(), False),
                    StructField("expected", TimestampType(), False),
                    StructField("delay", DoubleType(), False),
                    StructField("layer", StringType(), False),
                    StructField("master_mode", StringType(), False),
                ]
            ),
        )

        actual_timeliness_score_df = timeliness_score(
            df_timeliness,
            df_master,
            feature_domain_mapping={
                "fea_monthly_spend": "revenue",
                "fea_int_usage": "internet_usage",
                "fea_rech": "recharge",
            },
        )

        expected_timeliness_score_df = spark_session.createDataFrame(
            data=[
                [
                    "master",  # table_name
                    datetime.datetime(2020, 8, 24),  # run_time
                    "master",  # layer
                    "training",  # master_mode
                    "timeliness",  # dimension
                    100.0,  # score
                    "percentage",  # unit
                    "revenue",  # domain
                ],
                [
                    "master",  # table_name
                    datetime.datetime(2020, 8, 24),  # run_time
                    "master",  # layer
                    "training",  # master_mode
                    "timeliness",  # dimension
                    100.0,  # score
                    "percentage",  # unit
                    "recharge",  # domain
                ],
                [
                    "master",  # table_name
                    datetime.datetime(2020, 8, 24),  # run_time
                    "master",  # layer
                    "training",  # master_mode
                    "timeliness",  # dimension
                    100.0,  # score
                    "percentage",  # unit
                    "internet_usage",  # domain
                ],
                [
                    "master",  # table_name
                    datetime.datetime(2020, 8, 31),  # run_time
                    "master",  # layer
                    "training",  # master_mode
                    "timeliness",  # dimension
                    93.611,  # score
                    "percentage",  # unit
                    "revenue",  # domain
                ],
                [
                    "master",  # table_name
                    datetime.datetime(2020, 8, 31),  # run_time
                    "master",  # layer
                    "training",  # master_mode
                    "timeliness",  # dimension
                    93.611,  # score
                    "percentage",  # unit
                    "recharge",  # domain
                ],
                [
                    "master",  # table_name
                    datetime.datetime(2020, 8, 31),  # run_time
                    "master",  # layer
                    "training",  # master_mode
                    "timeliness",  # dimension
                    93.611,  # score
                    "percentage",  # unit
                    "internet_usage",  # domain
                ],
            ],
            schema=StructType(
                [
                    StructField("table_name", StringType(), False),
                    StructField("run_time", DateType(), False),
                    StructField("layer", StringType(), False),
                    StructField("master_mode", StringType(), False),
                    StructField("dimension", StringType(), False),
                    StructField("score", DoubleType(), False),
                    StructField("unit", StringType(), False),
                    StructField("domain", StringType(), False),
                ]
            ),
        )

        assert_df_frame_equal(
            actual_timeliness_score_df,
            expected_timeliness_score_df,
            order_by=actual_timeliness_score_df.columns,
        )
