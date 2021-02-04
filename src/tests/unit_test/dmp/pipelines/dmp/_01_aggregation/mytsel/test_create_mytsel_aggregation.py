import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType

from src.dmp.pipelines.dmp._01_aggregation.mytsel.create_mytsel_aggregation import (
    _create_mytsel_weekly_aggregation,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestMyTselAggregation:
    def test_create_mytsel_weekly_aggregation(self, spark_session: SparkSession):
        df_mytsel_daily_user = spark_session.createDataFrame(
            data=[
                [datetime.date(2020, 1, 1), "111"],
                [datetime.date(2020, 1, 1), "222"],
                [datetime.date(2020, 1, 1), "333"],
                [datetime.date(2020, 1, 2), "111"],
                [datetime.date(2020, 1, 2), "333"],
                [datetime.date(2020, 1, 3), "333"],
            ],
            schema=StructType(
                [
                    StructField("trx_date", DateType(), True),
                    StructField("msisdn", StringType(), True),
                ]
            ),
        )

        actual_mytsel_weekly = _create_mytsel_weekly_aggregation(
            df_mytsel_daily_user=df_mytsel_daily_user
        )
        expected_mytsel_weekly = spark_session.createDataFrame(
            data=[
                ["111", datetime.date(2020, 1, 6), 2],
                ["222", datetime.date(2020, 1, 6), 1],
                ["333", datetime.date(2020, 1, 6), 3],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("mytsel_login_count_days", LongType(), False),
                ]
            ),
        )

        assert_df_frame_equal(actual_mytsel_weekly, expected_mytsel_weekly)
