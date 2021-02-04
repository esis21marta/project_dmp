import datetime
from unittest import mock

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._04_features.mytsel.fea_mytsel import fea_mytsel
from src.tests.pysaprk_df_equality import assert_df_frame_equal


@mock.patch(
    "src.dmp.pipelines.dmp._04_features.mytsel.fea_mytsel.get_start_date",
    return_value=datetime.date(2018, 10, 10),
    autospec=True,
)
@mock.patch(
    "src.dmp.pipelines.dmp._04_features.mytsel.fea_mytsel.get_end_date",
    return_value=datetime.date(2020, 1, 26),
    autospec=True,
)
class TestMyTsel:
    def test_fea_mytsel(
        self, mock_get_end_date, mock_get_start_date, spark_session: SparkSession
    ):
        df_mytsel_weekly = spark_session.createDataFrame(
            data=[
                [datetime.date(2020, 1, 6), "111", 1],
                [datetime.date(2020, 1, 13), "111", None],
                [datetime.date(2020, 1, 20), "111", None],
                [datetime.date(2020, 1, 6), "222", 2],
                [datetime.date(2020, 1, 13), "222", 2],
                [datetime.date(2020, 1, 20), "222", None],
                [datetime.date(2020, 1, 6), "333", 0],
                [datetime.date(2020, 1, 13), "333", 1],
                [datetime.date(2020, 1, 20), "333", None],
                [datetime.date(2020, 1, 6), "444", None],
                [datetime.date(2020, 1, 13), "444", None],
                [datetime.date(2020, 1, 20), "444", None],
                # This weekstart should not come in output. Should be filtered
                [datetime.date(2020, 1, 27), "111", None],
                [datetime.date(2020, 1, 27), "222", None],
                [datetime.date(2020, 1, 27), "333", None],
                [datetime.date(2020, 1, 27), "444", None],
            ],
            schema=StructType(
                [
                    StructField("weekstart", DateType(), False),
                    StructField("msisdn", StringType(), False),
                    StructField("mytsel_login_count_days", LongType(), True),
                ]
            ),
        )

        config_feature = {
            "fea_mytsel_login_count_days": {
                "feature_name": "fea_mytsel_login_count_days_{period_string}",
                "periods": ["01w", "02w", "03w", "01m", "02m", "03m"],
            },
            "fea_mytsel_has_login": {
                "dependencies": ["fea_mytsel_login_count_days"],
                "feature_name": "fea_mytsel_has_login_{period_string}",
                "periods": ["01w", "02w", "03w", "01m", "02m", "03m"],
            },
        }

        actual_fea_mytsel = fea_mytsel(df_mytsel_weekly, config_feature, "all", [])
        expected_fea_mytsel = spark_session.createDataFrame(
            # fmt: off
            data = [
                ['111', datetime.date(2020, 1, 6), True, True, True, True, True, True, 1, 1, 1, 1, 1, 1],
                ['111', datetime.date(2020, 1, 13), False, True, True, True, True, True, 0, 1, 1, 1, 1, 1],
                ['111', datetime.date(2020, 1, 20), False, False, True, True, True, True, 0, 0, 1, 1, 1, 1],
                ['222', datetime.date(2020, 1, 6), True, True, True, True, True, True, 2, 2, 2, 2, 2, 2],
                ['222', datetime.date(2020, 1, 13), True, True, True, True, True, True, 2, 4, 4, 4, 4, 4],
                ['222', datetime.date(2020, 1, 20), False, True, True, True, True, True, 0, 2, 4, 4, 4, 4],
                ['333', datetime.date(2020, 1, 6), False, False, False, False, False, False, 0, 0, 0, 0, 0, 0],
                ['333', datetime.date(2020, 1, 13), True, True, True, True, True, True, 1, 1, 1, 1, 1, 1],
                ['333', datetime.date(2020, 1, 20), False, True, True, True, True, True, 0, 1, 1, 1, 1, 1],
                ['444', datetime.date(2020, 1, 6), False, False, False, False, False, False, 0, 0, 0, 0, 0, 0],
                ['444', datetime.date(2020, 1, 13), False, False, False, False, False, False, 0, 0, 0, 0, 0, 0],
                ['444', datetime.date(2020, 1, 20), False, False, False, False, False, False, 0, 0, 0, 0, 0, 0],
            ],
            # fmt: on
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("fea_mytsel_has_login_01w", BooleanType(), False),
                    StructField("fea_mytsel_has_login_02w", BooleanType(), False),
                    StructField("fea_mytsel_has_login_03w", BooleanType(), False),
                    StructField("fea_mytsel_has_login_01m", BooleanType(), False),
                    StructField("fea_mytsel_has_login_02m", BooleanType(), False),
                    StructField("fea_mytsel_has_login_03m", BooleanType(), False),
                    StructField("fea_mytsel_login_count_days_01w", LongType(), False),
                    StructField("fea_mytsel_login_count_days_02w", LongType(), False),
                    StructField("fea_mytsel_login_count_days_03w", LongType(), False),
                    StructField("fea_mytsel_login_count_days_01m", LongType(), False),
                    StructField("fea_mytsel_login_count_days_02m", LongType(), False),
                    StructField("fea_mytsel_login_count_days_03m", LongType(), False),
                ]
            )
        )

        assert_df_frame_equal(actual_fea_mytsel, expected_fea_mytsel)
