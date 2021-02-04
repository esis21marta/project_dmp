import datetime
from unittest import mock

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._04_features.recharge.fea_recharge import fea_recharge
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestFeatureTopupBehaviour:
    config_feature = yaml.load(open("conf/dmp/training/parameters_feature.yml"))[
        "config_feature"
    ]

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.recharge.fea_recharge.get_start_date",
        return_value=datetime.date(2018, 10, 10),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.recharge.fea_recharge.get_end_date",
        return_value=datetime.date(2020, 10, 10),
        autospec=True,
    )
    def test_fea_recharge(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        # Case A) Sanity test for df_topup_behav output
        df_rech_weekly = spark_session.createDataFrame(
            data=[
                ["111", datetime.date(2020, 7, 6), 100, 1],
                ["111", datetime.date(2020, 7, 13), 200, 2],
                ["222", datetime.date(2020, 7, 6), 300, 3],
                ["222", datetime.date(2020, 7, 13), 400, 4],
                ["222", datetime.date(2020, 7, 20), 500, 5],
                ["222", datetime.date(2020, 7, 27), None, 1],
                ["222", datetime.date(2020, 11, 2), None, 5],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("tot_amt", LongType(), True),
                    StructField("tot_trx", LongType(), True),
                ]
            ),
        )

        df_digi_rech_weekly = spark_session.createDataFrame(
            data=[
                ["111", datetime.date(2020, 7, 6), 100],
                ["111", datetime.date(2020, 7, 13), 200],
                ["222", datetime.date(2020, 7, 6), 300],
                ["222", datetime.date(2020, 7, 13), 400],
                ["222", datetime.date(2020, 7, 20), 500],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("tot_amt_digi", LongType(), True),
                ]
            ),
        )

        # Case B) Sanity test for df_no_bal output
        df_acc_bal_weekly = spark_session.createDataFrame(
            data=[
                [
                    "111",
                    datetime.date(2020, 7, 6),
                    1,
                    4.00,
                    4.000000,
                    0.0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    4.00,
                    None,
                    None,
                    None,
                    None,
                    "2020-07-05",
                    "2020-07-05",
                    "2020-07-05",
                    "2020-07-05",
                    None,
                ],
                [
                    "111",
                    datetime.date(2020, 7, 13),
                    2,
                    38000.00,
                    38000.000000,
                    0.0,
                    10,
                    7,
                    7,
                    7,
                    0,
                    7,
                    7,
                    7,
                    7,
                    38000.00,
                    "2020-07-10",
                    "2020-07-10",
                    "2020-07-10",
                    None,
                    None,
                    None,
                    None,
                    "2020-07-10",
                    None,
                ],
                [
                    "222",
                    datetime.date(2020, 7, 6),
                    3,
                    1450.00,
                    1450.000000,
                    0.0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    1450.00,
                    None,
                    None,
                    None,
                    None,
                    "2020-07-05",
                    "2020-07-05",
                    "2020-07-05",
                    "2020-07-05",
                    None,
                ],
                [
                    "222",
                    datetime.date(2020, 7, 13),
                    4,
                    6752.00,
                    12940.571429,
                    4186.404139479085,
                    5,
                    7,
                    5,
                    0,
                    0,
                    0,
                    0,
                    5,
                    5,
                    14182.00,
                    "2020-07-10",
                    "2020-07-10",
                    None,
                    None,
                    None,
                    "2020-07-07",
                    "2020-07-10",
                    "2020-07-10",
                    None,
                ],
                [
                    "222",
                    datetime.date(2020, 7, 20),
                    5,
                    10777.00,
                    11491.285714,
                    1889.822365046136,
                    5,
                    7,
                    7,
                    0,
                    0,
                    0,
                    0,
                    5,
                    5,
                    10777.00,
                    "2020-07-19",
                    "2020-07-19",
                    None,
                    None,
                    None,
                    None,
                    "2020-07-19",
                    "2020-07-19",
                    10777.000000,
                ],
                [
                    "222",
                    datetime.date(2020, 7, 27),
                    4,
                    10777.00,
                    11491.285714,
                    1889.822365046136,
                    5,
                    7,
                    7,
                    0,
                    0,
                    0,
                    0,
                    5,
                    3,
                    10777.00,
                    "2020-07-26",
                    "2020-07-26",
                    None,
                    None,
                    None,
                    None,
                    "2020-07-26",
                    "2020-07-26",
                    10777.000000,
                ],
                [
                    "222",
                    datetime.date(2020, 11, 2),
                    5,
                    6752.00,
                    12940.571429,
                    4186.404139479085,
                    5,
                    7,
                    5,
                    0,
                    0,
                    0,
                    0,
                    5,
                    7,
                    14182.00,
                    "2020-11-01",
                    "2020-11-01",
                    None,
                    None,
                    None,
                    "2020-10-27",
                    "2020-11-01",
                    "2020-11-01",
                    None,
                ],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("zero_or_neg_count", LongType(), True),
                    StructField("account_balance_min", DoubleType(), True),
                    StructField("account_balance_avg", DoubleType(), True),
                    StructField("account_balance_stddev", DoubleType(), True),
                    StructField("account_balance_below_500_count", LongType(), True),
                    StructField("num_days_bal_more_than_5000", LongType(), True),
                    StructField("num_days_bal_more_than_10000", LongType(), True),
                    StructField("num_days_bal_more_than_20000", LongType(), True),
                    StructField("num_days_bal_more_than_50000", LongType(), True),
                    StructField("num_days_bal_below_500", LongType(), True),
                    StructField(
                        "num_days_bal_more_than_20000_below_40000", LongType(), True
                    ),
                    StructField(
                        "num_days_bal_more_than_40000_below_60000", LongType(), True
                    ),
                    StructField(
                        "num_days_bal_more_than_20000_below_60000", LongType(), True
                    ),
                    StructField("current_balance", DoubleType(), True),
                    StructField("max_date_with_bal_more_than_5000", StringType(), True),
                    StructField(
                        "max_date_with_bal_more_than_10000", StringType(), True
                    ),
                    StructField(
                        "max_date_with_bal_more_than_20000", StringType(), True
                    ),
                    StructField(
                        "max_date_with_bal_more_than_50000", StringType(), True
                    ),
                    StructField("max_date_with_bal_5000_or_less", StringType(), True),
                    StructField("max_date_with_bal_10000_or_less", StringType(), True),
                    StructField("max_date_with_bal_20000_or_less", StringType(), True),
                    StructField("max_date_with_bal_50000_or_less", StringType(), True),
                    StructField("avg_last_bal_before_rech", DoubleType(), True),
                ]
            ),
        )

        # Case C) Sanity test for df_fea_pkg_prchse
        df_chg_pkg_prchse_weekly = spark_session.createDataFrame(
            data=[
                ["111", datetime.date(2020, 7, 6), 1, 1000],
                ["111", datetime.date(2020, 7, 13), 2, 2000],
                ["222", datetime.date(2020, 7, 6), 3, 3000],
                ["222", datetime.date(2020, 7, 13), 4, 4000],
                ["222", datetime.date(2020, 7, 20), 5, 5000],
                ["222", datetime.date(2020, 7, 27), 4, 4000],
                ["222", datetime.date(2020, 11, 2), 5, 5000],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("trx_pkg_prchse", LongType(), True),
                    StructField("rev_pkg_prchse", LongType(), True),
                ]
            ),
        )

        actual_fea_df = fea_recharge(
            df_rech_weekly,
            df_digi_rech_weekly,
            df_acc_bal_weekly,
            df_chg_pkg_prchse_weekly,
            self.config_feature,
            feature_mode="all",
            required_output_features=[],
        )
        actual_fea_df = actual_fea_df.select(
            "msisdn",
            "weekstart",
            "fea_rech_tot_amt_sum_01w",
            "fea_rech_tot_amt_sum_03m",
            "fea_rech_tot_trx_sum_01w",
            "fea_rech_tot_amt_stddev_03m",
            "fea_rech_tot_amt_sum_03w",
            "fea_rech_tot_amt_avg_02m",
            "fea_rech_tot_trx_trd_03m",
            "fea_rech_tot_trx_sum_01m",
            "fea_rech_tot_amt_sum_01m",
            "fea_rech_tot_amt_trd_03m",
            "fea_rech_tot_trx_sum_02w",
            "fea_rech_tot_trx_sum_03m",
            "fea_rech_tot_amt_avg_01w",
            "fea_rech_tot_amt_avg_03m",
            "fea_rech_tot_amt_sum_02m",
            "fea_rech_tot_trx_sum_03w",
            "fea_rech_tot_amt_sum_02w",
            "fea_rech_tot_amt_avg_03w",
            "fea_rech_tot_trx_sum_02m",
            "fea_rech_tot_amt_avg_01m",
            "fea_rech_tot_amt_avg_02w",
            "fea_rech_tot_days_bal_more_than_20000_03m",
            "fea_rech_tot_days_no_bal_avg_02m",
            "fea_rech_tot_days_bal_more_than_50000_01w",
            "fea_rech_num_days_past_since_bal_5000_or_less_03w",
            "fea_rech_tot_days_bal_more_than_50000_01m",
            "fea_rech_last_bal_before_rech_avg_03m",
            "fea_rech_bal_monthly_avg_03m",
            "fea_rech_tot_days_no_bal_avg_03m",
            "fea_rech_last_bal_before_rech_avg_03w",
            "fea_rech_tot_days_bal_more_than_5000_02m",
            "fea_rech_num_days_past_since_bal_more_than_20000_02w",
            "fea_rech_num_days_past_since_bal_50000_or_less_03m",
            "fea_rech_tot_days_bal_more_than_5000_03m",
            "fea_rech_bal_monthly_avg_02m",
            "fea_rech_last_bal_before_rech_avg_01m",
            "fea_rech_tot_days_bal_more_than_10000_02w",
            "fea_rech_num_days_past_since_bal_more_than_50000_03m",
            "fea_rech_num_days_past_since_bal_more_than_10000_02m",
            "fea_rech_num_days_past_since_bal_more_than_50000_02w",
            "fea_rech_num_days_past_since_bal_5000_or_less_01m",
            "fea_rech_num_days_past_since_bal_20000_or_less_02w",
            "fea_rech_num_days_past_since_bal_50000_or_less_03w",
            "fea_rech_bal_weekly_avg_01w",
            "fea_rech_num_days_past_since_bal_more_than_20000_01m",
            "fea_rech_num_days_past_since_bal_10000_or_less_01w",
            "fea_rech_tot_days_bal_more_than_10000_01w",
            "fea_rech_num_days_past_since_bal_20000_or_less_03w",
            "fea_rech_tot_days_bal_more_than_5000_02w",
            "fea_rech_num_days_past_since_bal_more_than_20000_02m",
            "fea_rech_num_days_past_since_bal_10000_or_less_02m",
            "fea_rech_last_bal_before_rech_avg_01w",
            "fea_rech_bal_min_02w",
            "fea_rech_num_days_past_since_bal_5000_or_less_02w",
            "fea_rech_num_days_past_since_bal_more_than_20000_03m",
            "fea_rech_num_days_past_since_bal_more_than_20000_01w",
            "fea_rech_tot_days_bal_more_than_20000_02w",
            "fea_rech_num_days_past_since_bal_10000_or_less_01m",
            "fea_rech_tot_days_bal_more_than_50000_02m",
            "fea_rech_last_bal_before_rech_avg_02m",
            "fea_rech_tot_days_bal_more_than_5000_01m",
            "fea_rech_num_days_past_since_bal_more_than_5000_02w",
            "fea_rech_num_days_past_since_bal_more_than_50000_03w",
            "fea_rech_num_days_past_since_bal_more_than_5000_01m",
            "fea_rech_num_days_past_since_bal_50000_or_less_02m",
            "fea_rech_tot_days_bal_more_than_20000_01m",
            "fea_rech_tot_days_no_bal_sum_02m",
            "fea_rech_num_days_past_since_bal_5000_or_less_01w",
            "fea_rech_num_days_past_since_bal_more_than_50000_01w",
            "fea_rech_bal_min_02m",
            "fea_rech_num_days_past_since_bal_more_than_20000_03w",
            "fea_rech_num_days_past_since_bal_20000_or_less_03m",
            "fea_rech_bal_weekly_avg_03m",
            "fea_rech_num_days_past_since_bal_10000_or_less_03m",
            "fea_rech_tot_days_bal_more_than_10000_03w",
            "fea_rech_bal_weekly_avg_02w",
            "fea_rech_tot_days_bal_more_than_50000_02w",
            "fea_rech_num_days_past_since_bal_more_than_10000_02w",
            "fea_rech_num_days_past_since_bal_more_than_5000_01w",
            "fea_rech_tot_days_bal_more_than_10000_01m",
            "fea_rech_num_days_past_since_bal_20000_or_less_02m",
            "fea_rech_tot_days_bal_more_than_20000_01w",
            "fea_rech_tot_days_bal_more_than_20000_02m",
            "fea_rech_num_days_past_since_bal_20000_or_less_01w",
            "fea_rech_num_days_past_since_bal_more_than_5000_02m",
            "fea_rech_bal_weekly_avg_02m",
            "fea_rech_num_days_past_since_bal_50000_or_less_02w",
            "fea_rech_num_days_past_since_bal_10000_or_less_03w",
            "fea_rech_tot_days_bal_more_than_10000_02m",
            "fea_rech_tot_days_no_bal_sum_01m",
            "fea_rech_bal_min_03w",
            "fea_rech_tot_days_no_bal_sum_02w",
            "fea_rech_num_days_past_since_bal_10000_or_less_02w",
            "fea_rech_current_balance",
            "fea_rech_num_days_past_since_bal_more_than_5000_03w",
            "fea_rech_bal_monthly_avg_01m",
            "fea_rech_last_bal_before_rech_avg_02w",
            "fea_rech_bal_min_01w",
            "fea_rech_num_days_past_since_bal_more_than_50000_02m",
            "fea_rech_tot_days_no_bal_sum_01w",
            "fea_rech_num_days_past_since_bal_50000_or_less_01w",
            "fea_rech_tot_days_bal_more_than_5000_01w",
            "fea_rech_num_days_past_since_bal_more_than_10000_03w",
            "fea_rech_tot_days_bal_more_than_50000_03w",
            "fea_rech_tot_days_bal_more_than_5000_03w",
            "fea_rech_num_days_past_since_bal_more_than_10000_03m",
            "fea_rech_num_days_past_since_bal_more_than_50000_01m",
            "fea_rech_num_days_past_since_bal_more_than_5000_03m",
            "fea_rech_bal_weekly_avg_03w",
            "fea_rech_num_days_past_since_bal_20000_or_less_01m",
            "fea_rech_num_days_past_since_bal_5000_or_less_03m",
            "fea_rech_num_days_past_since_bal_more_than_10000_01w",
            "fea_rech_num_days_past_since_bal_50000_or_less_01m",
            "fea_rech_num_days_past_since_bal_more_than_10000_01m",
            "fea_rech_tot_days_bal_more_than_50000_03m",
            "fea_rech_bal_min_03m",
            "fea_rech_tot_days_no_bal_sum_03m",
            "fea_rech_bal_min_01m",
            "fea_rech_tot_days_bal_more_than_20000_03w",
            "fea_rech_tot_days_bal_more_than_10000_03m",
            "fea_rech_bal_weekly_avg_01m",
            "fea_rech_num_days_past_since_bal_5000_or_less_02m",
            "fea_rech_tot_days_no_bal_sum_03w",
            "fea_rech_trx_pkg_prchse_sum_02m",
            "fea_rech_trx_pkg_prchse_sum_01m",
            "fea_rech_rev_pkg_prchse_sum_03m",
            "fea_rech_rev_pkg_prchse_sum_01m",
            "fea_rech_rev_pkg_prchse_sum_02w",
            "fea_rech_trx_pkg_prchse_stddev_03m",
            "fea_rech_rev_pkg_prchse_sum_03w",
            "fea_rech_trx_pkg_prchse_sum_03m",
            "fea_rech_rev_pkg_prchse_sum_01w",
            "fea_rech_rev_pkg_prchse_sum_02m",
            "fea_rech_rev_pkg_prchse_stddev_03m",
            "fea_rech_trx_pkg_prchse_avg_02m",
            "fea_rech_rev_pkg_prchse_avg_02m",
            "fea_rech_trx_pkg_prchse_avg_03m",
            "fea_rech_trx_pkg_prchse_sum_03w",
            "fea_rech_trx_pkg_prchse_sum_02w",
            "fea_rech_annualize_factor",
            "fea_rech_rev_pkg_prchse_avg_03m",
            "fea_rech_trx_pkg_prchse_sum_01w",
            "fea_rech_weeks_since_last_recharge",
            "fea_rech_bal_weekly_below_500_01m",
            "fea_rech_tot_days_bal_below_500_01m",
            "fea_rech_tot_days_bal_more_than_20000_below_40000_01m",
            "fea_rech_tot_days_bal_more_than_40000_below_60000_01m",
            "fea_rech_tot_days_bal_more_than_20000_below_60000_01m",
        )
        expected_rows = [
            [
                "111",  # msisdn
                datetime.date(2020, 7, 6),  # weekstart
                100,  # fea_rech_tot_amt_sum_01w
                100,  # fea_rech_tot_amt_sum_03m
                1,  # fea_rech_tot_trx_sum_01w
                float("nan"),  # fea_rech_tot_amt_stddev_03m
                100,  # fea_rech_tot_amt_sum_03w
                100.0,  # fea_rech_tot_amt_avg_02m
                1.0,  # fea_rech_tot_trx_trd_03m
                1,  # fea_rech_tot_trx_sum_01m
                100,  # fea_rech_tot_amt_sum_01m
                1.0,  # fea_rech_tot_amt_trd_03m
                1,  # fea_rech_tot_trx_sum_02w
                1,  # fea_rech_tot_trx_sum_03m
                100.0,  # fea_rech_tot_amt_avg_01w
                100.0,  # fea_rech_tot_amt_avg_03m
                100,  # fea_rech_tot_amt_sum_02m
                1,  # fea_rech_tot_trx_sum_03w
                100,  # fea_rech_tot_amt_sum_02w
                100.0,  # fea_rech_tot_amt_avg_03w
                1,  # fea_rech_tot_trx_sum_02m
                100.0,  # fea_rech_tot_amt_avg_01m
                100.0,  # fea_rech_tot_amt_avg_02w
                0,  # fea_rech_tot_days_bal_more_than_20000_03m
                0.5,  # fea_rech_tot_days_no_bal_avg_02m
                0,  # fea_rech_tot_days_bal_more_than_50000_01w
                1,  # fea_rech_num_days_past_since_bal_5000_or_less_03w
                0,  # fea_rech_tot_days_bal_more_than_50000_01m
                None,  # fea_rech_last_bal_before_rech_avg_03m
                1.3333333333333333,  # fea_rech_bal_monthly_avg_03m
                0.3333333333333333,  # fea_rech_tot_days_no_bal_avg_03m
                None,  # fea_rech_last_bal_before_rech_avg_03w
                0,  # fea_rech_tot_days_bal_more_than_5000_02m
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_02w
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_03m
                0,  # fea_rech_tot_days_bal_more_than_5000_03m
                2.0,  # fea_rech_bal_monthly_avg_02m
                None,  # fea_rech_last_bal_before_rech_avg_01m
                0,  # fea_rech_tot_days_bal_more_than_10000_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_03m
                None,  # fea_rech_num_days_past_since_bal_more_than_10000_02m
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_02w
                1,  # fea_rech_num_days_past_since_bal_5000_or_less_01m
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_02w
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_03w
                4.0,  # fea_rech_bal_weekly_avg_01w
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_01m
                1,  # fea_rech_num_days_past_since_bal_10000_or_less_01w
                0,  # fea_rech_tot_days_bal_more_than_10000_01w
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_03w
                0,  # fea_rech_tot_days_bal_more_than_5000_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_02m
                1,  # fea_rech_num_days_past_since_bal_10000_or_less_02m
                None,  # fea_rech_last_bal_before_rech_avg_01w
                4.0,  # fea_rech_bal_min_02w
                1,  # fea_rech_num_days_past_since_bal_5000_or_less_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_03m
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_01w
                0,  # fea_rech_tot_days_bal_more_than_20000_02w
                1,  # fea_rech_num_days_past_since_bal_10000_or_less_01m
                0,  # fea_rech_tot_days_bal_more_than_50000_02m
                None,  # fea_rech_last_bal_before_rech_avg_02m
                0,  # fea_rech_tot_days_bal_more_than_5000_01m
                None,  # fea_rech_num_days_past_since_bal_more_than_5000_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_03w
                None,  # fea_rech_num_days_past_since_bal_more_than_5000_01m
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_02m
                0,  # fea_rech_tot_days_bal_more_than_20000_01m
                1,  # fea_rech_tot_days_no_bal_sum_02m
                1,  # fea_rech_num_days_past_since_bal_5000_or_less_01w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_01w
                4.0,  # fea_rech_bal_min_02m
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_03w
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_03m
                4.0,  # fea_rech_bal_weekly_avg_03m
                1,  # fea_rech_num_days_past_since_bal_10000_or_less_03m
                0,  # fea_rech_tot_days_bal_more_than_10000_03w
                4.0,  # fea_rech_bal_weekly_avg_02w
                0,  # fea_rech_tot_days_bal_more_than_50000_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_10000_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_5000_01w
                0,  # fea_rech_tot_days_bal_more_than_10000_01m
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_02m
                0,  # fea_rech_tot_days_bal_more_than_20000_01w
                0,  # fea_rech_tot_days_bal_more_than_20000_02m
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_01w
                None,  # fea_rech_num_days_past_since_bal_more_than_5000_02m
                4.0,  # fea_rech_bal_weekly_avg_02m
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_02w
                1,  # fea_rech_num_days_past_since_bal_10000_or_less_03w
                0,  # fea_rech_tot_days_bal_more_than_10000_02m
                1,  # fea_rech_tot_days_no_bal_sum_01m
                4.0,  # fea_rech_bal_min_03w
                1,  # fea_rech_tot_days_no_bal_sum_02w
                1,  # fea_rech_num_days_past_since_bal_10000_or_less_02w
                4.0,  # fea_rech_current_balance
                None,  # fea_rech_num_days_past_since_bal_more_than_5000_03w
                4.0,  # fea_rech_bal_monthly_avg_01m
                None,  # fea_rech_last_bal_before_rech_avg_02w
                4.0,  # fea_rech_bal_min_01w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_02m
                1,  # fea_rech_tot_days_no_bal_sum_01w
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_01w
                0,  # fea_rech_tot_days_bal_more_than_5000_01w
                None,  # fea_rech_num_days_past_since_bal_more_than_10000_03w
                0,  # fea_rech_tot_days_bal_more_than_50000_03w
                0,  # fea_rech_tot_days_bal_more_than_5000_03w
                None,  # fea_rech_num_days_past_since_bal_more_than_10000_03m
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_01m
                None,  # fea_rech_num_days_past_since_bal_more_than_5000_03m
                4.0,  # fea_rech_bal_weekly_avg_03w
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_01m
                1,  # fea_rech_num_days_past_since_bal_5000_or_less_03m
                None,  # fea_rech_num_days_past_since_bal_more_than_10000_01w
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_01m
                None,  # fea_rech_num_days_past_since_bal_more_than_10000_01m
                0,  # fea_rech_tot_days_bal_more_than_50000_03m
                4.0,  # fea_rech_bal_min_03m
                1,  # fea_rech_tot_days_no_bal_sum_03m
                4.0,  # fea_rech_bal_min_01m
                0,  # fea_rech_tot_days_bal_more_than_20000_03w
                0,  # fea_rech_tot_days_bal_more_than_10000_03m
                4.0,  # fea_rech_bal_weekly_avg_01m
                1,  # fea_rech_num_days_past_since_bal_5000_or_less_02m
                1,  # fea_rech_tot_days_no_bal_sum_03w
                1,  # fea_rech_trx_pkg_prchse_sum_02m
                1,  # fea_rech_trx_pkg_prchse_sum_01m
                1000,  # fea_rech_rev_pkg_prchse_sum_03m
                1000,  # fea_rech_rev_pkg_prchse_sum_01m
                1000,  # fea_rech_rev_pkg_prchse_sum_02w
                float("nan"),  # fea_rech_trx_pkg_prchse_stddev_03m
                1000,  # fea_rech_rev_pkg_prchse_sum_03w
                1,  # fea_rech_trx_pkg_prchse_sum_03m
                1000,  # fea_rech_rev_pkg_prchse_sum_01w
                1000,  # fea_rech_rev_pkg_prchse_sum_02m
                float("nan"),  # fea_rech_rev_pkg_prchse_stddev_03m
                0.125,  # fea_rech_trx_pkg_prchse_avg_02m
                125.0,  # fea_rech_rev_pkg_prchse_avg_02m
                0.07692307692307693,  # fea_rech_trx_pkg_prchse_avg_03m
                1,  # fea_rech_trx_pkg_prchse_sum_03w
                1,  # fea_rech_trx_pkg_prchse_sum_02w
                7.211102550927978,  # fea_rech_annualize_factor
                76.92307692307692,  # fea_rech_rev_pkg_prchse_avg_03m
                1,  # fea_rech_trx_pkg_prchse_sum_01w
                0,  # fea_rech_weeks_since_last_recharge
                0,  # fea_rech_bal_weekly_below_500_01m
                0,  # fea_rech_tot_days_bal_below_500_01m
                0,  # fea_rech_tot_days_bal_more_than_20000_below_40000_01m
                0,  # fea_rech_tot_days_bal_more_than_40000_below_60000_01m
                0,  # fea_rech_tot_days_bal_more_than_20000_below_60000_01m
            ],
            [
                "111",  # msisdn
                datetime.date(2020, 7, 13),  # weekstart
                200,  # fea_rech_tot_amt_sum_01w
                300,  # fea_rech_tot_amt_sum_03m
                2,  # fea_rech_tot_trx_sum_01w
                0.0,  # fea_rech_tot_amt_stddev_03m
                300,  # fea_rech_tot_amt_sum_03w
                100.0,  # fea_rech_tot_amt_avg_02m
                1.0,  # fea_rech_tot_trx_trd_03m
                3,  # fea_rech_tot_trx_sum_01m
                300,  # fea_rech_tot_amt_sum_01m
                1.0,  # fea_rech_tot_amt_trd_03m
                3,  # fea_rech_tot_trx_sum_02w
                3,  # fea_rech_tot_trx_sum_03m
                100.0,  # fea_rech_tot_amt_avg_01w
                100.0,  # fea_rech_tot_amt_avg_03m
                300,  # fea_rech_tot_amt_sum_02m
                3,  # fea_rech_tot_trx_sum_03w
                300,  # fea_rech_tot_amt_sum_02w
                100.0,  # fea_rech_tot_amt_avg_03w
                3,  # fea_rech_tot_trx_sum_02m
                100.0,  # fea_rech_tot_amt_avg_01m
                100.0,  # fea_rech_tot_amt_avg_02w
                7,  # fea_rech_tot_days_bal_more_than_20000_03m
                1.5,  # fea_rech_tot_days_no_bal_avg_02m
                0,  # fea_rech_tot_days_bal_more_than_50000_01w
                8,  # fea_rech_num_days_past_since_bal_5000_or_less_03w
                0,  # fea_rech_tot_days_bal_more_than_50000_01m
                None,  # fea_rech_last_bal_before_rech_avg_03m
                12668.0,  # fea_rech_bal_monthly_avg_03m
                1.0,  # fea_rech_tot_days_no_bal_avg_03m
                None,  # fea_rech_last_bal_before_rech_avg_03w
                7,  # fea_rech_tot_days_bal_more_than_5000_02m
                3,  # fea_rech_num_days_past_since_bal_more_than_20000_02w
                3,  # fea_rech_num_days_past_since_bal_50000_or_less_03m
                7,  # fea_rech_tot_days_bal_more_than_5000_03m
                19002.0,  # fea_rech_bal_monthly_avg_02m
                None,  # fea_rech_last_bal_before_rech_avg_01m
                7,  # fea_rech_tot_days_bal_more_than_10000_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_03m
                3,  # fea_rech_num_days_past_since_bal_more_than_10000_02m
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_02w
                8,  # fea_rech_num_days_past_since_bal_5000_or_less_01m
                8,  # fea_rech_num_days_past_since_bal_20000_or_less_02w
                3,  # fea_rech_num_days_past_since_bal_50000_or_less_03w
                38000.0,  # fea_rech_bal_weekly_avg_01w
                3,  # fea_rech_num_days_past_since_bal_more_than_20000_01m
                None,  # fea_rech_num_days_past_since_bal_10000_or_less_01w
                7,  # fea_rech_tot_days_bal_more_than_10000_01w
                8,  # fea_rech_num_days_past_since_bal_20000_or_less_03w
                7,  # fea_rech_tot_days_bal_more_than_5000_02w
                3,  # fea_rech_num_days_past_since_bal_more_than_20000_02m
                8,  # fea_rech_num_days_past_since_bal_10000_or_less_02m
                None,  # fea_rech_last_bal_before_rech_avg_01w
                4.0,  # fea_rech_bal_min_02w
                8,  # fea_rech_num_days_past_since_bal_5000_or_less_02w
                3,  # fea_rech_num_days_past_since_bal_more_than_20000_03m
                3,  # fea_rech_num_days_past_since_bal_more_than_20000_01w
                7,  # fea_rech_tot_days_bal_more_than_20000_02w
                8,  # fea_rech_num_days_past_since_bal_10000_or_less_01m
                0,  # fea_rech_tot_days_bal_more_than_50000_02m
                None,  # fea_rech_last_bal_before_rech_avg_02m
                7,  # fea_rech_tot_days_bal_more_than_5000_01m
                3,  # fea_rech_num_days_past_since_bal_more_than_5000_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_03w
                3,  # fea_rech_num_days_past_since_bal_more_than_5000_01m
                3,  # fea_rech_num_days_past_since_bal_50000_or_less_02m
                7,  # fea_rech_tot_days_bal_more_than_20000_01m
                3,  # fea_rech_tot_days_no_bal_sum_02m
                None,  # fea_rech_num_days_past_since_bal_5000_or_less_01w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_01w
                4.0,  # fea_rech_bal_min_02m
                3,  # fea_rech_num_days_past_since_bal_more_than_20000_03w
                8,  # fea_rech_num_days_past_since_bal_20000_or_less_03m
                19002.0,  # fea_rech_bal_weekly_avg_03m
                8,  # fea_rech_num_days_past_since_bal_10000_or_less_03m
                7,  # fea_rech_tot_days_bal_more_than_10000_03w
                19002.0,  # fea_rech_bal_weekly_avg_02w
                0,  # fea_rech_tot_days_bal_more_than_50000_02w
                3,  # fea_rech_num_days_past_since_bal_more_than_10000_02w
                3,  # fea_rech_num_days_past_since_bal_more_than_5000_01w
                7,  # fea_rech_tot_days_bal_more_than_10000_01m
                8,  # fea_rech_num_days_past_since_bal_20000_or_less_02m
                7,  # fea_rech_tot_days_bal_more_than_20000_01w
                7,  # fea_rech_tot_days_bal_more_than_20000_02m
                None,  # fea_rech_num_days_past_since_bal_20000_or_less_01w
                3,  # fea_rech_num_days_past_since_bal_more_than_5000_02m
                19002.0,  # fea_rech_bal_weekly_avg_02m
                3,  # fea_rech_num_days_past_since_bal_50000_or_less_02w
                8,  # fea_rech_num_days_past_since_bal_10000_or_less_03w
                7,  # fea_rech_tot_days_bal_more_than_10000_02m
                3,  # fea_rech_tot_days_no_bal_sum_01m
                4.0,  # fea_rech_bal_min_03w
                3,  # fea_rech_tot_days_no_bal_sum_02w
                8,  # fea_rech_num_days_past_since_bal_10000_or_less_02w
                38000.0,  # fea_rech_current_balance
                3,  # fea_rech_num_days_past_since_bal_more_than_5000_03w
                38004.0,  # fea_rech_bal_monthly_avg_01m
                None,  # fea_rech_last_bal_before_rech_avg_02w
                38000.0,  # fea_rech_bal_min_01w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_02m
                2,  # fea_rech_tot_days_no_bal_sum_01w
                3,  # fea_rech_num_days_past_since_bal_50000_or_less_01w
                7,  # fea_rech_tot_days_bal_more_than_5000_01w
                3,  # fea_rech_num_days_past_since_bal_more_than_10000_03w
                0,  # fea_rech_tot_days_bal_more_than_50000_03w
                7,  # fea_rech_tot_days_bal_more_than_5000_03w
                3,  # fea_rech_num_days_past_since_bal_more_than_10000_03m
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_01m
                3,  # fea_rech_num_days_past_since_bal_more_than_5000_03m
                19002.0,  # fea_rech_bal_weekly_avg_03w
                8,  # fea_rech_num_days_past_since_bal_20000_or_less_01m
                8,  # fea_rech_num_days_past_since_bal_5000_or_less_03m
                3,  # fea_rech_num_days_past_since_bal_more_than_10000_01w
                3,  # fea_rech_num_days_past_since_bal_50000_or_less_01m
                3,  # fea_rech_num_days_past_since_bal_more_than_10000_01m
                0,  # fea_rech_tot_days_bal_more_than_50000_03m
                4.0,  # fea_rech_bal_min_03m
                3,  # fea_rech_tot_days_no_bal_sum_03m
                4.0,  # fea_rech_bal_min_01m
                7,  # fea_rech_tot_days_bal_more_than_20000_03w
                7,  # fea_rech_tot_days_bal_more_than_10000_03m
                19002.0,  # fea_rech_bal_weekly_avg_01m
                8,  # fea_rech_num_days_past_since_bal_5000_or_less_02m
                3,  # fea_rech_tot_days_no_bal_sum_03w
                3,  # fea_rech_trx_pkg_prchse_sum_02m
                3,  # fea_rech_trx_pkg_prchse_sum_01m
                3000,  # fea_rech_rev_pkg_prchse_sum_03m
                3000,  # fea_rech_rev_pkg_prchse_sum_01m
                3000,  # fea_rech_rev_pkg_prchse_sum_02w
                5.099019513592785,  # fea_rech_trx_pkg_prchse_stddev_03m
                3000,  # fea_rech_rev_pkg_prchse_sum_03w
                3,  # fea_rech_trx_pkg_prchse_sum_03m
                2000,  # fea_rech_rev_pkg_prchse_sum_01w
                3000,  # fea_rech_rev_pkg_prchse_sum_02m
                5099.019513592785,  # fea_rech_rev_pkg_prchse_stddev_03m
                0.375,  # fea_rech_trx_pkg_prchse_avg_02m
                375.0,  # fea_rech_rev_pkg_prchse_avg_02m
                0.23076923076923078,  # fea_rech_trx_pkg_prchse_avg_03m
                3,  # fea_rech_trx_pkg_prchse_sum_03w
                3,  # fea_rech_trx_pkg_prchse_sum_02w
                7.211102550927978,  # fea_rech_annualize_factor
                230.76923076923077,  # fea_rech_rev_pkg_prchse_avg_03m
                2,  # fea_rech_trx_pkg_prchse_sum_01w
                0,  # fea_rech_weeks_since_last_recharge
                10,  # fea_rech_bal_weekly_below_500_01m
                7,  # fea_rech_tot_days_bal_below_500_01m
                7,  # fea_rech_tot_days_bal_more_than_20000_below_40000_01m
                7,  # fea_rech_tot_days_bal_more_than_40000_below_60000_01m
                7,  # fea_rech_tot_days_bal_more_than_20000_below_60000_01m
            ],
            [
                "222",  # msisdn
                datetime.date(2020, 7, 6),  # weekstart
                300,  # fea_rech_tot_amt_sum_01w
                300,  # fea_rech_tot_amt_sum_03m
                3,  # fea_rech_tot_trx_sum_01w
                float("nan"),  # fea_rech_tot_amt_stddev_03m
                300,  # fea_rech_tot_amt_sum_03w
                100.0,  # fea_rech_tot_amt_avg_02m
                1.0,  # fea_rech_tot_trx_trd_03m
                3,  # fea_rech_tot_trx_sum_01m
                300,  # fea_rech_tot_amt_sum_01m
                1.0,  # fea_rech_tot_amt_trd_03m
                3,  # fea_rech_tot_trx_sum_02w
                3,  # fea_rech_tot_trx_sum_03m
                100.0,  # fea_rech_tot_amt_avg_01w
                100.0,  # fea_rech_tot_amt_avg_03m
                300,  # fea_rech_tot_amt_sum_02m
                3,  # fea_rech_tot_trx_sum_03w
                300,  # fea_rech_tot_amt_sum_02w
                100.0,  # fea_rech_tot_amt_avg_03w
                3,  # fea_rech_tot_trx_sum_02m
                100.0,  # fea_rech_tot_amt_avg_01m
                100.0,  # fea_rech_tot_amt_avg_02w
                0,  # fea_rech_tot_days_bal_more_than_20000_03m
                1.5,  # fea_rech_tot_days_no_bal_avg_02m
                0,  # fea_rech_tot_days_bal_more_than_50000_01w
                1,  # fea_rech_num_days_past_since_bal_5000_or_less_03w
                0,  # fea_rech_tot_days_bal_more_than_50000_01m
                None,  # fea_rech_last_bal_before_rech_avg_03m
                483.3333333333333,  # fea_rech_bal_monthly_avg_03m
                1.0,  # fea_rech_tot_days_no_bal_avg_03m
                None,  # fea_rech_last_bal_before_rech_avg_03w
                0,  # fea_rech_tot_days_bal_more_than_5000_02m
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_02w
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_03m
                0,  # fea_rech_tot_days_bal_more_than_5000_03m
                725.0,  # fea_rech_bal_monthly_avg_02m
                None,  # fea_rech_last_bal_before_rech_avg_01m
                0,  # fea_rech_tot_days_bal_more_than_10000_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_03m
                None,  # fea_rech_num_days_past_since_bal_more_than_10000_02m
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_02w
                1,  # fea_rech_num_days_past_since_bal_5000_or_less_01m
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_02w
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_03w
                1450.0,  # fea_rech_bal_weekly_avg_01w
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_01m
                1,  # fea_rech_num_days_past_since_bal_10000_or_less_01w
                0,  # fea_rech_tot_days_bal_more_than_10000_01w
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_03w
                0,  # fea_rech_tot_days_bal_more_than_5000_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_02m
                1,  # fea_rech_num_days_past_since_bal_10000_or_less_02m
                None,  # fea_rech_last_bal_before_rech_avg_01w
                1450.0,  # fea_rech_bal_min_02w
                1,  # fea_rech_num_days_past_since_bal_5000_or_less_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_03m
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_01w
                0,  # fea_rech_tot_days_bal_more_than_20000_02w
                1,  # fea_rech_num_days_past_since_bal_10000_or_less_01m
                0,  # fea_rech_tot_days_bal_more_than_50000_02m
                None,  # fea_rech_last_bal_before_rech_avg_02m
                0,  # fea_rech_tot_days_bal_more_than_5000_01m
                None,  # fea_rech_num_days_past_since_bal_more_than_5000_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_03w
                None,  # fea_rech_num_days_past_since_bal_more_than_5000_01m
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_02m
                0,  # fea_rech_tot_days_bal_more_than_20000_01m
                3,  # fea_rech_tot_days_no_bal_sum_02m
                1,  # fea_rech_num_days_past_since_bal_5000_or_less_01w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_01w
                1450.0,  # fea_rech_bal_min_02m
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_03w
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_03m
                1450.0,  # fea_rech_bal_weekly_avg_03m
                1,  # fea_rech_num_days_past_since_bal_10000_or_less_03m
                0,  # fea_rech_tot_days_bal_more_than_10000_03w
                1450.0,  # fea_rech_bal_weekly_avg_02w
                0,  # fea_rech_tot_days_bal_more_than_50000_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_10000_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_5000_01w
                0,  # fea_rech_tot_days_bal_more_than_10000_01m
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_02m
                0,  # fea_rech_tot_days_bal_more_than_20000_01w
                0,  # fea_rech_tot_days_bal_more_than_20000_02m
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_01w
                None,  # fea_rech_num_days_past_since_bal_more_than_5000_02m
                1450.0,  # fea_rech_bal_weekly_avg_02m
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_02w
                1,  # fea_rech_num_days_past_since_bal_10000_or_less_03w
                0,  # fea_rech_tot_days_bal_more_than_10000_02m
                3,  # fea_rech_tot_days_no_bal_sum_01m
                1450.0,  # fea_rech_bal_min_03w
                3,  # fea_rech_tot_days_no_bal_sum_02w
                1,  # fea_rech_num_days_past_since_bal_10000_or_less_02w
                1450.0,  # fea_rech_current_balance
                None,  # fea_rech_num_days_past_since_bal_more_than_5000_03w
                1450.0,  # fea_rech_bal_monthly_avg_01m
                None,  # fea_rech_last_bal_before_rech_avg_02w
                1450.0,  # fea_rech_bal_min_01w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_02m
                3,  # fea_rech_tot_days_no_bal_sum_01w
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_01w
                0,  # fea_rech_tot_days_bal_more_than_5000_01w
                None,  # fea_rech_num_days_past_since_bal_more_than_10000_03w
                0,  # fea_rech_tot_days_bal_more_than_50000_03w
                0,  # fea_rech_tot_days_bal_more_than_5000_03w
                None,  # fea_rech_num_days_past_since_bal_more_than_10000_03m
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_01m
                None,  # fea_rech_num_days_past_since_bal_more_than_5000_03m
                1450.0,  # fea_rech_bal_weekly_avg_03w
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_01m
                1,  # fea_rech_num_days_past_since_bal_5000_or_less_03m
                None,  # fea_rech_num_days_past_since_bal_more_than_10000_01w
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_01m
                None,  # fea_rech_num_days_past_since_bal_more_than_10000_01m
                0,  # fea_rech_tot_days_bal_more_than_50000_03m
                1450.0,  # fea_rech_bal_min_03m
                3,  # fea_rech_tot_days_no_bal_sum_03m
                1450.0,  # fea_rech_bal_min_01m
                0,  # fea_rech_tot_days_bal_more_than_20000_03w
                0,  # fea_rech_tot_days_bal_more_than_10000_03m
                1450.0,  # fea_rech_bal_weekly_avg_01m
                1,  # fea_rech_num_days_past_since_bal_5000_or_less_02m
                3,  # fea_rech_tot_days_no_bal_sum_03w
                3,  # fea_rech_trx_pkg_prchse_sum_02m
                3,  # fea_rech_trx_pkg_prchse_sum_01m
                3000,  # fea_rech_rev_pkg_prchse_sum_03m
                3000,  # fea_rech_rev_pkg_prchse_sum_01m
                3000,  # fea_rech_rev_pkg_prchse_sum_02w
                float("nan"),  # fea_rech_trx_pkg_prchse_stddev_03m
                3000,  # fea_rech_rev_pkg_prchse_sum_03w
                3,  # fea_rech_trx_pkg_prchse_sum_03m
                3000,  # fea_rech_rev_pkg_prchse_sum_01w
                3000,  # fea_rech_rev_pkg_prchse_sum_02m
                float("nan"),  # fea_rech_rev_pkg_prchse_stddev_03m
                0.375,  # fea_rech_trx_pkg_prchse_avg_02m
                375.0,  # fea_rech_rev_pkg_prchse_avg_02m
                0.23076923076923078,  # fea_rech_trx_pkg_prchse_avg_03m
                3,  # fea_rech_trx_pkg_prchse_sum_03w
                3,  # fea_rech_trx_pkg_prchse_sum_02w
                7.211102550927978,  # fea_rech_annualize_factor
                230.76923076923077,  # fea_rech_rev_pkg_prchse_avg_03m
                3,  # fea_rech_trx_pkg_prchse_sum_01w
                0,  # fea_rech_weeks_since_last_recharge
                0,  # fea_rech_bal_weekly_below_500_01m
                0,  # fea_rech_tot_days_bal_below_500_01m
                0,  # fea_rech_tot_days_bal_more_than_20000_below_40000_01m
                0,
                0,
            ],
            [
                "222",  # msisdn
                datetime.date(2020, 7, 13),  # weekstart
                400,  # fea_rech_tot_amt_sum_01w
                700,  # fea_rech_tot_amt_sum_03m
                4,  # fea_rech_tot_trx_sum_01w
                0.0,  # fea_rech_tot_amt_stddev_03m
                700,  # fea_rech_tot_amt_sum_03w
                100.0,  # fea_rech_tot_amt_avg_02m
                1.0,  # fea_rech_tot_trx_trd_03m
                7,  # fea_rech_tot_trx_sum_01m
                700,  # fea_rech_tot_amt_sum_01m
                1.0,  # fea_rech_tot_amt_trd_03m
                7,  # fea_rech_tot_trx_sum_02w
                7,  # fea_rech_tot_trx_sum_03m
                100.0,  # fea_rech_tot_amt_avg_01w
                100.0,  # fea_rech_tot_amt_avg_03m
                700,  # fea_rech_tot_amt_sum_02m
                7,  # fea_rech_tot_trx_sum_03w
                700,  # fea_rech_tot_amt_sum_02w
                100.0,  # fea_rech_tot_amt_avg_03w
                7,  # fea_rech_tot_trx_sum_02m
                100.0,  # fea_rech_tot_amt_avg_01m
                100.0,  # fea_rech_tot_amt_avg_02w
                0,  # fea_rech_tot_days_bal_more_than_20000_03m
                3.5,  # fea_rech_tot_days_no_bal_avg_02m
                0,  # fea_rech_tot_days_bal_more_than_50000_01w
                8,  # fea_rech_num_days_past_since_bal_5000_or_less_03w
                0,  # fea_rech_tot_days_bal_more_than_50000_01m
                None,  # fea_rech_last_bal_before_rech_avg_03m
                4796.857143,  # fea_rech_bal_monthly_avg_03m
                2.3333333333333335,  # fea_rech_tot_days_no_bal_avg_03m
                None,  # fea_rech_last_bal_before_rech_avg_03w
                7,  # fea_rech_tot_days_bal_more_than_5000_02m
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_02w
                3,  # fea_rech_num_days_past_since_bal_50000_or_less_03m
                7,  # fea_rech_tot_days_bal_more_than_5000_03m
                7195.2857145,  # fea_rech_bal_monthly_avg_02m
                None,  # fea_rech_last_bal_before_rech_avg_01m
                5,  # fea_rech_tot_days_bal_more_than_10000_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_03m
                3,  # fea_rech_num_days_past_since_bal_more_than_10000_02m
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_02w
                8,  # fea_rech_num_days_past_since_bal_5000_or_less_01m
                3,  # fea_rech_num_days_past_since_bal_20000_or_less_02w
                3,  # fea_rech_num_days_past_since_bal_50000_or_less_03w
                12940.571429,  # fea_rech_bal_weekly_avg_01w
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_01m
                6,  # fea_rech_num_days_past_since_bal_10000_or_less_01w
                5,  # fea_rech_tot_days_bal_more_than_10000_01w
                3,  # fea_rech_num_days_past_since_bal_20000_or_less_03w
                7,  # fea_rech_tot_days_bal_more_than_5000_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_02m
                6,  # fea_rech_num_days_past_since_bal_10000_or_less_02m
                None,  # fea_rech_last_bal_before_rech_avg_01w
                1450.0,  # fea_rech_bal_min_02w
                8,  # fea_rech_num_days_past_since_bal_5000_or_less_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_03m
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_01w
                0,  # fea_rech_tot_days_bal_more_than_20000_02w
                6,  # fea_rech_num_days_past_since_bal_10000_or_less_01m
                0,  # fea_rech_tot_days_bal_more_than_50000_02m
                None,  # fea_rech_last_bal_before_rech_avg_02m
                7,  # fea_rech_tot_days_bal_more_than_5000_01m
                3,  # fea_rech_num_days_past_since_bal_more_than_5000_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_03w
                3,  # fea_rech_num_days_past_since_bal_more_than_5000_01m
                3,  # fea_rech_num_days_past_since_bal_50000_or_less_02m
                0,  # fea_rech_tot_days_bal_more_than_20000_01m
                7,  # fea_rech_tot_days_no_bal_sum_02m
                None,  # fea_rech_num_days_past_since_bal_5000_or_less_01w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_01w
                1450.0,  # fea_rech_bal_min_02m
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_03w
                3,  # fea_rech_num_days_past_since_bal_20000_or_less_03m
                7195.2857145,  # fea_rech_bal_weekly_avg_03m
                6,  # fea_rech_num_days_past_since_bal_10000_or_less_03m
                5,  # fea_rech_tot_days_bal_more_than_10000_03w
                7195.2857145,  # fea_rech_bal_weekly_avg_02w
                0,  # fea_rech_tot_days_bal_more_than_50000_02w
                3,  # fea_rech_num_days_past_since_bal_more_than_10000_02w
                3,  # fea_rech_num_days_past_since_bal_more_than_5000_01w
                5,  # fea_rech_tot_days_bal_more_than_10000_01m
                3,  # fea_rech_num_days_past_since_bal_20000_or_less_02m
                0,  # fea_rech_tot_days_bal_more_than_20000_01w
                0,  # fea_rech_tot_days_bal_more_than_20000_02m
                3,  # fea_rech_num_days_past_since_bal_20000_or_less_01w
                3,  # fea_rech_num_days_past_since_bal_more_than_5000_02m
                7195.2857145,  # fea_rech_bal_weekly_avg_02m
                3,  # fea_rech_num_days_past_since_bal_50000_or_less_02w
                6,  # fea_rech_num_days_past_since_bal_10000_or_less_03w
                5,  # fea_rech_tot_days_bal_more_than_10000_02m
                7,  # fea_rech_tot_days_no_bal_sum_01m
                1450.0,  # fea_rech_bal_min_03w
                7,  # fea_rech_tot_days_no_bal_sum_02w
                6,  # fea_rech_num_days_past_since_bal_10000_or_less_02w
                14182.0,  # fea_rech_current_balance
                3,  # fea_rech_num_days_past_since_bal_more_than_5000_03w
                14390.571429,  # fea_rech_bal_monthly_avg_01m
                None,  # fea_rech_last_bal_before_rech_avg_02w
                6752.0,  # fea_rech_bal_min_01w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_02m
                4,  # fea_rech_tot_days_no_bal_sum_01w
                3,  # fea_rech_num_days_past_since_bal_50000_or_less_01w
                7,  # fea_rech_tot_days_bal_more_than_5000_01w
                3,  # fea_rech_num_days_past_since_bal_more_than_10000_03w
                0,  # fea_rech_tot_days_bal_more_than_50000_03w
                7,  # fea_rech_tot_days_bal_more_than_5000_03w
                3,  # fea_rech_num_days_past_since_bal_more_than_10000_03m
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_01m
                3,  # fea_rech_num_days_past_since_bal_more_than_5000_03m
                7195.2857145,  # fea_rech_bal_weekly_avg_03w
                3,  # fea_rech_num_days_past_since_bal_20000_or_less_01m
                8,  # fea_rech_num_days_past_since_bal_5000_or_less_03m
                3,  # fea_rech_num_days_past_since_bal_more_than_10000_01w
                3,  # fea_rech_num_days_past_since_bal_50000_or_less_01m
                3,  # fea_rech_num_days_past_since_bal_more_than_10000_01m
                0,  # fea_rech_tot_days_bal_more_than_50000_03m
                1450.0,  # fea_rech_bal_min_03m
                7,  # fea_rech_tot_days_no_bal_sum_03m
                1450.0,  # fea_rech_bal_min_01m
                0,  # fea_rech_tot_days_bal_more_than_20000_03w
                5,  # fea_rech_tot_days_bal_more_than_10000_03m
                7195.2857145,  # fea_rech_bal_weekly_avg_01m
                8,  # fea_rech_num_days_past_since_bal_5000_or_less_02m
                7,  # fea_rech_tot_days_no_bal_sum_03w
                7,  # fea_rech_trx_pkg_prchse_sum_02m
                7,  # fea_rech_trx_pkg_prchse_sum_01m
                7000,  # fea_rech_rev_pkg_prchse_sum_03m
                7000,  # fea_rech_rev_pkg_prchse_sum_01m
                7000,  # fea_rech_rev_pkg_prchse_sum_02w
                5.099019513592785,  # fea_rech_trx_pkg_prchse_stddev_03m
                7000,  # fea_rech_rev_pkg_prchse_sum_03w
                7,  # fea_rech_trx_pkg_prchse_sum_03m
                4000,  # fea_rech_rev_pkg_prchse_sum_01w
                7000,  # fea_rech_rev_pkg_prchse_sum_02m
                5099.019513592785,  # fea_rech_rev_pkg_prchse_stddev_03m
                0.875,  # fea_rech_trx_pkg_prchse_avg_02m
                875.0,  # fea_rech_rev_pkg_prchse_avg_02m
                0.5384615384615384,  # fea_rech_trx_pkg_prchse_avg_03m
                7,  # fea_rech_trx_pkg_prchse_sum_03w
                7,  # fea_rech_trx_pkg_prchse_sum_02w
                7.211102550927978,  # fea_rech_annualize_factor
                538.4615384615385,  # fea_rech_rev_pkg_prchse_avg_03m
                4,  # fea_rech_trx_pkg_prchse_sum_01w
                0,  # fea_rech_weeks_since_last_recharge
                5,  # fea_rech_bal_weekly_below_500_01m
                0,  # fea_rech_tot_days_bal_below_500_01m
                0,  # fea_rech_tot_days_bal_more_than_20000_below_40000_01m
                5,  # fea_rech_tot_days_bal_more_than_40000_below_60000_01m
                5,  # fea_rech_tot_days_bal_more_than_20000_below_60000_01m
            ],
            [
                "222",  # msisdn
                datetime.date(2020, 7, 20),  # weekstart
                500,  # fea_rech_tot_amt_sum_01w
                1200,  # fea_rech_tot_amt_sum_03m
                5,  # fea_rech_tot_trx_sum_01w
                0.0,  # fea_rech_tot_amt_stddev_03m
                1200,  # fea_rech_tot_amt_sum_03w
                100.0,  # fea_rech_tot_amt_avg_02m
                1.0,  # fea_rech_tot_trx_trd_03m
                12,  # fea_rech_tot_trx_sum_01m
                1200,  # fea_rech_tot_amt_sum_01m
                1.0,  # fea_rech_tot_amt_trd_03m
                9,  # fea_rech_tot_trx_sum_02w
                12,  # fea_rech_tot_trx_sum_03m
                100.0,  # fea_rech_tot_amt_avg_01w
                100.0,  # fea_rech_tot_amt_avg_03m
                1200,  # fea_rech_tot_amt_sum_02m
                12,  # fea_rech_tot_trx_sum_03w
                900,  # fea_rech_tot_amt_sum_02w
                100.0,  # fea_rech_tot_amt_avg_03w
                12,  # fea_rech_tot_trx_sum_02m
                100.0,  # fea_rech_tot_amt_avg_01m
                100.0,  # fea_rech_tot_amt_avg_02w
                0,  # fea_rech_tot_days_bal_more_than_20000_03m
                6.0,  # fea_rech_tot_days_no_bal_avg_02m
                0,  # fea_rech_tot_days_bal_more_than_50000_01w
                15,  # fea_rech_num_days_past_since_bal_5000_or_less_03w
                0,  # fea_rech_tot_days_bal_more_than_50000_01m
                10777.0,  # fea_rech_last_bal_before_rech_avg_03m
                8627.285714333333,  # fea_rech_bal_monthly_avg_03m
                4.0,  # fea_rech_tot_days_no_bal_avg_03m
                10777.0,  # fea_rech_last_bal_before_rech_avg_03w
                14,  # fea_rech_tot_days_bal_more_than_5000_02m
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_02w
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_03m
                14,  # fea_rech_tot_days_bal_more_than_5000_03m
                12940.9285715,  # fea_rech_bal_monthly_avg_02m
                10777.0,  # fea_rech_last_bal_before_rech_avg_01m
                12,  # fea_rech_tot_days_bal_more_than_10000_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_03m
                1,  # fea_rech_num_days_past_since_bal_more_than_10000_02m
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_02w
                15,  # fea_rech_num_days_past_since_bal_5000_or_less_01m
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_02w
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_03w
                11491.285714,  # fea_rech_bal_weekly_avg_01w
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_01m
                None,  # fea_rech_num_days_past_since_bal_10000_or_less_01w
                7,  # fea_rech_tot_days_bal_more_than_10000_01w
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_03w
                14,  # fea_rech_tot_days_bal_more_than_5000_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_02m
                13,  # fea_rech_num_days_past_since_bal_10000_or_less_02m
                10777.0,  # fea_rech_last_bal_before_rech_avg_01w
                6752.0,  # fea_rech_bal_min_02w
                None,  # fea_rech_num_days_past_since_bal_5000_or_less_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_03m
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_01w
                0,  # fea_rech_tot_days_bal_more_than_20000_02w
                13,  # fea_rech_num_days_past_since_bal_10000_or_less_01m
                0,  # fea_rech_tot_days_bal_more_than_50000_02m
                10777.0,  # fea_rech_last_bal_before_rech_avg_02m
                14,  # fea_rech_tot_days_bal_more_than_5000_01m
                1,  # fea_rech_num_days_past_since_bal_more_than_5000_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_03w
                1,  # fea_rech_num_days_past_since_bal_more_than_5000_01m
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_02m
                0,  # fea_rech_tot_days_bal_more_than_20000_01m
                12,  # fea_rech_tot_days_no_bal_sum_02m
                None,  # fea_rech_num_days_past_since_bal_5000_or_less_01w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_01w
                1450.0,  # fea_rech_bal_min_02m
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_03w
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_03m
                8627.285714333333,  # fea_rech_bal_weekly_avg_03m
                13,  # fea_rech_num_days_past_since_bal_10000_or_less_03m
                12,  # fea_rech_tot_days_bal_more_than_10000_03w
                12215.9285715,  # fea_rech_bal_weekly_avg_02w
                0,  # fea_rech_tot_days_bal_more_than_50000_02w
                1,  # fea_rech_num_days_past_since_bal_more_than_10000_02w
                1,  # fea_rech_num_days_past_since_bal_more_than_5000_01w
                12,  # fea_rech_tot_days_bal_more_than_10000_01m
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_02m
                0,  # fea_rech_tot_days_bal_more_than_20000_01w
                0,  # fea_rech_tot_days_bal_more_than_20000_02m
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_01w
                1,  # fea_rech_num_days_past_since_bal_more_than_5000_02m
                8627.285714333333,  # fea_rech_bal_weekly_avg_02m
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_02w
                13,  # fea_rech_num_days_past_since_bal_10000_or_less_03w
                12,  # fea_rech_tot_days_bal_more_than_10000_02m
                12,  # fea_rech_tot_days_no_bal_sum_01m
                1450.0,  # fea_rech_bal_min_03w
                9,  # fea_rech_tot_days_no_bal_sum_02w
                13,  # fea_rech_num_days_past_since_bal_10000_or_less_02w
                10777.0,  # fea_rech_current_balance
                1,  # fea_rech_num_days_past_since_bal_more_than_5000_03w
                25881.857143,  # fea_rech_bal_monthly_avg_01m
                10777.0,  # fea_rech_last_bal_before_rech_avg_02w
                10777.0,  # fea_rech_bal_min_01w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_02m
                5,  # fea_rech_tot_days_no_bal_sum_01w
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_01w
                7,  # fea_rech_tot_days_bal_more_than_5000_01w
                1,  # fea_rech_num_days_past_since_bal_more_than_10000_03w
                0,  # fea_rech_tot_days_bal_more_than_50000_03w
                14,  # fea_rech_tot_days_bal_more_than_5000_03w
                1,  # fea_rech_num_days_past_since_bal_more_than_10000_03m
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_01m
                1,  # fea_rech_num_days_past_since_bal_more_than_5000_03m
                8627.285714333333,  # fea_rech_bal_weekly_avg_03w
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_01m
                15,  # fea_rech_num_days_past_since_bal_5000_or_less_03m
                1,  # fea_rech_num_days_past_since_bal_more_than_10000_01w
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_01m
                1,  # fea_rech_num_days_past_since_bal_more_than_10000_01m
                0,  # fea_rech_tot_days_bal_more_than_50000_03m
                1450.0,  # fea_rech_bal_min_03m
                12,  # fea_rech_tot_days_no_bal_sum_03m
                1450.0,  # fea_rech_bal_min_01m
                0,  # fea_rech_tot_days_bal_more_than_20000_03w
                12,  # fea_rech_tot_days_bal_more_than_10000_03m
                8627.285714333333,  # fea_rech_bal_weekly_avg_01m
                15,  # fea_rech_num_days_past_since_bal_5000_or_less_02m
                12,  # fea_rech_tot_days_no_bal_sum_03w
                12,  # fea_rech_trx_pkg_prchse_sum_02m
                12,  # fea_rech_trx_pkg_prchse_sum_01m
                12000,  # fea_rech_rev_pkg_prchse_sum_03m
                12000,  # fea_rech_rev_pkg_prchse_sum_01m
                9000,  # fea_rech_rev_pkg_prchse_sum_02w
                7.211102550927978,  # fea_rech_trx_pkg_prchse_stddev_03m
                12000,  # fea_rech_rev_pkg_prchse_sum_03w
                12,  # fea_rech_trx_pkg_prchse_sum_03m
                5000,  # fea_rech_rev_pkg_prchse_sum_01w
                12000,  # fea_rech_rev_pkg_prchse_sum_02m
                7211.102550927978,  # fea_rech_rev_pkg_prchse_stddev_03m
                1.5,  # fea_rech_trx_pkg_prchse_avg_02m
                1500.0,  # fea_rech_rev_pkg_prchse_avg_02m
                0.9230769230769231,  # fea_rech_trx_pkg_prchse_avg_03m
                12,  # fea_rech_trx_pkg_prchse_sum_03w
                9,  # fea_rech_trx_pkg_prchse_sum_02w
                7.211102550927978,  # fea_rech_annualize_factor
                923.0769230769231,  # fea_rech_rev_pkg_prchse_avg_03m
                5,  # fea_rech_trx_pkg_prchse_sum_01w
                0,  # fea_rech_weeks_since_last_recharge
                10,  # fea_rech_bal_weekly_below_500_01m
                0,  # fea_rech_tot_days_bal_below_500_01m
                0,  # fea_rech_tot_days_bal_more_than_20000_below_40000_01m
                10,  # fea_rech_tot_days_bal_more_than_40000_below_60000_01m
                10,  # fea_rech_tot_days_bal_more_than_20000_below_60000_01m
            ],
            [
                "222",  # msisdn
                datetime.date(2020, 7, 27),  # weekstart
                None,  # fea_rech_tot_amt_sum_01w
                1200,  # fea_rech_tot_amt_sum_03m
                1,  # fea_rech_tot_trx_sum_01w
                0.0,  # fea_rech_tot_amt_stddev_03m
                900,  # fea_rech_tot_amt_sum_03w
                92.3076923076923,  # fea_rech_tot_amt_avg_02m
                1.0,  # fea_rech_tot_trx_trd_03m
                13,  # fea_rech_tot_trx_sum_01m
                1200,  # fea_rech_tot_amt_sum_01m
                1.0,  # fea_rech_tot_amt_trd_03m
                6,  # fea_rech_tot_trx_sum_02w
                13,  # fea_rech_tot_trx_sum_03m
                None,  # fea_rech_tot_amt_avg_01w
                92.3076923076923,  # fea_rech_tot_amt_avg_03m
                1200,  # fea_rech_tot_amt_sum_02m
                10,  # fea_rech_tot_trx_sum_03w
                500,  # fea_rech_tot_amt_sum_02w
                90.0,  # fea_rech_tot_amt_avg_03w
                13,  # fea_rech_tot_trx_sum_02m
                92.3076923076923,  # fea_rech_tot_amt_avg_01m
                83.33333333333333,  # fea_rech_tot_amt_avg_02w
                0,  # fea_rech_tot_days_bal_more_than_20000_03m
                8.0,  # fea_rech_tot_days_no_bal_avg_02m
                0,  # fea_rech_tot_days_bal_more_than_50000_01w
                None,  # fea_rech_num_days_past_since_bal_5000_or_less_03w
                0,  # fea_rech_tot_days_bal_more_than_50000_01m
                10777.0,  # fea_rech_last_bal_before_rech_avg_03m
                12457.714285666667,  # fea_rech_bal_monthly_avg_03m
                5.333333333333333,  # fea_rech_tot_days_no_bal_avg_03m
                10777.0,  # fea_rech_last_bal_before_rech_avg_03w
                21,  # fea_rech_tot_days_bal_more_than_5000_02m
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_02w
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_03m
                21,  # fea_rech_tot_days_bal_more_than_5000_03m
                18686.5714285,  # fea_rech_bal_monthly_avg_02m
                10777.0,  # fea_rech_last_bal_before_rech_avg_01m
                14,  # fea_rech_tot_days_bal_more_than_10000_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_03m
                1,  # fea_rech_num_days_past_since_bal_more_than_10000_02m
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_02w
                22,  # fea_rech_num_days_past_since_bal_5000_or_less_01m
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_02w
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_03w
                11491.285714,  # fea_rech_bal_weekly_avg_01w
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_01m
                None,  # fea_rech_num_days_past_since_bal_10000_or_less_01w
                7,  # fea_rech_tot_days_bal_more_than_10000_01w
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_03w
                14,  # fea_rech_tot_days_bal_more_than_5000_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_02m
                20,  # fea_rech_num_days_past_since_bal_10000_or_less_02m
                10777.0,  # fea_rech_last_bal_before_rech_avg_01w
                10777.0,  # fea_rech_bal_min_02w
                None,  # fea_rech_num_days_past_since_bal_5000_or_less_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_03m
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_01w
                0,  # fea_rech_tot_days_bal_more_than_20000_02w
                20,  # fea_rech_num_days_past_since_bal_10000_or_less_01m
                0,  # fea_rech_tot_days_bal_more_than_50000_02m
                10777.0,  # fea_rech_last_bal_before_rech_avg_02m
                21,  # fea_rech_tot_days_bal_more_than_5000_01m
                1,  # fea_rech_num_days_past_since_bal_more_than_5000_02w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_03w
                1,  # fea_rech_num_days_past_since_bal_more_than_5000_01m
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_02m
                0,  # fea_rech_tot_days_bal_more_than_20000_01m
                16,  # fea_rech_tot_days_no_bal_sum_02m
                None,  # fea_rech_num_days_past_since_bal_5000_or_less_01w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_01w
                1450.0,  # fea_rech_bal_min_02m
                None,  # fea_rech_num_days_past_since_bal_more_than_20000_03w
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_03m
                9343.28571425,  # fea_rech_bal_weekly_avg_03m
                20,  # fea_rech_num_days_past_since_bal_10000_or_less_03m
                19,  # fea_rech_tot_days_bal_more_than_10000_03w
                11491.285714,  # fea_rech_bal_weekly_avg_02w
                0,  # fea_rech_tot_days_bal_more_than_50000_02w
                1,  # fea_rech_num_days_past_since_bal_more_than_10000_02w
                1,  # fea_rech_num_days_past_since_bal_more_than_5000_01w
                19,  # fea_rech_tot_days_bal_more_than_10000_01m
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_02m
                0,  # fea_rech_tot_days_bal_more_than_20000_01w
                0,  # fea_rech_tot_days_bal_more_than_20000_02m
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_01w
                1,  # fea_rech_num_days_past_since_bal_more_than_5000_02m
                9343.28571425,  # fea_rech_bal_weekly_avg_02m
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_02w
                20,  # fea_rech_num_days_past_since_bal_10000_or_less_03w
                19,  # fea_rech_tot_days_bal_more_than_10000_02m
                16,  # fea_rech_tot_days_no_bal_sum_01m
                6752.0,  # fea_rech_bal_min_03w
                9,  # fea_rech_tot_days_no_bal_sum_02w
                None,  # fea_rech_num_days_past_since_bal_10000_or_less_02w
                10777.0,  # fea_rech_current_balance
                1,  # fea_rech_num_days_past_since_bal_more_than_5000_03w
                37373.142857,  # fea_rech_bal_monthly_avg_01m
                10777.0,  # fea_rech_last_bal_before_rech_avg_02w
                10777.0,  # fea_rech_bal_min_01w
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_02m
                4,  # fea_rech_tot_days_no_bal_sum_01w
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_01w
                7,  # fea_rech_tot_days_bal_more_than_5000_01w
                1,  # fea_rech_num_days_past_since_bal_more_than_10000_03w
                0,  # fea_rech_tot_days_bal_more_than_50000_03w
                21,  # fea_rech_tot_days_bal_more_than_5000_03w
                1,  # fea_rech_num_days_past_since_bal_more_than_10000_03m
                None,  # fea_rech_num_days_past_since_bal_more_than_50000_01m
                1,  # fea_rech_num_days_past_since_bal_more_than_5000_03m
                11974.380952333333,  # fea_rech_bal_weekly_avg_03w
                1,  # fea_rech_num_days_past_since_bal_20000_or_less_01m
                22,  # fea_rech_num_days_past_since_bal_5000_or_less_03m
                1,  # fea_rech_num_days_past_since_bal_more_than_10000_01w
                1,  # fea_rech_num_days_past_since_bal_50000_or_less_01m
                1,  # fea_rech_num_days_past_since_bal_more_than_10000_01m
                0,  # fea_rech_tot_days_bal_more_than_50000_03m
                1450.0,  # fea_rech_bal_min_03m
                16,  # fea_rech_tot_days_no_bal_sum_03m
                1450.0,  # fea_rech_bal_min_01m
                0,  # fea_rech_tot_days_bal_more_than_20000_03w
                19,  # fea_rech_tot_days_bal_more_than_10000_03m
                9343.28571425,  # fea_rech_bal_weekly_avg_01m
                22,  # fea_rech_num_days_past_since_bal_5000_or_less_02m
                13,  # fea_rech_tot_days_no_bal_sum_03w
                16,  # fea_rech_trx_pkg_prchse_sum_02m
                16,  # fea_rech_trx_pkg_prchse_sum_01m
                16000,  # fea_rech_rev_pkg_prchse_sum_03m
                16000,  # fea_rech_rev_pkg_prchse_sum_01m
                9000,  # fea_rech_rev_pkg_prchse_sum_02w
                5.887840577551898,  # fea_rech_trx_pkg_prchse_stddev_03m
                13000,  # fea_rech_rev_pkg_prchse_sum_03w
                16,  # fea_rech_trx_pkg_prchse_sum_03m
                4000,  # fea_rech_rev_pkg_prchse_sum_01w
                16000,  # fea_rech_rev_pkg_prchse_sum_02m
                5887.840577551897,  # fea_rech_rev_pkg_prchse_stddev_03m
                2.0,  # fea_rech_trx_pkg_prchse_avg_02m
                2000.0,  # fea_rech_rev_pkg_prchse_avg_02m
                1.2307692307692308,  # fea_rech_trx_pkg_prchse_avg_03m
                13,  # fea_rech_trx_pkg_prchse_sum_03w
                9,  # fea_rech_trx_pkg_prchse_sum_02w
                7.211102550927978,  # fea_rech_annualize_factor
                1230.7692307692307,  # fea_rech_rev_pkg_prchse_avg_03m
                4,  # fea_rech_trx_pkg_prchse_sum_01w
                1,  # fea_rech_weeks_since_last_recharge
                15,  # fea_rech_bal_weekly_below_500_01m
                0,  # fea_rech_tot_days_bal_below_500_01m
                0,  # fea_rech_tot_days_bal_more_than_20000_below_40000_01m
                15,  # fea_rech_tot_days_bal_more_than_40000_below_60000_01m
                13,  # fea_rech_tot_days_bal_more_than_20000_below_60000_01m
            ],
        ]
        expected_fea_df = spark_session.createDataFrame(
            data=expected_rows,
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("fea_rech_tot_amt_sum_01w", LongType(), True),
                    StructField("fea_rech_tot_amt_sum_03m", LongType(), True),
                    StructField("fea_rech_tot_trx_sum_01w", LongType(), True),
                    StructField("fea_rech_tot_amt_stddev_03m", DoubleType(), True),
                    StructField("fea_rech_tot_amt_sum_03w", LongType(), True),
                    StructField("fea_rech_tot_amt_avg_02m", DoubleType(), True),
                    StructField("fea_rech_tot_trx_trd_03m", DoubleType(), True),
                    StructField("fea_rech_tot_trx_sum_01m", LongType(), True),
                    StructField("fea_rech_tot_amt_sum_01m", LongType(), True),
                    StructField("fea_rech_tot_amt_trd_03m", DoubleType(), True),
                    StructField("fea_rech_tot_trx_sum_02w", LongType(), True),
                    StructField("fea_rech_tot_trx_sum_03m", LongType(), True),
                    StructField("fea_rech_tot_amt_avg_01w", DoubleType(), True),
                    StructField("fea_rech_tot_amt_avg_03m", DoubleType(), True),
                    StructField("fea_rech_tot_amt_sum_02m", LongType(), True),
                    StructField("fea_rech_tot_trx_sum_03w", LongType(), True),
                    StructField("fea_rech_tot_amt_sum_02w", LongType(), True),
                    StructField("fea_rech_tot_amt_avg_03w", DoubleType(), True),
                    StructField("fea_rech_tot_trx_sum_02m", LongType(), True),
                    StructField("fea_rech_tot_amt_avg_01m", DoubleType(), True),
                    StructField("fea_rech_tot_amt_avg_02w", DoubleType(), True),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_20000_03m", LongType(), True
                    ),
                    StructField("fea_rech_tot_days_no_bal_avg_02m", DoubleType(), True),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_50000_01w", LongType(), True
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_5000_or_less_03w",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_50000_01m", LongType(), True
                    ),
                    StructField(
                        "fea_rech_last_bal_before_rech_avg_03m", DoubleType(), True
                    ),
                    StructField("fea_rech_bal_monthly_avg_03m", DoubleType(), True),
                    StructField("fea_rech_tot_days_no_bal_avg_03m", DoubleType(), True),
                    StructField(
                        "fea_rech_last_bal_before_rech_avg_03w", DoubleType(), True
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_5000_02m", LongType(), True
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_20000_02w",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_50000_or_less_03m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_5000_03m", LongType(), True
                    ),
                    StructField("fea_rech_bal_monthly_avg_02m", DoubleType(), True),
                    StructField(
                        "fea_rech_last_bal_before_rech_avg_01m", DoubleType(), True
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_10000_02w", LongType(), True
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_50000_03m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_10000_02m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_50000_02w",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_5000_or_less_01m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_20000_or_less_02w",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_50000_or_less_03w",
                        IntegerType(),
                        True,
                    ),
                    StructField("fea_rech_bal_weekly_avg_01w", DoubleType(), True),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_20000_01m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_10000_or_less_01w",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_10000_01w", LongType(), True
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_20000_or_less_03w",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_5000_02w", LongType(), True
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_20000_02m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_10000_or_less_02m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_last_bal_before_rech_avg_01w", DoubleType(), True
                    ),
                    StructField("fea_rech_bal_min_02w", DoubleType(), True),
                    StructField(
                        "fea_rech_num_days_past_since_bal_5000_or_less_02w",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_20000_03m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_20000_01w",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_20000_02w", LongType(), True
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_10000_or_less_01m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_50000_02m", LongType(), True
                    ),
                    StructField(
                        "fea_rech_last_bal_before_rech_avg_02m", DoubleType(), True
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_5000_01m", LongType(), True
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_5000_02w",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_50000_03w",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_5000_01m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_50000_or_less_02m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_20000_01m", LongType(), True
                    ),
                    StructField("fea_rech_tot_days_no_bal_sum_02m", LongType(), True),
                    StructField(
                        "fea_rech_num_days_past_since_bal_5000_or_less_01w",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_50000_01w",
                        IntegerType(),
                        True,
                    ),
                    StructField("fea_rech_bal_min_02m", DoubleType(), True),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_20000_03w",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_20000_or_less_03m",
                        IntegerType(),
                        True,
                    ),
                    StructField("fea_rech_bal_weekly_avg_03m", DoubleType(), True),
                    StructField(
                        "fea_rech_num_days_past_since_bal_10000_or_less_03m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_10000_03w", LongType(), True
                    ),
                    StructField("fea_rech_bal_weekly_avg_02w", DoubleType(), True),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_50000_02w", LongType(), True
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_10000_02w",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_5000_01w",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_10000_01m", LongType(), True
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_20000_or_less_02m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_20000_01w", LongType(), True
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_20000_02m", LongType(), True
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_20000_or_less_01w",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_5000_02m",
                        IntegerType(),
                        True,
                    ),
                    StructField("fea_rech_bal_weekly_avg_02m", DoubleType(), True),
                    StructField(
                        "fea_rech_num_days_past_since_bal_50000_or_less_02w",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_10000_or_less_03w",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_10000_02m", LongType(), True
                    ),
                    StructField("fea_rech_tot_days_no_bal_sum_01m", LongType(), True),
                    StructField("fea_rech_bal_min_03w", DoubleType(), True),
                    StructField("fea_rech_tot_days_no_bal_sum_02w", LongType(), True),
                    StructField(
                        "fea_rech_num_days_past_since_bal_10000_or_less_02w",
                        IntegerType(),
                        True,
                    ),
                    StructField("fea_rech_current_balance", DoubleType(), True),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_5000_03w",
                        IntegerType(),
                        True,
                    ),
                    StructField("fea_rech_bal_monthly_avg_01m", DoubleType(), True),
                    StructField(
                        "fea_rech_last_bal_before_rech_avg_02w", DoubleType(), True
                    ),
                    StructField("fea_rech_bal_min_01w", DoubleType(), True),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_50000_02m",
                        IntegerType(),
                        True,
                    ),
                    StructField("fea_rech_tot_days_no_bal_sum_01w", LongType(), True),
                    StructField(
                        "fea_rech_num_days_past_since_bal_50000_or_less_01w",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_5000_01w", LongType(), True
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_10000_03w",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_50000_03w", LongType(), True
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_5000_03w", LongType(), True
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_10000_03m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_50000_01m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_5000_03m",
                        IntegerType(),
                        True,
                    ),
                    StructField("fea_rech_bal_weekly_avg_03w", DoubleType(), True),
                    StructField(
                        "fea_rech_num_days_past_since_bal_20000_or_less_01m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_5000_or_less_03m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_10000_01w",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_50000_or_less_01m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_num_days_past_since_bal_more_than_10000_01m",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_50000_03m", LongType(), True
                    ),
                    StructField("fea_rech_bal_min_03m", DoubleType(), True),
                    StructField("fea_rech_tot_days_no_bal_sum_03m", LongType(), True),
                    StructField("fea_rech_bal_min_01m", DoubleType(), True),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_20000_03w", LongType(), True
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_10000_03m", LongType(), True
                    ),
                    StructField("fea_rech_bal_weekly_avg_01m", DoubleType(), True),
                    StructField(
                        "fea_rech_num_days_past_since_bal_5000_or_less_02m",
                        IntegerType(),
                        True,
                    ),
                    StructField("fea_rech_tot_days_no_bal_sum_03w", LongType(), True),
                    StructField("fea_rech_trx_pkg_prchse_sum_02m", LongType(), True),
                    StructField("fea_rech_trx_pkg_prchse_sum_01m", LongType(), True),
                    StructField("fea_rech_rev_pkg_prchse_sum_03m", LongType(), True),
                    StructField("fea_rech_rev_pkg_prchse_sum_01m", LongType(), True),
                    StructField("fea_rech_rev_pkg_prchse_sum_02w", LongType(), True),
                    StructField(
                        "fea_rech_trx_pkg_prchse_stddev_03m", DoubleType(), True
                    ),
                    StructField("fea_rech_rev_pkg_prchse_sum_03w", LongType(), True),
                    StructField("fea_rech_trx_pkg_prchse_sum_03m", LongType(), True),
                    StructField("fea_rech_rev_pkg_prchse_sum_01w", LongType(), True),
                    StructField("fea_rech_rev_pkg_prchse_sum_02m", LongType(), True),
                    StructField(
                        "fea_rech_rev_pkg_prchse_stddev_03m", DoubleType(), True
                    ),
                    StructField("fea_rech_trx_pkg_prchse_avg_02m", DoubleType(), True),
                    StructField("fea_rech_rev_pkg_prchse_avg_02m", DoubleType(), True),
                    StructField("fea_rech_trx_pkg_prchse_avg_03m", DoubleType(), True),
                    StructField("fea_rech_trx_pkg_prchse_sum_03w", LongType(), True),
                    StructField("fea_rech_trx_pkg_prchse_sum_02w", LongType(), True),
                    StructField("fea_rech_annualize_factor", DoubleType(), True),
                    StructField("fea_rech_rev_pkg_prchse_avg_03m", DoubleType(), True),
                    StructField("fea_rech_trx_pkg_prchse_sum_01w", LongType(), True),
                    StructField("fea_rech_weeks_since_last_recharge", LongType(), True),
                    StructField("fea_rech_bal_weekly_below_500_01m", LongType(), True),
                    StructField(
                        "fea_rech_tot_days_bal_below_500_01m", LongType(), True
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_20000_below_40000_01m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_40000_below_60000_01m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_rech_tot_days_bal_more_than_20000_below_60000_01m",
                        LongType(),
                        True,
                    ),
                ]
            ),
        )
        assert_df_frame_equal(actual_fea_df, expected_fea_df)
