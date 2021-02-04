import datetime
from unittest import mock

import yaml
from pyspark.sql import Row, SparkSession

from src.dmp.pipelines.dmp._04_features.revenue.fea_revenue_threshold_voice_data import (
    fea_revenue_threshold_voice_data,
)


class TestFeatureRevenueThresholdVoiceData:
    config_feature = yaml.load(open("conf/dmp/training/parameters_feature.yml"))[
        "config_feature"
    ]

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.revenue.fea_revenue_threshold_voice_data.get_start_date",
        return_value=datetime.date(2018, 10, 10),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.revenue.fea_revenue_threshold_voice_data.get_end_date",
        return_value=datetime.date(2020, 10, 10),
        autospec=True,
    )
    def test_fea_revenue_threshold_voice_data(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        sc = spark_session.sparkContext

        df_revenue_weekly = sc.parallelize(
            [
                # Case A.2) Sanity test - 0 days
                [
                    "111",
                    "2019-10-14",
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                ],
                # Case B.2) Sanity test - with filled days
                [
                    "222",
                    "2019-10-14",
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                ],
                # Case C.2) Edge case - three weeks with None and 0 days
                [
                    "333",
                    "2019-10-07",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ],
                [
                    "333",
                    "2019-10-14",
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                ],
                [
                    "333",
                    "2019-10-21",
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                ],
                [
                    "333",
                    "2019-10-28",
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                ],
            ]
        )

        df_revenue_weekly = spark_session.createDataFrame(
            df_revenue_weekly.map(
                lambda x: Row(
                    msisdn=x[0],
                    weekstart=x[1],
                    days_with_rev_payu_voice_above_99_below_1000=x[2],
                    days_with_rev_payu_voice_above_999_below_5000=x[3],
                    days_with_rev_payu_voice_above_4999_below_10000=x[4],
                    days_with_rev_payu_voice_above_9999_below_50000=x[5],
                    days_with_rev_payu_voice_above_49999=x[6],
                    days_with_rev_payu_data_above_99_below_1000=x[7],
                    days_with_rev_payu_data_above_999_below_5000=x[8],
                    days_with_rev_payu_data_above_4999_below_10000=x[9],
                    days_with_rev_payu_data_above_9999_below_50000=x[10],
                    days_with_rev_payu_data_above_49999=x[11],
                    rev_alt_days_with_data_above_999_below_5000=x[12],
                    rev_alt_days_with_data_above_4999_below_10000=x[13],
                    rev_alt_days_with_data_above_9999_below_50000=x[14],
                    rev_alt_days_with_data_above_49999=x[15],
                    rev_alt_days_with_voice_above_999_below_5000=x[16],
                    rev_alt_days_with_voice_above_4999_below_10000=x[17],
                    rev_alt_days_with_voice_above_9999_below_50000=x[18],
                    rev_alt_days_with_voice_above_49999=x[19],
                )
            )
        )

        out_cols = [
            "msisdn",  # 1
            "weekstart",  # 2
            "fea_days_with_rev_voice_payu_above_99_below_1000_sum_03m",  # 3
            "fea_days_with_rev_voice_payu_above_999_below_5000_sum_03m",  # 4
            "fea_days_with_rev_voice_payu_above_4999_below_10000_sum_03m",  # 5
            "fea_days_with_rev_voice_payu_above_9999_below_50000_sum_03m",  # 6
            "fea_days_with_rev_voice_payu_above_49999_sum_03m",  # 7
            "fea_days_with_rev_data_payu_above_99_below_1000_sum_03m",  # 8
            "fea_days_with_rev_data_payu_above_999_below_5000_sum_03m",  # 9
            "fea_days_with_rev_data_payu_above_4999_below_10000_sum_03m",  # 10
            "fea_days_with_rev_data_payu_above_9999_below_50000_sum_03m",  # 11
            "fea_days_with_rev_data_payu_above_49999_sum_03m",  # 12
            "fea_rev_alt_days_with_data_above_999_below_5000_sum_03m",  # 13
            "fea_rev_alt_days_with_data_above_4999_below_10000_sum_03m",  # 14
            "fea_rev_alt_days_with_data_above_9999_below_50000_sum_03m",  # 15
            "fea_rev_alt_days_with_data_above_49999_sum_03m",  # 16
            "fea_rev_alt_days_with_voice_above_999_below_5000_sum_03m",  # 17
            "fea_rev_alt_days_with_voice_above_4999_below_10000_sum_03m",  # 18
            "fea_rev_alt_days_with_voice_above_9999_below_50000_sum_03m",  # 19
            "fea_rev_alt_days_with_voice_above_49999_sum_03m",  # 20
        ]

        out = fea_revenue_threshold_voice_data(
            df_revenue_weekly, self.config_feature, "all", []
        ).select(out_cols)
        out_list = [
            [
                i[0],
                i[1],
                i[2],
                i[3],
                i[4],
                i[5],
                i[6],
                i[7],
                i[8],
                i[9],
                i[10],
                i[11],
                i[12],
                i[13],
                i[14],
                i[15],
                i[16],
                i[17],
                i[18],
                i[19],
            ]
            for i in out.collect()
        ]

        assert sorted(out_list) == [
            # Case A) Sanity test - 0 days features
            ["111", "2019-10-14", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            # Case B) Sanity test - filled values for days features
            ["222", "2019-10-14", 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            # Case C) Edge case - days values null and 0 until two weeks enter
            [
                "333",
                "2019-10-07",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            ["333", "2019-10-14", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            ["333", "2019-10-21", 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            ["333", "2019-10-28", 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
        ]
