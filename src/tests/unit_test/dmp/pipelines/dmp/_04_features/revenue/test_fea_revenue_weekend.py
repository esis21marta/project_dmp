import datetime
from unittest import mock

import yaml
from pyspark.sql import Row, SparkSession

from src.dmp.pipelines.dmp._04_features.revenue.fea_revenue_weekend import (
    fea_revenue_weekend,
)


class TestFeatureRevenueWeekend:
    config_feature = yaml.load(open("conf/dmp/training/parameters_feature.yml"))[
        "config_feature"
    ]

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.revenue.fea_revenue_weekend.get_start_date",
        return_value=datetime.date(2018, 10, 10),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.revenue.fea_revenue_weekend.get_end_date",
        return_value=datetime.date(2020, 10, 10),
        autospec=True,
    )
    def test_fea_revenue_weekend(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        sc = spark_session.sparkContext

        df_revenue_weekly = sc.parallelize(
            [
                # Case A.1) Sanity test - only weekdays
                # Case A.2) Sanity test - only payu
                [
                    "111",
                    "2019-10-14",
                    300,
                    300,
                    300,
                    300,
                    300,
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
                # Case B.1) Sanity test - only weekends
                # Case B.2) Sanity test - only package
                [
                    "222",
                    "2019-10-14",
                    300,
                    300,
                    300,
                    300,
                    300,
                    300,
                    300,
                    300,
                    300,
                    300,
                    300,
                    300,
                    300,
                    300,
                ],
                # Case C.1) Edge case - three weeks with one null weekend
                # Case C.2) Edge case - three weeks with one payu, one package, and one full payu & package
                [
                    "333",
                    "2019-10-07",
                    10,
                    10,
                    10,
                    10,
                    10,
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
                    "2019-10-21",
                    0,
                    0,
                    0,
                    0,
                    0,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                ],
                # Case D) Edge case - three weeks with one null week
                [
                    "444",
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
                ],
                [
                    "444",
                    "2019-10-14",
                    10,
                    10,
                    10,
                    10,
                    10,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                ],
                [
                    "444",
                    "2019-10-21",
                    10,
                    10,
                    10,
                    10,
                    10,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                ],
            ]
        )

        df_revenue_weekly = spark_session.createDataFrame(
            df_revenue_weekly.map(
                lambda x: Row(
                    msisdn=x[0],
                    weekstart=x[1],
                    rev_voice=x[2],
                    rev_data=x[3],
                    rev_sms=x[4],
                    rev_payu_tot=x[5],
                    rev_pkg_tot=x[6],
                    rev_voice_weekend=x[7],
                    rev_data_weekend=x[8],
                    rev_sms_weekend=x[9],
                    rev_alt_sms=x[10],
                    rev_alt_data=x[11],
                    rev_alt_voice=x[12],
                    rev_alt_sms_weekend=x[13],
                    rev_alt_data_weekend=x[14],
                    rev_alt_voice_weekend=x[15],
                )
            )
        )

        out_cols = [
            "msisdn",  # 1
            "weekstart",  # 2
            "fea_rev_voice_tot_sum_03m",  # 3
            "fea_rev_data_tot_sum_03m",  # 4
            "fea_rev_data_sms_tot_sum_03m",  # 5
            "fea_rev_sms_tot_sum_03m",  # 6
            "fea_rev_voice_sms_weekend_tot_sum_03m",  # 7
            "fea_rev_voice_data_weekend_tot_sum_03m",  # 8
            "fea_rev_voice_weekend_tot_sum_03m",  # 9
            "fea_rev_sms_weekend_tot_sum_03m",  # 10
            "fea_rev_data_weekend_tot_sum_03m",  # 11
            "fea_rev_data_sms_weekend_tot_sum_03m",  # 12
            "fea_rev_alt_sms_tot_sum_03m",  # 13
            "fea_rev_alt_data_tot_sum_03m",  # 14
            "fea_rev_alt_data_sms_tot_sum_03m",  # 15
            "fea_rev_alt_voice_weekend_tot_sum_03m",  # 16
            "fea_rev_alt_sms_weekend_tot_sum_03m",  # 17
            "fea_rev_alt_data_weekend_tot_sum_03m",  # 18
        ]
        out = fea_revenue_weekend(
            df_revenue_weekly,
            self.config_feature,
            feature_mode="all",
            required_output_features=[],
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
            ]
            for i in out.collect()
        ]

        assert sorted(out_list) == [
            # Case A) Sanity test - null values for weekend features
            [
                "111",
                "2019-10-14",
                300,
                300,
                600,
                300,
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
            # Case B) Sanity test - filled values for weekend features
            [
                "222",
                "2019-10-14",
                300,
                300,
                600,
                300,
                600,
                600,
                300,
                300,
                300,
                600,
                300,
                300,
                600,
                300,
                300,
                300,
            ],
            # Case C) Edge case - weekend values null until week enters
            [
                "333",
                "2019-10-07",
                10,
                10,
                20,
                10,
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
                10,
                10,
                20,
                10,
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
                "2019-10-21",
                10,
                10,
                20,
                10,
                40,
                40,
                20,
                20,
                20,
                40,
                20,
                20,
                40,
                20,
                20,
                20,
            ],
            # Case D) Edge case - null week until next week enters
            [
                "444",
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
            ],
            [
                "444",
                "2019-10-14",
                10,
                10,
                20,
                10,
                40,
                40,
                20,
                20,
                20,
                40,
                20,
                20,
                40,
                20,
                20,
                20,
            ],
            [
                "444",
                "2019-10-21",
                20,
                20,
                40,
                20,
                80,
                80,
                40,
                40,
                40,
                80,
                40,
                40,
                80,
                40,
                40,
                40,
            ],
        ]
