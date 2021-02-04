# Copyright 2018-present QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
from unittest import mock

import yaml
from pyspark.sql import Row, SparkSession

from src.dmp.pipelines.dmp._04_features.revenue.fea_revenue_salary import (
    fea_revenue_salary,
)


class TestFeatureRevenueWeekend:
    config_feature = yaml.load(open("conf/dmp/training/parameters_feature.yml"))[
        "config_feature"
    ]

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.revenue.fea_revenue_salary.get_start_date",
        return_value=datetime.date(2018, 10, 10),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.revenue.fea_revenue_salary.get_end_date",
        return_value=datetime.date(2020, 10, 10),
        autospec=True,
    )
    def test_fea_revenue_salary(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        sc = spark_session.sparkContext

        df_revenue_weekly = sc.parallelize(
            [
                # Case A) Sanity test - all salary distribution in across 1_5, 11_15, 21_25, 26_31
                [
                    "111",
                    "2019-10-07",
                    10,
                    10,
                    10,
                    5,
                    5,
                    10,
                    10,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    20,
                    0,
                    0,
                ],
                [
                    "111",
                    "2019-10-14",
                    10,
                    10,
                    10,
                    5,
                    5,
                    10,
                    10,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    20,
                    0,
                    0,
                ],
                [
                    "111",
                    "2019-10-21",
                    20,
                    20,
                    20,
                    10,
                    10,
                    20,
                    20,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    10,
                    0,
                    0,
                    20,
                    0,
                    0,
                ],
                [
                    "111",
                    "2019-10-28",
                    10,
                    10,
                    10,
                    5,
                    5,
                    10,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    10,
                    0,
                    0,
                    20,
                    0,
                    0,
                ],
                # Case B) Sanity test - salary distribution only 1_5 and 26_31
                [
                    "222",
                    "2019-10-07",
                    10,
                    10,
                    10,
                    5,
                    5,
                    10,
                    10,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    20,
                    0,
                    0,
                ],
                [
                    "222",
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
                    0,
                ],
                [
                    "222",
                    "2019-10-21",
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
                    0,
                ],
                [
                    "222",
                    "2019-10-28",
                    10,
                    10,
                    10,
                    5,
                    5,
                    10,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    10,
                    0,
                    0,
                    20,
                    0,
                    0,
                ],
                # Case C) Edge case - salary distribution after null week
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
                    0,
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
                    10,
                    5,
                    5,
                    10,
                    10,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    20,
                    0,
                    0,
                ],
                [
                    "333",
                    "2019-10-21",
                    10,
                    10,
                    10,
                    5,
                    5,
                    10,
                    10,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    10,
                    0,
                    0,
                    20,
                    0,
                    0,
                ],
                [
                    "333",
                    "2019-10-28",
                    20,
                    20,
                    20,
                    10,
                    10,
                    20,
                    20,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    10,
                    0,
                    0,
                    20,
                    0,
                    0,
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
                    rev_payu_roam=x[5],
                    rev_pkg_roam=x[6],
                    rev_payu_tot=x[7],
                    rev_pkg_tot=x[8],
                    rev_voice_tot_1_5d=x[9],
                    rev_voice_tot_6_10d=x[10],
                    rev_voice_tot_11_15d=x[11],
                    rev_voice_tot_16_20d=x[12],
                    rev_voice_tot_21_25d=x[13],
                    rev_voice_tot_26_31d=x[14],
                    rev_data_tot_1_5d=x[15],
                    rev_data_tot_6_10d=x[16],
                    rev_data_tot_11_15d=x[17],
                    rev_data_tot_16_20d=x[18],
                    rev_data_tot_21_25d=x[19],
                    rev_data_tot_26_31d=x[20],
                    rev_sms_tot_1_5d=x[21],
                    rev_sms_tot_6_10d=x[22],
                    rev_sms_tot_11_15d=x[23],
                    rev_sms_tot_16_20d=x[24],
                    rev_sms_tot_21_25d=x[25],
                    rev_sms_tot_26_31d=x[26],
                    rev_roam_tot_1_5d=x[27],
                    rev_roam_tot_6_10d=x[28],
                    rev_roam_tot_11_15d=x[29],
                    rev_roam_tot_16_20d=x[30],
                    rev_roam_tot_21_25d=x[31],
                    rev_roam_tot_26_31d=x[32],
                    rev_alt_total=x[33],
                    rev_alt_voice=x[34],
                    rev_alt_data=x[35],
                    rev_alt_sms=x[36],
                    rev_alt_digital=x[37],
                    rev_alt_roam=x[38],
                )
            )
        )

        out_cols = [
            "msisdn",  # 1
            "weekstart",  # 2
            "fea_monthly_spend_voice_pattern_salary_1_5_over_03m",  # 3
            "fea_monthly_spend_voice_pattern_salary_11_15_over_03m",  # 4
            "fea_monthly_spend_voice_pattern_salary_21_25_over_03m",  # 5
            "fea_monthly_spend_voice_pattern_salary_26_31_over_03m",  # 6
            "fea_monthly_spend_data_pattern_salary_1_5_over_03m",  # 7
            "fea_monthly_spend_data_pattern_salary_11_15_over_03m",  # 8
            "fea_monthly_spend_data_pattern_salary_21_25_over_03m",  # 9
            "fea_monthly_spend_data_pattern_salary_26_31_over_03m",  # 10
            "fea_monthly_spend_sms_pattern_salary_1_5_over_03m",  # 11
            "fea_monthly_spend_sms_pattern_salary_11_15_over_03m",  # 12
            "fea_monthly_spend_sms_pattern_salary_21_25_over_03m",  # 13
            "fea_monthly_spend_sms_pattern_salary_26_31_over_03m",  # 14
            "fea_monthly_spend_roam_pattern_salary_1_5_over_03m",  # 15
            "fea_monthly_spend_roam_pattern_salary_11_15_over_03m",  # 16
            "fea_monthly_spend_roam_pattern_salary_21_25_over_03m",  # 17
            "fea_monthly_spend_roam_pattern_salary_26_31_over_03m",  # 18
        ]

        out = fea_revenue_salary(
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
            ]
            for i in out.collect()
        ]

        assert sorted(out_list) == [
            # Case A) Sanity test - all salary distribution in across 1_5, 11_15, 21_25, 26_31
            [
                "111",
                "2019-10-07",
                1.0,
                0.0,
                0.0,
                0.0,
                1.0,
                0.0,
                0.0,
                0.0,
                1.0,
                0.0,
                0.0,
                0.0,
                1.0,
                0.0,
                0.0,
                0.0,
            ],
            [
                "111",
                "2019-10-14",
                0.0,
                0.5,
                0.0,
                0.0,
                0.0,
                0.5,
                0.0,
                0.0,
                0.0,
                0.5,
                0.0,
                0.0,
                0.0,
                0.5,
                0.0,
                0.0,
            ],
            [
                "111",
                "2019-10-21",
                0.0,
                0.0,
                0.25,
                0.0,
                0.0,
                0.0,
                0.25,
                0.0,
                0.0,
                0.0,
                0.25,
                0.0,
                0.0,
                0.0,
                0.25,
                0.0,
            ],
            [
                "111",
                "2019-10-28",
                0.0,
                0.0,
                0.0,
                0.2,
                0.0,
                0.0,
                0.0,
                0.2,
                0.0,
                0.0,
                0.0,
                0.2,
                0.0,
                0.0,
                0.0,
                0.2,
            ],
            # Case B) Sanity test - salary distribution only 1_5 and 26_31
            [
                "222",
                "2019-10-07",
                1.0,
                0.0,
                0.0,
                0.0,
                1.0,
                0.0,
                0.0,
                0.0,
                1.0,
                0.0,
                0.0,
                0.0,
                1.0,
                0.0,
                0.0,
                0.0,
            ],
            [
                "222",
                "2019-10-14",
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
            ],
            [
                "222",
                "2019-10-21",
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
            ],
            [
                "222",
                "2019-10-28",
                0.0,
                0.0,
                0.0,
                0.5,
                0.0,
                0.0,
                0.0,
                0.5,
                0.0,
                0.0,
                0.0,
                0.5,
                0.0,
                0.0,
                0.0,
                0.5,
            ],
            # Case C) Edge case - salary distribution 11_15, 21_25, 26_31 after null week
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
            ],
            [
                "333",
                "2019-10-14",
                0.0,
                1.0,
                0.0,
                0.0,
                0.0,
                1.0,
                0.0,
                0.0,
                0.0,
                1.0,
                0.0,
                0.0,
                0.0,
                1.0,
                0.0,
                0.0,
            ],
            [
                "333",
                "2019-10-21",
                0.0,
                0.0,
                0.5,
                0.0,
                0.0,
                0.0,
                0.5,
                0.0,
                0.0,
                0.0,
                0.5,
                0.0,
                0.0,
                0.0,
                0.5,
                0.0,
            ],
            [
                "333",
                "2019-10-28",
                0.0,
                0.0,
                0.0,
                0.25,
                0.0,
                0.0,
                0.0,
                0.25,
                0.0,
                0.0,
                0.0,
                0.25,
                0.0,
                0.0,
                0.0,
                0.25,
            ],
        ]