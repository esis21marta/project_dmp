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

from src.dmp.pipelines.dmp._04_features.revenue.fea_revenue_spend_pattern import (
    fea_revenue_spend_pattern,
)


class TestFeatureRevenueSpendPattern:
    config_feature = yaml.load(open("conf/dmp/training/parameters_feature.yml"))[
        "config_feature"
    ]

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.revenue.fea_revenue_spend_pattern.get_start_date",
        return_value=datetime.date(2018, 10, 10),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.revenue.fea_revenue_spend_pattern.get_end_date",
        return_value=datetime.date(2020, 10, 10),
        autospec=True,
    )
    def test_fea_revenue_spend_pattern(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        sc = spark_session.sparkContext

        df_recharge_weekly = sc.parallelize(
            [
                ["111", "2019-10-07", 100],
                ["111", "2019-10-21", 100],
                ["111", "2019-10-28", 200],
                ["111", "2019-11-04", 200],
            ]
        )

        df_recharge_weekly = spark_session.createDataFrame(
            df_recharge_weekly.map(
                lambda x: Row(msisdn=x[0], weekstart=x[1], tot_amt=x[2],)
            )
        )

        df_revenue_weekly = sc.parallelize(
            [
                # Case A.1) Sanity test - ratio payu/pkg all with values
                # Case A.2) Sanity test - ratio roam/total roam all with values
                [
                    "111",
                    "2019-10-07",
                    200,
                    200,
                    200,
                    400,
                    100,
                    100,
                    100,
                    100,
                    400,
                    100,
                    100,
                    100,
                    100,
                    "2019-10-07",
                    "2019-10-07",
                    "2019-10-07",
                    7,
                    7,
                    7,
                    10,
                    10,
                    10,
                    10,
                    10,
                    10,
                    10,
                    10,
                    10,
                    10,
                ],
                # Case B.1) Sanity test - ratio payu/pkg with 0 values on package
                # Case B.2) Sanity test - ratio roam/total roam with 0 values on roam
                [
                    "222",
                    "2019-10-14",
                    0,
                    0,
                    50,
                    50,
                    0,
                    50,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    "2019-10-14",
                    "2019-10-14",
                    "2019-10-14",
                    7,
                    7,
                    7,
                    7,
                    7,
                    7,
                    7,
                    7,
                    7,
                    7,
                    7,
                    7,
                    7,
                ],
                # Case C) Edge case - all 0/nulls until next week
                [
                    "333",
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
                    "2019-10-28",
                    200,
                    200,
                    200,
                    400,
                    100,
                    100,
                    100,
                    100,
                    400,
                    100,
                    100,
                    100,
                    100,
                    "2019-10-28",
                    "2019-10-28",
                    "2019-10-28",
                    7,
                    7,
                    7,
                    100,
                    8,
                    8,
                    8,
                    8,
                    10,
                    10,
                    10,
                    10,
                    10,
                ],
                [
                    "333",
                    "2019-11-04",
                    200,
                    200,
                    200,
                    0,
                    100,
                    100,
                    100,
                    100,
                    0,
                    100,
                    100,
                    100,
                    100,
                    "2019-11-04",
                    "2019-11-04",
                    "2019-11-04",
                    7,
                    7,
                    7,
                    100,
                    8,
                    8,
                    8,
                    8,
                    10,
                    10,
                    10,
                    10,
                    10,
                ],
                [
                    "333",
                    "2019-11-11",
                    200,
                    200,
                    200,
                    10,
                    100,
                    100,
                    100,
                    10,
                    0,
                    100,
                    100,
                    100,
                    100,
                    "2019-11-11",
                    "2019-11-11",
                    "2019-11-11",
                    7,
                    7,
                    7,
                    100,
                    8,
                    8,
                    8,
                    8,
                    10,
                    10,
                    10,
                    10,
                    10,
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
                    rev_payu_voice=x[6],
                    rev_payu_sms=x[7],
                    rev_payu_data=x[8],
                    rev_payu_roam=x[9],
                    rev_pkg_tot=x[10],
                    rev_pkg_voice=x[11],
                    rev_pkg_sms=x[12],
                    rev_pkg_data=x[13],
                    rev_pkg_roam=x[14],
                    max_date_payu=x[15],
                    max_date_pkg=x[16],
                    max_date_tot=x[17],
                    days_since_last_rev_payu=x[18],
                    days_since_last_rev_pkg=x[19],
                    days_since_last_rev_tot=x[20],
                    rev_alt_voice=x[21],
                    rev_alt_data=x[22],
                    rev_alt_sms=x[23],
                    rev_alt_roam=x[24],
                    rev_alt_total=x[25],
                    rev_alt_digital=x[26],
                    rev_alt_voice_pkg=x[27],
                    rev_alt_sms_pkg=x[28],
                    rev_alt_data_pkg=x[29],
                    rev_alt_roam_pkg=x[30],
                )
            )
        )

        out_cols = [
            "msisdn",  # 1
            "weekstart",  # 2
            "fea_ratio_voice_payu_by_pkg_avg_03m",  # 3
            "fea_ratio_data_payu_by_pkg_avg_03m",  # 4
            "fea_ratio_sms_payu_by_pkg_avg_03m",  # 5
            "fea_ratio_roam_payu_by_pkg_avg_03m",  # 6
            "fea_ratio_roam_by_tot_avg_03m",  # 7
            "fea_rev_weeks_since_last_rev_total",  # 8
            "fea_rev_weeks_since_last_rev_payu_tot",  # 9
            "fea_rev_weeks_since_last_rev_pkg_tot",  # 10
        ]

        out = fea_revenue_spend_pattern(
            df_revenue_weekly, df_recharge_weekly, self.config_feature, "all", []
        ).select(out_cols)

        out_list = [
            [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9]]
            for i in out.collect()
        ]

        assert sorted(out_list) == [
            # Case A.1) Sanity test - ratio payu/pkg will be equals to 1.0
            # Case A.2) Sanity test - ratio roam/total will be equals to 0.25 --> 1.0 / 4.0
            ["111", "2019-10-07", 1.0, 1.0, 1.0, 1.0, 0.25, 0, 0, 0],
            # Case B.1) Sanity test - ratio payu/pkg will be None (divided by zero)
            # Case B.2) Sanity test - ratio roam/total will be equals to 0.0 --> 0.0 / 0.0
            ["222", "2019-10-14", None, None, None, None, 0.0, 0, 0, 99999],
            # Case C) Edge case - all 0/nulls until next week
            ["333", "2019-10-21", None, None, None, None, None, 99999, 99999, 99999],
            ["333", "2019-10-28", 1.0, 1.0, 1.0, 1.0, 0.25, 0, 0, 0],
            ["333", "2019-11-04", 1.0, 1.0, 1.0, 1.0, 0.5, 1, 1, 1],
            ["333", "2019-11-11", 1.0, 1.0, 1.0, 0.7, 0.6296296296296297, 0, 0, 2],
        ]
