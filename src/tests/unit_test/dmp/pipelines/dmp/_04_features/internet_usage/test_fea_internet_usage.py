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

from src.dmp.pipelines.dmp._04_features.internet_usage.fea_internet_usage import (
    compute_day_night_features,
    compute_days_with_data_usage_above_threshold,
    compute_rolling_window_calculations,
    compute_uniq_payload_area_data_usage,
    compute_weekday_weekend_usage,
)


@mock.patch(
    "src.dmp.pipelines.dmp._04_features.internet_usage.fea_internet_usage.get_start_date",
    return_value=datetime.date(2018, 10, 10),
    autospec=True,
)
@mock.patch(
    "src.dmp.pipelines.dmp._04_features.internet_usage.fea_internet_usage.get_end_date",
    return_value=datetime.date(2020, 10, 10),
    autospec=True,
)
class TestFeatureInternetUsage:
    config_feature = yaml.load(open("conf/dmp/training/parameters_feature.yml"))[
        "config_feature"
    ]

    def test_compute_rolling_window_calculations(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        sc = spark_session.sparkContext

        df = sc.parallelize(
            [
                # Case A) Sanity test - 1 row with usage, 1 row without usage
                (
                    "111",
                    "2019-10-07",
                    300,
                    1,
                    100,
                    100,
                    100,
                    2,
                    5,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                ),
                ("111", "2019-10-14", 0, 0, 0, 0, 0, 2, 5, 1, 1, 1, 1, 1, 1, 1, 1),
                ("111", "2019-10-21", 0, 0, 0, 0, 0, 2, 5, 1, 1, 1, 1, 1, 1, 1, 1),
                # Case B) Sanity test - single row only with no usage
                ("222", "2019-10-07", 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1),
                ("222", "2019-10-14", None, 0, 0, 0, 5, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1),
                ("222", "2019-10-21", None, 0, 0, 0, 7, 0, 7, 1, 1, 1, 1, 1, 1, 1, 1),
                ("222", "2019-10-28", 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1),
            ]
        )

        df = spark_session.createDataFrame(
            df.map(
                lambda x: Row(
                    msisdn=x[0],
                    weekstart=x[1],
                    tot_kb=x[2],
                    tot_trx=x[3],
                    vol_data_2g_kb=x[4],
                    vol_data_3g_kb=x[5],
                    vol_data_4g_kb=x[6],
                    count_zero_usage_days=x[7],
                    count_non_zero_usage_days=x[8],
                    stddev_total_data_usage_day=x[9],
                    stddev_4g_data_usage_day=x[10],
                    min_vol_data_tot_kb_day=x[11],
                    med_vol_data_tot_kb_day=x[12],
                    max_vol_data_tot_kb_day=x[13],
                    min_vol_data_4g_kb_day=x[14],
                    med_vol_data_4g_kb_day=x[15],
                    max_vol_data_4g_kb_day=x[16],
                )
            )
        )

        out_cols = [
            "msisdn",
            "weekstart",
            "fea_int_usage_zero_usage_days_03m",
            "fea_int_usage_non_zero_usage_days_03m",
            "fea_int_usage_zero_usage_count_to_usage_count_ratio_03m",
            "fea_int_usage_tot_kb_data_usage_01m",
            "fea_int_usage_daily_avg_kb_data_usage_03m",
            "fea_int_usage_2g_kb_data_usage_01m",
            "fea_int_usage_3g_kb_data_usage_01m",
            "fea_int_usage_4g_kb_data_usage_01m",
            "fea_int_usage_weeks_since_last_internet_usage",
        ]

        out = compute_rolling_window_calculations(
            df, self.config_feature, "all", []
        ).select(out_cols)
        out_list = [
            [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10]]
            for i in out.collect()
        ]

        assert sorted(out_list) == [
            # Case A) Sanity test - all features calculated correctly
            ["111", "2019-10-07", 2, 5, 0.4, 300, 3.2967032967032965, 100, 100, 100, 0],
            [
                "111",
                "2019-10-14",
                4,
                10,
                0.4,
                300,
                3.2967032967032965,
                100,
                100,
                100,
                1,
            ],
            [
                "111",
                "2019-10-21",
                6,
                15,
                0.4,
                300,
                3.2967032967032965,
                100,
                100,
                100,
                2,
            ],
            # Case B) Sanity test - ratio default to negative when undefined value (e.g. 1/0)
            ["222", "2019-10-07", 0, 0, -1.0, 0, 0.0, 0, 0, 0, 99999],
            ["222", "2019-10-14", 0, 0, -1.0, 0, 0.0, 0, 0, 5, 99999],
            ["222", "2019-10-21", 0, 7, 0.0, 0, 0.0, 0, 0, 12, 99999],
            ["222", "2019-10-28", 0, 7, 0.0, 1, 0.01098901098901099, 0, 0, 12, 0],
        ]

    def test_compute_weekday_weekend_usage(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        sc = spark_session.sparkContext

        df = sc.parallelize(
            [
                # Case A) Sanity test - usage, multiple rows
                ("111", "2019-10-07", 100, 200, 50, 50, 0, 100, 50, 50),
                ("111", "2019-10-14", 300, 400, 100, 100, 100, 200, 50, 150),
                ("111", "2019-10-21", 500, 600, 200, 100, 200, 200, 200, 200),
                # Case B) Sanity test - single row, no usage
                ("222", "2019-10-28", 0, 0, 0, 0, 0, 0, 0, 0),
            ]
        )

        df = spark_session.createDataFrame(
            df.map(
                lambda x: Row(
                    msisdn=x[0],
                    weekstart=x[1],
                    tot_kb_weekday=x[2],
                    tot_kb_weekend=x[3],
                    vol_data_4g_kb_weekday=x[4],
                    vol_data_3g_kb_weekday=x[5],
                    vol_data_2g_kb_weekday=x[6],
                    vol_data_4g_kb_weekend=x[7],
                    vol_data_3g_kb_weekend=x[8],
                    vol_data_2g_kb_weekend=x[9],
                )
            )
        )

        out_cols = [
            "msisdn",
            "weekstart",
            "tot_kb_weekday",
            "tot_kb_weekend",
            "fea_int_usage_weekday_kb_usage_01m",
            "fea_int_usage_weekend_kb_usage_01m",
            "fea_int_usage_tot_kb_data_usage_4g_weekday_01w",
            "fea_int_usage_tot_kb_data_usage_3g_weekday_01w",
            "fea_int_usage_tot_kb_data_usage_2g_weekday_01w",
            "fea_int_usage_tot_kb_data_usage_4g_weekend_01w",
            "fea_int_usage_tot_kb_data_usage_3g_weekend_01w",
            "fea_int_usage_tot_kb_data_usage_2g_weekend_01w",
        ]

        out = compute_weekday_weekend_usage(
            df, self.config_feature["internet_usage"]
        ).select(out_cols)
        out_list = [
            [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10], i[11]]
            for i in out.collect()
        ]

        assert sorted(out_list) == [
            ["111", "2019-10-07", 100, 200, 100, 200, 50, 50, 0, 100, 50, 50],
            ["111", "2019-10-14", 300, 400, 400, 600, 100, 100, 100, 200, 50, 150],
            ["111", "2019-10-21", 500, 600, 900, 1200, 200, 100, 200, 200, 200, 200],
            ["222", "2019-10-28", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        ]

    def test_compute_uniq_payload_area_data_usage(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        sc = spark_session.sparkContext

        df = sc.parallelize(
            [
                # Case A) Sanity test - usage, multiple subdistrict
                (
                    "111",
                    "2019-10-07",
                    ["Cengkareng", "Mampang", "Senayan", "Cengkareng"],
                ),
                ("111", "2019-10-14", ["Cengkareng", "Kalideres", "Gambir"]),
                ("111", "2019-10-21", ["Senayan", "Senayan"]),
                # Case B) Sanity test - single row, no subdistrict
                ("222", "2019-10-28", []),
            ]
        )

        df = spark_session.createDataFrame(
            df.map(
                lambda x: Row(
                    msisdn=x[0], weekstart=x[1], sub_district_list_data_usage=x[2]
                )
            )
        )

        out_cols = [
            "msisdn",
            "weekstart",
            "fea_int_usage_num_unique_payload_sub_district_01w",
            "fea_int_usage_num_unique_payload_sub_district_01m",
        ]

        out = compute_uniq_payload_area_data_usage(
            df, self.config_feature["internet_usage"]
        ).select(out_cols)
        out_list = [[i[0], i[1], i[2], i[3]] for i in out.collect()]

        assert sorted(out_list) == [
            ["111", "2019-10-07", 3, 3],
            ["111", "2019-10-14", 3, 5],
            ["111", "2019-10-21", 1, 5],
            ["222", "2019-10-28", 0, 0],
        ]

    def test_compute_days_with_data_usage_above_threshold(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        sc = spark_session.sparkContext

        df = sc.parallelize(
            [
                # Case A) Sanity test - usage
                ("111", "2019-10-07", 4, 2, 1, 2, 1, 0),
                ("111", "2019-10-14", 6, 3, 2, 3, 2, 1),
                ("111", "2019-10-21", 1, 0, 0, 1, 0, 0),
                # Case B) Sanity test - single row, no usage
                ("222", "2019-10-28", 0, 0, 0, 0, 0, 0),
            ]
        )

        df = spark_session.createDataFrame(
            df.map(
                lambda x: Row(
                    msisdn=x[0],
                    weekstart=x[1],
                    days_with_payload_total_above_2mb=x[2],
                    days_with_payload_total_above_50mb=x[3],
                    days_with_payload_total_above_500mb=x[4],
                    days_with_payload_4g_above_2mb=x[5],
                    days_with_payload_4g_above_50mb=x[6],
                    days_with_payload_4g_above_500mb=x[7],
                )
            )
        ).select(
            "msisdn",
            "weekstart",
            "days_with_payload_total_above_2mb",
            "days_with_payload_total_above_50mb",
            "days_with_payload_total_above_500mb",
            "days_with_payload_4g_above_2mb",
            "days_with_payload_4g_above_50mb",
            "days_with_payload_4g_above_500mb",
        )

        out_cols = [
            "msisdn",
            "weekstart",
            "fea_int_usage_num_days_with_total_data_usage_above_2mb_01w",
            "fea_int_usage_num_days_with_total_data_usage_above_2mb_01m",
            "fea_int_usage_num_days_with_total_data_usage_above_50mb_01w",
            "fea_int_usage_num_days_with_total_data_usage_above_50mb_01m",
            "fea_int_usage_num_days_with_total_data_usage_above_500mb_01w",
            "fea_int_usage_num_days_with_total_data_usage_above_500mb_01m",
            "fea_int_usage_num_days_with_4g_data_usage_above_2mb_01w",
            "fea_int_usage_num_days_with_4g_data_usage_above_2mb_01m",
            "fea_int_usage_num_days_with_4g_data_usage_above_50mb_01w",
            "fea_int_usage_num_days_with_4g_data_usage_above_50mb_01m",
            "fea_int_usage_num_days_with_4g_data_usage_above_500mb_01w",
            "fea_int_usage_num_days_with_4g_data_usage_above_500mb_01m",
        ]

        out = compute_days_with_data_usage_above_threshold(
            df, self.config_feature["internet_usage"]
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
            ]
            for i in out.collect()
        ]

        assert sorted(out_list) == [
            ["111", "2019-10-07", 4, 4, 2, 2, 1, 1, 2, 2, 1, 1, 0, 0],
            ["111", "2019-10-14", 6, 10, 3, 5, 2, 3, 3, 5, 2, 3, 1, 1],
            ["111", "2019-10-21", 1, 11, 0, 5, 0, 3, 1, 6, 0, 3, 0, 1],
            ["222", "2019-10-28", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        ]

    def test_compute_day_night_features(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        sc = spark_session.sparkContext

        df = sc.parallelize(
            [
                # Case A) Sanity test - usage
                ("111", "2019-10-07", 10, 2, 1, 5, 5, 1),
                # Case B) Sanity test - three rows, 1 row no usage
                ("222", "2019-10-14", 10, 2, 1, 5, 5, 1),
                ("222", "2019-10-21", 0, 0, 0, 0, 0, 0),
                ("222", "2019-10-28", 10, 2, 1, 5, 5, 1),
            ]
        )

        df = spark_session.createDataFrame(
            df.map(
                lambda x: Row(
                    msisdn=x[0],
                    weekstart=x[1],
                    tot_kb_day=x[2],
                    tot_trx_day=x[3],
                    count_day_sessions=x[4],
                    tot_kb_night=x[5],
                    tot_trx_night=x[6],
                    count_night_sessions=x[7],
                )
            )
        )

        out_cols = [
            "msisdn",
            "weekstart",
            "fea_int_usage_tot_kb_day_01m",
            "fea_int_usage_tot_trx_day_01m",
            "fea_int_usage_count_day_sessions_01m",
            "fea_int_usage_tot_kb_night_01m",
            "fea_int_usage_tot_trx_night_01m",
            "fea_int_usage_count_night_sessions_01m",
        ]

        out = compute_day_night_features(
            df, self.config_feature["internet_usage"]
        ).select(out_cols)
        out_list = [
            [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7]] for i in out.collect()
        ]

        assert sorted(out_list) == [
            ["111", "2019-10-07", 10, 2, 1, 5, 5, 1],
            ["222", "2019-10-14", 10, 2, 1, 5, 5, 1],
            ["222", "2019-10-21", 10, 2, 1, 5, 5, 1],
            ["222", "2019-10-28", 20, 4, 2, 10, 10, 2],
        ]
