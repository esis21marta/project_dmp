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

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._01_aggregation.recharge.create_account_balance_weekly import (
    _weekly_aggregation as create_weekly_account_balance,
)
from src.dmp.pipelines.dmp._01_aggregation.recharge.create_digital_recharge_weekly import (
    _weekly_aggregation as create_digital_recharge_weekly,
)
from src.dmp.pipelines.dmp._01_aggregation.recharge.create_recharge_weekly import (
    _weekly_aggregation as create_recharge_weekly,
)
from src.dmp.pipelines.dmp._02_primary.recharge.recharge_fill_time_series import (
    fill_time_series_recharge,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestRechargeAggregate:
    def test_sanity_test_create_recharge_weekly(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        df_rech_daily_abt_dd = sc.parallelize(
            [
                # Sanity test - make sure weekstart aggregated correctly
                ("111", "2019-10-01 10:00:00", 1, 100, "2019-10-01 10:00:00", "user-1"),
                ("222", "2019-10-01 10:00:00", 2, 200, "2019-10-01 10:00:00", "user-1"),
                # Sanity test - make sure weekstart aggregated correctly
                ("111", "2019-10-02 10:00:00", 3, 300, "2019-10-02 10:00:00", "user-2"),
                ("222", "2019-10-02 10:00:00", 4, 400, "2019-10-02 10:00:00", "user-2"),
            ]
        )

        df_rech_daily_abt_dd = spark_session.createDataFrame(
            df_rech_daily_abt_dd.map(
                lambda x: Row(
                    msisdn=x[0],
                    event_date=x[1],
                    tot_amt=x[2],
                    tot_trx=x[3],
                    load_ts=x[4],
                    load_user=x[5],
                )
            )
        )

        out = create_recharge_weekly(df_recharge_daily=df_rech_daily_abt_dd)
        out_list = [
            [i[0], i[1].strftime("%Y-%m-%d"), i[2], i[3]] for i in out.collect()
        ]

        assert sorted(out_list) == [
            ["111", "2019-10-07", 4, 400],
            ["222", "2019-10-07", 6, 600],
        ]

    def test_sanity_test_create_digital_recharge_weekly(
        self, spark_session: SparkSession
    ):
        df_rech_urp = spark_session.createDataFrame(
            data=[
                ["111", datetime.date(2019, 10, 1), 100],
                ["222", datetime.date(2019, 10, 1), 200],
                ["111", datetime.date(2019, 10, 2), 300],
                ["222", datetime.date(2019, 10, 2), 400],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("event_date", DateType(), False),
                    StructField("rech", LongType(), True),
                ]
            ),
        )

        actual_agg_df = create_digital_recharge_weekly(df_rech_urp=df_rech_urp)
        expected_agg_df = spark_session.createDataFrame(
            data=[
                ["111", datetime.date(2019, 10, 7), 400],
                ["222", datetime.date(2019, 10, 7), 600],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("tot_amt_digi", LongType(), True),
                ]
            ),
        )
        assert_df_frame_equal(actual_agg_df, expected_agg_df)

    def test_sanity_test_create_weekly_balance_table(self, spark_session: SparkSession):
        df_ocs_bal = spark_session.createDataFrame(
            data=[
                [
                    datetime.date(2019, 8, 31),
                    datetime.date(2019, 8, 31),
                    100,
                    "channel-a",
                ],  # Older date
                [
                    datetime.date(2019, 9, 30),
                    datetime.date(2019, 9, 30),
                    5000,
                    "channel-c",
                ],
                [
                    datetime.date(2019, 10, 1),
                    datetime.date(2019, 10, 1),
                    100,
                    "channel-a",
                ],
                [
                    datetime.date(2019, 10, 2),
                    datetime.date(2019, 10, 2),
                    100000,
                    "channel-a",
                ],
                [
                    datetime.date(2019, 10, 3),
                    datetime.date(2019, 10, 3),
                    51000,
                    "channel-b",
                ],
                [
                    datetime.date(2019, 10, 4),
                    datetime.date(2019, 10, 4),
                    21000,
                    "channel-c",
                ],
                [
                    datetime.date(2019, 10, 5),
                    datetime.date(2019, 10, 5),
                    11000,
                    "channel-c",
                ],
                [
                    datetime.date(2019, 10, 6),
                    datetime.date(2019, 10, 6),
                    -100,
                    "channel-c",
                ],
            ],
            schema=StructType(
                [
                    StructField("event_date", DateType(), False),
                    StructField("timestamp_1", DateType(), True),
                    StructField("account_balance", LongType(), True),
                    StructField("pay_channel", StringType(), True),
                ]
            ),
        )

        df_cb_pre_dd = spark_session.createDataFrame(
            data=[
                ["111", datetime.date(2019, 8, 31), "channel-a"],  # Older date
                ["111", datetime.date(2019, 9, 30), "channel-c"],
                ["111", datetime.date(2019, 10, 1), "channel-a"],
                ["111", datetime.date(2019, 10, 2), "channel-a"],
                ["111", datetime.date(2019, 10, 3), "channel-b"],
                ["111", datetime.date(2019, 10, 4), "channel-c"],
                ["111", datetime.date(2019, 10, 5), "channel-c"],
                ["111", datetime.date(2019, 10, 6), "channel-c"],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("event_date", DateType(), False),
                    StructField("paychannel", StringType(), True),
                ]
            ),
        )

        df_abt_rech_daily = spark_session.createDataFrame(
            data=[
                ("111", datetime.date(2019, 10, 2)),
                # handle duplicate entries on recharge daily
                ("111", datetime.date(2019, 10, 2)),
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("event_date", DateType(), False),
                ]
            ),
        )

        df_scaffoling_msisdn_week = spark_session.createDataFrame(
            data=[
                ["111", datetime.date(2019, 10, 7)],
                ["222", datetime.date(2019, 10, 7)],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                ]
            ),
        )

        out = create_weekly_account_balance(
            df_ocs_bal=df_ocs_bal,
            df_cb_pre_dd=df_cb_pre_dd,
            df_abt_rech_daily=df_abt_rech_daily,
        )
        actual_agg_df = fill_time_series_recharge(
            df_weekly_scaffold=df_scaffoling_msisdn_week, df_data=out
        )
        expected_output = [
            [
                "111",
                datetime.date(2019, 10, 7),
                1,
                -100,
                26857.14285714286,
                36830.415743099125,
                14,
                4,
                4,
                3,
                2,
                2,
                1,
                1,
                2,
                5000,
                datetime.date(2019, 10, 5),
                datetime.date(2019, 10, 5),
                datetime.date(2019, 10, 4),
                datetime.date(2019, 10, 3),
                datetime.date(2019, 10, 6),
                datetime.date(2019, 10, 6),
                datetime.date(2019, 10, 6),
                datetime.date(2019, 10, 6),
                100.0,
            ],
            [
                "222",
                datetime.date(2019, 10, 7),
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
                None,
                None,
                None,
                None,
                None,
                None,
            ],
        ]
        expected_agg_df = spark_session.createDataFrame(
            data=expected_output,
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("zero_or_neg_count", LongType(), True),
                    StructField("account_balance_min", LongType(), True),
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
                    StructField("current_balance", LongType(), True),
                    StructField("max_date_with_bal_more_than_5000", DateType(), True),
                    StructField("max_date_with_bal_more_than_10000", DateType(), True),
                    StructField("max_date_with_bal_more_than_20000", DateType(), True),
                    StructField("max_date_with_bal_more_than_50000", DateType(), True),
                    StructField("max_date_with_bal_5000_or_less", DateType(), True),
                    StructField("max_date_with_bal_10000_or_less", DateType(), True),
                    StructField("max_date_with_bal_20000_or_less", DateType(), True),
                    StructField("max_date_with_bal_50000_or_less", DateType(), True),
                    StructField("avg_last_bal_before_rech", DoubleType(), True),
                ]
            ),
        )

        assert_df_frame_equal(actual_agg_df, expected_agg_df)
