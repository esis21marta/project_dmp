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

import pyspark.sql.functions as f
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import from_unixtime, unix_timestamp

from src.dmp.pipelines.dmp._01_aggregation.internet_usage.agg_internet_usage_to_weekly import (
    _daily_aggregation as aggregate_daily,
)


class TestInternetUsageAggregate:
    def test_aggregate_daily(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        df = sc.parallelize(
            [
                # Case A) Happy case - internet usage w/ weekday and weekend
                (
                    "111",
                    "2019-10-06 10:00:00",
                    "Cengkareng",
                    1000,
                    5,
                    200,
                    300,
                    500,
                    10,
                    2,
                    1,
                    5,
                    5,
                    1,
                ),  # 2g 3g 4g
                (
                    "111",
                    "2019-10-07 12:00:00",
                    "Cengkareng",
                    2000,
                    2,
                    0,
                    1000,
                    1000,
                    10,
                    2,
                    1,
                    5,
                    5,
                    1,
                ),  # no 2g
                (
                    "111",
                    "2019-10-07 14:00:00",
                    "Mampang",
                    3000,
                    3,
                    0,
                    0,
                    3000,
                    10,
                    2,
                    1,
                    5,
                    5,
                    1,
                ),  # 4g weekday
                # Case B) No internet usage (rows exist but data doesn't)
                (
                    "222",
                    "2019-10-07 14:00:00",
                    "",
                    0,
                    1,
                    0,
                    0,
                    0,
                    10,
                    2,
                    1,
                    5,
                    5,
                    1,
                ),  # 2g
                (
                    "222",
                    "2019-10-07 14:00:00",
                    "",
                    0,
                    2,
                    0,
                    0,
                    0,
                    10,
                    2,
                    1,
                    5,
                    5,
                    1,
                ),  # 3g
                (
                    "222",
                    "2019-10-07 14:00:00",
                    "",
                    0,
                    3,
                    0,
                    0,
                    0,
                    10,
                    2,
                    1,
                    5,
                    5,
                    1,
                ),  # 4g
            ]
        )

        df = spark_session.createDataFrame(
            df.map(
                lambda x: Row(
                    msisdn=x[0],
                    event_date=x[1],
                    city=x[2],
                    tot_kb=int(x[3]),
                    tot_trx=int(x[4]),
                    vol_data_2g_kb=int(x[5]),
                    vol_data_3g_kb=int(x[6]),
                    vol_data_4g_kb=int(x[7]),
                    kb_day=int(x[8]),
                    trx_day=int(x[9]),
                    cnt_session_day=int(x[10]),
                    kb_night=int(x[11]),
                    trx_night=int(x[12]),
                    cnt_session_night=int(x[13]),
                )
            )
        ).select(
            "msisdn",
            from_unixtime(unix_timestamp(f.col("event_date"), "yyyy-MM-dd")).alias(
                "event_date"
            ),
            "city",
            "tot_kb",
            "tot_trx",
            "vol_data_2g_kb",
            "vol_data_3g_kb",
            "vol_data_4g_kb",
            "kb_day",
            "trx_day",
            "cnt_session_day",
            "kb_night",
            "trx_night",
            "cnt_session_night",
        )

        out = aggregate_daily(df)

        out_cols = [
            "msisdn",
            "event_date",
            "weekstart",
            "tot_kb",
            "tot_trx",
            "vol_data_2g_kb",
            "vol_data_3g_kb",
            "vol_data_4g_kb",
            "tot_kb_weekday",
            "tot_kb_weekend",
            "sub_district_list",
            "kb_day",
            "trx_day",
            "cnt_session_day",
            "kb_night",
            "trx_night",
            "cnt_session_night",
        ]

        out_list = [
            [
                i[0],
                i[1],
                i[2].strftime("%Y-%m-%d"),
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
            ]
            for i in out.select(out_cols).collect()
        ]

        assert sorted(out_list) == [
            # Case A) Expect 2 rows (one of them is a group of 2 rows) with distribution across 2g,3g,4g and weekday/weekend
            [
                "111",
                "2019-10-06 00:00:00",
                "2019-10-07",
                1000.0,
                5,
                200,
                300,
                500,
                0.0,
                1000,
                ["Cengkareng"],
                10,
                2,
                1,
                5,
                5,
                1,
            ],
            [
                "111",
                "2019-10-07 00:00:00",
                "2019-10-14",
                5000.0,
                5,
                0.0,
                1000.0,
                4000.0,
                5000.0,
                0.0,
                ["Mampang", "Cengkareng"],
                20,
                4,
                2,
                10,
                10,
                2,
            ],
            # Case B) Expect 1 row with no values for 2g,3g,4g and weekday/weekend
            [
                "222",
                "2019-10-07 00:00:00",
                "2019-10-14",
                0.0,
                6,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                [""],
                30,
                6,
                3,
                15,
                15,
                3,
            ],
        ]
