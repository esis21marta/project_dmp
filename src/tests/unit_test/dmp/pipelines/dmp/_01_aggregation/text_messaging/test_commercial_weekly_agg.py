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
    ArrayType,
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._01_aggregation.text_messaging.commercial_weekly import (
    _weekly_aggregation as create_commercial_text_messaging_weekly,
)
from src.dmp.pipelines.dmp._01_aggregation.text_messaging.commercial_weekly import (
    convert_hexa_to_text,
    get_kredivo_msisdns,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestCommercialMessagingWeeklyAggregate:
    def test_convert_hexa_to_text(self, spark_session: SparkSession):
        sc = spark_session.sparkContext
        bnumber = sc.parallelize(
            [
                # sanity check
                ("46494647524?5550",),
                ("53686?706565",),
                ("4444",),
                ("5768617473417070",),
                # kredivo handling
                ("4;72656469766",),
                # Regular number
                ("6281112222220000",),
            ]
        )
        bnumber_df = spark_session.createDataFrame(
            bnumber.map(lambda x: Row(bnumber=x[0],))
        )
        res = convert_hexa_to_text(bnumber_df)
        expected = [
            ["46494647524?5550", "FIFGROUP"],
            ["53686?706565", "Shopee"],
            ["4444", "DD"],
            ["5768617473417070", "WhatsApp"],
            ["4;72656469766", "kredivo"],
            ["6281112222220000", "6281112222220000"],
        ]
        res_list = []
        for i in res.collect():
            res_list.append([j for j in i])

        assert sorted(res_list) == sorted(expected)

    def test_get_kredivo_msisdns(self, spark_session: SparkSession):
        comm_agg_df = spark_session.createDataFrame(
            data=[
                [
                    "6281122",
                    datetime.date(2020, 2, 3),
                    "credit lending companies",
                    ["kredivo"],
                    3,
                    0,
                ],
                [
                    "6281122",
                    datetime.date(2020, 1, 6),
                    "credit lending companies",
                    ["kredivo"],
                    1,
                    1,
                ],
                ["6281122", datetime.date(2020, 2, 10), "restaurants", ["dd"], 1, 0],
                [
                    "6281123",
                    datetime.date(2020, 2, 3),
                    "ecommerce shopping",
                    ["lazada", "shopee"],
                    5,
                    0,
                ],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("category", StringType(), True),
                    StructField("senders_07d", ArrayType(StringType(), True), True),
                    StructField("incoming_count_07d", LongType(), True),
                    StructField(
                        "count_txt_msg_incoming_government_tax", LongType(), True
                    ),
                ]
            ),
        )

        actual_df = get_kredivo_msisdns(comm_agg_df)
        expected_df = spark_session.createDataFrame(
            data=[["6281122",],],
            schema=StructType([StructField("msisdn", StringType(), False),]),
        )

        assert_df_frame_equal(actual_df, expected_df)

    def test_create_commercial_text_messaging_weekly(self, spark_session: SparkSession):
        sc = spark_session.sparkContext
        df_abt_usage_mss_dd = sc.parallelize(
            [
                # sanity check
                ("2020-02-02", "6281122", "4444", "05_sms_in", 1,),
                ("2020-02-02", "6281122", "5768617473417070", "05_sms_in", 3,),
                ("2020-02-02", "6281123", "6281112222220000", "05_sms_in", 1,),
                ("2020-02-02", "6281123", "53686?706565", "05_sms_in", 2,),
                # handle broken data
                ("2020-02-02", "6281123", "624<415:414441", "05_sms_in", 3,),
                ("2020-01-01", "6281123", "624554<494>41", "05_sms_in", 1,),
                # check if other than 05_sms_in are not count
                ("2020-02-02", "6281122", "5768617473417070", "04_sms_out", 1,),
                # check government tax count
                ("2020-01-01", "6281122", "626170656e6461", "05_sms_in", 1,),
            ]
        )
        df_comm_text_mapping = sc.parallelize(
            [
                ("bapenda", "government", "tax office",),
                ("shopee", "ecommerce shopping", None,),
                ("dd", "restaurants", None,),
                ("whatsapp", "messaging", None,),
                ("lazada", "ecommerce shopping", None,),
            ]
        )
        df_broken = sc.parallelize(
            [
                ("624554<494>41", "kulina", "other online services", None,),
                ("6242414>44=414>544150", "bankmantap", "banks", "test",),
            ]
        )

        df_abt_usage_mss_dd = spark_session.createDataFrame(
            df_abt_usage_mss_dd.map(
                lambda x: Row(
                    event_date=x[0],
                    anumber=x[1],
                    bnumber=x[2],
                    calltype=x[3],
                    total_trx=x[4],
                )
            )
        )
        df_comm_text_mapping = spark_session.createDataFrame(
            df_comm_text_mapping.map(
                lambda x: Row(sender=x[0], category=x[1], evalueserve_comments=x[2],)
            )
        )
        df_broken = spark_session.createDataFrame(
            df_broken.map(
                lambda x: Row(
                    bnumber=x[0], sender=x[1], category=x[2], evalueserve_comments=x[3],
                )
            )
        )
        res = create_commercial_text_messaging_weekly(
            df_abt_usage_mss_dd, df_comm_text_mapping, df_broken
        )

        expected = [
            ["6281122", datetime.date(2020, 2, 3), "messaging", ["whatsapp"], 3, 0],
            ["6281122", datetime.date(2020, 1, 6), "government", ["bapenda"], 1, 1],
            ["6281122", datetime.date(2020, 2, 3), "restaurants", ["dd"], 1, 0],
            [
                "6281123",
                datetime.date(2020, 2, 3),
                "ecommerce shopping",
                ["lazada", "shopee"],
                5,
                0,
            ],
            [
                "6281123",
                datetime.date(2020, 1, 6),
                "other online services",
                ["kulina"],
                1,
                0,
            ],
        ]

        res_list = []
        for i in res.collect():
            res_list.append([j for j in i])

        assert sorted(res_list) == sorted(expected)
