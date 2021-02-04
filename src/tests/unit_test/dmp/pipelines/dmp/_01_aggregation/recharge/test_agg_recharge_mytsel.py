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

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType

from src.dmp.pipelines.dmp._01_aggregation.recharge.create_mytsel_recharge_weekly import (
    _weekly_aggregation as create_weekly_mytsel_recharge,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestMyTselRechargeAggregate:
    def test_sanity_test_create_recharge_weekly(self, spark_session: SparkSession):

        df_mytsel_rech = spark_session.createDataFrame(
            data=[
                # Weekstart 1
                # msisdn    , rech  , trx   , channel   , store_location    , trx_date
                ["111", 150, 1, "xxx", "0", datetime.date(2020, 10, 1),],
                ["222", 80, 1, "yyy", "0", datetime.date(2020, 10, 2),],
                ["222", 40, 2, "xxx", "0", datetime.date(2020, 10, 4),],
                # Weekstart 2
                ["111", 100, 1, "zzz", "VA_BNI", datetime.date(2020, 10, 8),],
                ["111", 200, 1, "xxx", "VA_BNI", datetime.date(2020, 10, 7),],
                ["222", 200, 1, "yyy", "VA_MANDIRI", datetime.date(2020, 10, 9),],
                ["222", 100, 2, "xxx", "0", datetime.date(2020, 10, 5),],
                # Weekstart 3
                ["111", 200, 1, "abc", "CREDIT_CARD", datetime.date(2020, 10, 12),],
                ["111", 100, 1, "def", "0", datetime.date(2020, 10, 12),],
                ["111", 400, 2, "xxx", "CREDIT_CARD", datetime.date(2020, 10, 15),],
                ["333", 50, 1, "xxx", "0", datetime.date(2020, 10, 14),],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("rech", LongType(), True),
                    StructField("trx", LongType(), True),
                    StructField("channel", StringType(), True),
                    StructField("store_location", StringType(), True),
                    StructField("trx_date", DateType(), False),
                ]
            ),
        )

        actual_fea_df = create_weekly_mytsel_recharge(
            df_rech_mytsel=df_mytsel_rech, col_group_by=["msisdn", "store_location"]
        )

        expected_fea_df = spark_session.createDataFrame(
            data=[
                # Weekstart 1
                # msisdn    , store_location    , weekstart                 , tot_trx_sum       , tot_amt_sum
                ["111", "0", datetime.date(2020, 10, 5), 1, 150],
                ["222", "0", datetime.date(2020, 10, 5), 3, 120],
                # Weekstart 2
                ["111", "VA_BNI", datetime.date(2020, 10, 12), 2, 300],
                ["222", "VA_MANDIRI", datetime.date(2020, 10, 12), 1, 200],
                ["222", "0", datetime.date(2020, 10, 12), 2, 100],
                # Weekstart 3
                ["111", "CREDIT_CARD", datetime.date(2020, 10, 19), 3, 600],
                ["111", "0", datetime.date(2020, 10, 19), 1, 100],
                ["333", "0", datetime.date(2020, 10, 19), 1, 50],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("store_location", StringType(), False),
                    StructField("weekstart", DateType(), True),
                    StructField("tot_trx_sum", LongType(), True),
                    StructField("tot_amt_sum", LongType(), True),
                ]
            ),
        )

        assert_df_frame_equal(actual_fea_df, expected_fea_df)
