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

from src.dmp.pipelines.dmp._01_aggregation.text_messaging.text_messaging_weekly import (
    _weekly_aggregation as create_text_messaging_weekly,
)


class TestTextMessagingWeeklyAggregate:
    def test_create_text_messaging_weekly(self, spark_session: SparkSession):
        sc = spark_session.sparkContext
        df_abt_usage_mss_dd = sc.parallelize(
            [
                ("2020-02-02", "6281122", "05_sms_in", 5),
                ("2020-02-02", "6281122", "02_call_in", 6),
                ("2020-02-02", "6281123", "01_call_out", 7),
                ("2020-02-02", "6281123", "05_sms_in", 8),
                ("2020-02-02", "6281123", "04_sms_out", 9),
            ]
        )
        df_abt_usage_mss_dd = spark_session.createDataFrame(
            df_abt_usage_mss_dd.map(
                lambda x: Row(
                    event_date=x[0], anumber=x[1], calltype=x[2], total_trx=x[3],
                )
            )
        )
        res = create_text_messaging_weekly(df_abt_usage_mss_dd)
        expected = [
            ["6281122", datetime.date(2020, 2, 3), 5, 0, 5, 6, 0, 6],
            ["6281123", datetime.date(2020, 2, 3), 8, 9, 17, 0, 7, 7],
        ]
        res_list = []

        for i in res.collect():
            res_list.append([j for j in i])

        assert sorted(res_list) == sorted(expected)
