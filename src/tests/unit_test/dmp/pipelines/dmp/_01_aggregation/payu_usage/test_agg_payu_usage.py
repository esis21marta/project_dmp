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

from src.dmp.pipelines.dmp._01_aggregation.payu_usage.create_payu_usage_weekly import (
    _weekly_aggregation as weekly_payu_usage,
)


class TestPayuUsageAggregate:
    def test_weekly_payu_usage(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        df = sc.parallelize(
            [
                # Case A) Payu Usage
                ("111", "2019-10-27 10:00:00", 1000, 700, 300),
                ("111", "2019-10-28 13:00:00", 2000, 200, 1800),
                ("111", "2019-10-28 15:00:00", 500, 400, 100),
                # Case B) No Payu Usage
                ("222", "2019-10-28 14:00:00", 0, 0, 0),
            ]
        )

        df = spark_session.createDataFrame(
            df.map(
                lambda x: Row(
                    msisdn=x[0],
                    event_date=x[1],
                    vol_data_kb=int(x[2]),
                    vol_data_day_kb=int(x[3]),
                    vol_data_night_kb=int(x[4]),
                )
            )
        ).select(
            "msisdn",
            from_unixtime(unix_timestamp(f.col("event_date"), "yyyy-MM-dd")).alias(
                "event_date"
            ),
            "vol_data_kb",
            "vol_data_day_kb",
            "vol_data_night_kb",
        )

        out = weekly_payu_usage(df)

        out_cols = [
            "msisdn",
            "weekstart",
            "vol_data_kb_weekends",
            "vol_data_kb_weekdays",
            "vol_data_day_kb_weekends",
            "vol_data_day_kb_weekdays",
            "vol_data_night_kb_weekends",
            "vol_data_night_kb_weekdays",
        ]

        out_list = [
            [i[0], i[1].strftime("%Y-%m-%d"), i[2], i[3], i[4], i[5], i[6], i[7],]
            for i in out.select(out_cols).collect()
        ]

        assert sorted(out_list) == [
            ["111", "2019-10-28", 1000, None, 700, None, 300, None],
            ["111", "2019-11-04", None, 2500, None, 600, None, 1900],
            ["222", "2019-11-04", None, 0, None, 0, None, 0],
        ]
