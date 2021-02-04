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

import pyspark.sql.functions as f
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import from_unixtime, unix_timestamp

from src.dmp.pipelines.dmp._01_aggregation.handset.create_handset_weekly_data import (
    _weekly_aggregation,
)


@mock.patch(
    "src.dmp.pipelines.dmp._01_aggregation.handset.create_handset_weekly_data.get_start_date",
    return_value=datetime.date(2018, 10, 10),
    autospec=True,
)
@mock.patch(
    "src.dmp.pipelines.dmp._01_aggregation.handset.create_handset_weekly_data.get_end_date",
    return_value=datetime.date(2020, 10, 10),
    autospec=True,
)
class TestCreateHandsetWeeklyData:
    def test_create_handset_weekly_data(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        sc = spark_session.sparkContext

        df_handsets_dd = sc.parallelize(
            [
                # Case A) Happy case - one handset per week
                ("111", "tac-1000", "2019-10-01 10:00:00", "2019-10-01"),
                # Case B) Two handsets in a single week
                ("111", "tac-2000", "2019-10-10 10:00:00", "2019-10-01"),
                ("111", "tac-3000", "2019-10-11 10:00:00", "2019-10-01"),
            ]
        )

        df_handsets_dd = spark_session.createDataFrame(
            df_handsets_dd.map(
                lambda x: Row(msisdn=x[0], imei=x[1], trx_date=x[2], event_date=x[3])
            )
        ).select(
            "msisdn",
            "imei",
            from_unixtime(unix_timestamp(f.col("trx_date"))).alias("trx_date"),
        )

        df_device_dim = sc.parallelize(
            [
                ("tac-1000", "manufacturer-1", "marketname-1", "devicetype-1"),
                ("tac-2000", "manufacturer-2", "marketname-2", "devicetype-2"),
                ("tac-3000", "manufacturer-3", "marketname-3", "devicetype-3"),
            ]
        )

        df_device_dim = spark_session.createDataFrame(
            df_device_dim.map(
                lambda x: Row(
                    tac=x[0], manufacturer=x[1], market_name=x[2], device_type=x[3]
                )
            )
        )

        out = _weekly_aggregation(
            df_handset_dd=df_handsets_dd, df_handset_lookup=df_device_dim
        )
        out = [
            [i[0], i[1].strftime("%Y-%m-%d"), i[2], i[3], i[4], i[5]]
            for i in out.collect()
        ]

        assert sorted(out) == [
            # Case A) Happy case - one handset per week
            [
                "111",
                "2019-10-07",
                ["manufacturer-1"],
                ["tac-1000"],
                ["devicetype-1"],
                ["marketname-1"],
            ],
            # Case B) Two handsets in a single week
            [
                "111",
                "2019-10-14",
                ["manufacturer-2", "manufacturer-3"],
                ["tac-2000", "tac-3000"],
                ["devicetype-3", "devicetype-2"],
                ["marketname-2", "marketname-3"],
            ],
        ]
