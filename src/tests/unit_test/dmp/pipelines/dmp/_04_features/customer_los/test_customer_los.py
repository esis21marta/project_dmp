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

from pyspark.sql import Row, SparkSession

from src.dmp.pipelines.dmp._04_features.customer_los.fea_customer_los import (
    fea_customer_los,
)


@mock.patch(
    "src.dmp.pipelines.dmp._04_features.customer_los.fea_customer_los.get_start_date",
    return_value=datetime.date(2018, 10, 10),
    autospec=True,
)
@mock.patch(
    "src.dmp.pipelines.dmp._04_features.customer_los.fea_customer_los.get_end_date",
    return_value=datetime.date(2020, 10, 10),
    autospec=True,
)
class TestCustomerLosFeature:
    def test_fea_customer_los(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        sc = spark_session.sparkContext

        df_customer_los_agg = sc.parallelize(
            [("333", "2018-09-30", "2019-03-25"), ("444", "2018-09-09", "2019-03-25"),]
        )
        df_customer_los_agg = spark_session.createDataFrame(
            df_customer_los_agg.map(
                lambda x: Row(msisdn=x[0], activation_date=x[1], weekstart=x[2])
            )
        )

        df_msisdn_weekstart = sc.parallelize(
            [
                ("333", "2019-03-18"),
                ("333", "2019-03-25"),
                ("444", "2019-03-18"),
                ("444", "2019-03-25"),
            ]
        )
        df_msisdn_weekstart = spark_session.createDataFrame(
            df_msisdn_weekstart.map(lambda x: Row(msisdn=x[0], weekstart=x[1]))
        )

        out = fea_customer_los(
            df_customer_los_agg, df_msisdn_weekstart, "all", [],
        ).select("weekstart", "msisdn", "fea_los")

        out_list = [[i[0], i[1], i[2]] for i in out.collect()]

        assert sorted(out_list) == sorted(
            [
                ["2019-03-18", "444", 190],
                ["2019-03-25", "444", 197],
                ["2019-03-18", "333", 169],
                ["2019-03-25", "333", 176],
            ]
        )
