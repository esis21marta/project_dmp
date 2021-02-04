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

from pyspark.sql import Row, SparkSession

from src.dmp.pipelines.dmp._01_aggregation.customer_los.create_weekly_customer_los import (
    _weekly_aggregation,
    _weekly_aggregation_first_weekstart,
)


class TestAggCustomerLos:
    def test_weekly_aggregation(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        prev_df = sc.parallelize([("444", "2018-11-30", "2019-01-07"),])
        prev_df = spark_session.createDataFrame(
            prev_df.map(
                lambda x: Row(msisdn=x[0], activation_date=x[1], weekstart=x[2])
            )
        )

        cb_pre_post_dd = sc.parallelize(
            [
                # sanity check
                ("444", "2019-03-19", "2019-03-20", "postpaid"),
                ("555", "2019-03-21", "2019-03-22", "postpaid"),
                # ignore prepaid to postpaid migration activation_date
                ("222", "1999-10-23", "2019-03-22", "prepaid"),
                ("222", "2019-03-23", "2019-03-23", "postpaid"),
                # ignore activation_date if suddenly the activation date changes
                ("333", "2019-03-01", "2019-03-21", "postpaid"),
                ("333", "2019-03-21", "2019-03-22", "postpaid"),
            ]
        )
        cb_pre_post_dd = spark_session.createDataFrame(
            cb_pre_post_dd.map(
                lambda x: Row(
                    msisdn=x[0],
                    activation_date=x[1],
                    event_date=x[2],
                    prepost_flag=x[3],
                )
            )
        )

        out = _weekly_aggregation(cb_pre_post_dd, prev_df, weekstart="2019-03-25",)

        out_list = [[i[0], i[1]] for i in out.collect()]

        assert sorted(out_list) == sorted(
            [
                ["555", "2019-03-21"],
                ["444", "2018-11-30"],
                ["222", "1999-10-23"],
                ["333", "2019-03-01"],
            ]
        )

    def test_weekly_aggregation_first_weekstart(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        cb_pre_post_dd = sc.parallelize(
            [
                # sanity check
                ("444", "2019-03-19", "2019-03-20", "postpaid"),
                ("555", "2019-03-21", "2019-03-22", "postpaid"),
                # ignore prepaid to postpaid migration activation_date
                ("222", "1999-10-23", "2019-03-22", "prepaid"),
                ("222", "2019-03-23", "2019-03-23", "postpaid"),
                # ignore activation_date if suddenly the activation date changes
                ("333", "2019-03-01", "2019-03-21", "postpaid"),
                ("333", "2019-03-21", "2019-03-22", "postpaid"),
            ]
        )
        cb_pre_post_dd = spark_session.createDataFrame(
            cb_pre_post_dd.map(
                lambda x: Row(
                    msisdn=x[0],
                    activation_date=x[1],
                    event_date=x[2],
                    prepost_flag=x[3],
                )
            )
        )
        churn_period = 90

        out = _weekly_aggregation_first_weekstart(
            cb_pre_post_dd, "2019-03-25", churn_period
        )

        out_list = [[i[0], i[2]] for i in out.collect()]

        assert sorted(out_list) == sorted(
            [
                ["555", "2019-03-21"],
                ["444", "2019-03-19"],
                ["222", "1999-10-23"],
                ["333", "2019-03-01"],
            ]
        )
