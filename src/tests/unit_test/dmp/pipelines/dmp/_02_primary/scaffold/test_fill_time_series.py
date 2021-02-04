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

from pyspark.sql import Row

from src.dmp.pipelines.dmp._02_primary.scaffold.fill_time_series import fill_time_series


class TestFillTimeSeries:
    def test_fill_time_series(self, spark_session):
        sc = spark_session.sparkContext

        df_weekly_scaffold = sc.parallelize(
            [("321", "2019-10-07"), ("654", "2019-10-07"), ("987", "2019-10-14")]
        )

        df_weekly_scaffold = spark_session.createDataFrame(
            df_weekly_scaffold.map(lambda x: Row(msisdn=x[0], weekstart=x[1]))
        )

        df_data = sc.parallelize(
            [
                ("123", "2019-10-07"),
                ("456", "2019-10-07"),
                ("456", "2019-10-19"),
                ("789", "2019-10-14"),
            ]
        )

        df_data = spark_session.createDataFrame(
            df_data.map(lambda x: Row(msisdn=x[0], weekstart=x[1]))
        )

        actual_result_df = fill_time_series(key="msisdn")(
            df_weekly_scaffold=df_weekly_scaffold, df_data=df_data
        )
        actual_result_list = [
            [row.msisdn, row.weekstart] for row in actual_result_df.collect()
        ]

        expected_result_list = [
            ["654", "2019-10-07"],
            ["321", "2019-10-07"],
            ["987", "2019-10-14"],
        ]

        assert sorted(actual_result_list) == sorted(expected_result_list)
