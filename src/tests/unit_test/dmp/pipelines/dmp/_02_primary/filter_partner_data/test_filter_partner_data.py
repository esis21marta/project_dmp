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

from src.dmp.pipelines.dmp._02_primary.filter_partner_data.filter_partner_data import (
    filter_partner_data,
)


class TestFilterPatnerData:
    def test_filter_patner_data(self, spark_session):
        """
        When filter_data is set to False in parameters.yml, it will not filter Partner MSISDNs Data
        """
        sc = spark_session.sparkContext
        df_partner_msisdns = sc.parallelize([("123",), ("456",)])

        df_partner_msisdns = spark_session.createDataFrame(
            df_partner_msisdns.map(lambda x: Row(msisdn=x[0]))
        )

        df_data = sc.parallelize([("123",), ("456",), ("789",)])

        df_data = spark_session.createDataFrame(df_data.map(lambda x: Row(msisdn=x[0])))

        actual_result_df = filter_partner_data(
            df_partner_msisdn=df_partner_msisdns, df_data=df_data, filter_data=False
        )

        actual_result_list = [[i[0]] for i in actual_result_df.collect()]

        expected_result_list = [["123"], ["456"], ["789"]]

        assert sorted(actual_result_list) == sorted(expected_result_list)

    def test_filter_patner_data_training(self, spark_session):
        """
        When filter_data is set to True in parameters.yml, it will filter Partner MSISDNs Data only
        """
        sc = spark_session.sparkContext
        df_partner_msisdns = sc.parallelize([("123",), ("456",),])

        df_partner_msisdns = spark_session.createDataFrame(
            df_partner_msisdns.map(lambda x: Row(msisdn=x[0]))
        )

        df_data = sc.parallelize([("123",), ("456",), ("789",)])

        df_data = spark_session.createDataFrame(df_data.map(lambda x: Row(msisdn=x[0])))

        actual_result_df = filter_partner_data(
            df_partner_msisdn=df_partner_msisdns, df_data=df_data, filter_data=True
        )

        actual_result_list = [[i[0]] for i in actual_result_df.collect()]

        expected_result_list = [["123"], ["456"]]

        assert sorted(actual_result_list) == sorted(expected_result_list)
