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

import os

from pyspark.sql import SparkSession

from src.dmp.pipelines.dmp._01_aggregation.customer_profile.create_weekly_sellthru import (
    _weekly_aggregation as create_weekly_sellthru,
)


class TestSellthruAggregation(object):
    """
    Test Case for Sellthru Aggregation
    """

    # File Paths
    base_path = os.path.join(
        os.getcwd(), "src/tests/unit_test/dmp/pipelines/dmp/data/customer_profile"
    )
    raw_data = "file://{}/agg_l1_digipos_sp_migration.csv".format(base_path)
    expected_data = "file://{}/create_weekly_sellthru.csv".format(base_path)

    def test_sellthru_weekly(self, spark_session: SparkSession) -> None:
        """
        Testing Sellthru Weekly Aggregation
        """
        # Read Sample Input Data
        df_raw_data = spark_session.read.csv(path=self.raw_data, header=True)

        # Read Sample Output Data
        df_expected_data = spark_session.read.csv(path=self.expected_data, header=True)

        # Call Function
        df_res = create_weekly_sellthru(df_sellthru=df_raw_data)

        # Compare Sample Output Data & Function Output Data
        df_comp = (df_expected_data.exceptAll(df_res)).union(
            df_res.exceptAll(df_expected_data)
        )

        assert df_comp.count() == 0
