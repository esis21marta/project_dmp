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

from src.dmp.pipelines.dmp._01_aggregation.customer_profile.create_weekly_outlet_dim import (
    _weekly_aggregation as create_weekly_outlet_dim,
)


class TestOutletDimAggregation(object):
    """
    Test Case for Outlet Dim Aggregation
    """

    # File Paths
    base_path = os.path.join(
        os.getcwd(), "src/tests/unit_test/dmp/pipelines/dmp/data/customer_profile"
    )
    l1_outlet_dim_dd_data = "file://{}/agg_l1_digipos_outlet_reference_dd.csv".format(
        base_path
    )
    create_weekly_outlet_data = "file://{}/create_weekly_outlet_data.csv".format(
        base_path
    )

    def test_weekly_outlet_dim(self, spark_session: SparkSession) -> None:
        """
        Testing Outlet Dim Weekly Aggregation
        """
        # Read Sample Input Data
        df_l1_outlet_dim_dd_data = spark_session.read.csv(
            path=self.l1_outlet_dim_dd_data, header=True
        )

        # Read Sample Output Data
        df_weekly_outlet_data = spark_session.read.csv(
            path=self.create_weekly_outlet_data, header=True
        )

        # Call Function
        df_res = create_weekly_outlet_dim(df_outlet=df_l1_outlet_dim_dd_data)

        # Compare Sample Output Data & Function Output Data
        df_comp = (df_weekly_outlet_data.exceptAll(df_res)).union(
            df_res.exceptAll(df_weekly_outlet_data)
        )

        assert df_comp.count() == 0
