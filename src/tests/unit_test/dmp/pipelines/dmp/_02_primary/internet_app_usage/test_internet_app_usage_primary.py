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

from src.dmp.pipelines.dmp._02_primary.filter_partner_data.filter_partner_data import (
    filter_partner_data,
)
from src.dmp.pipelines.dmp._02_primary.internet_app_usage.fill_time_series import (
    fill_bcp_time_series,
)
from src.dmp.pipelines.dmp._02_primary.scaffold.fill_time_series import fill_time_series


class TestInternetAppUsagePrimary(object):
    """
    Test Case for Internet App Usage Primary
    """

    # File Paths
    base_path = os.path.join(
        os.getcwd(), "src/tests/unit_test/dmp/pipelines/dmp/data/internet_app_usage"
    )
    partner = "file://{}/partner.csv".format(base_path)
    weekly_scaffold = "file://{}/weekly_scaffold.csv".format(base_path)
    app_usage_data = "file://{}/app_usage_weekly_agg.parquet".format(base_path)
    int_usage_data = "file://{}/int_usage_weekly_agg.parquet".format(base_path)
    app_usage_partner_data = "file://{}/app_usage_weekly_agg_partner.parquet".format(
        base_path
    )
    int_usage_partner_data = "file://{}/int_usage_weekly_agg_partner.parquet".format(
        base_path
    )
    app_usage_scaffold_data = "file://{}/app_usage_weekly_agg_time_series_filled.parquet".format(
        base_path
    )
    int_usage_scaffold_data = "file://{}/int_usage_weekly_agg_time_series_filled.parquet".format(
        base_path
    )

    def test_app_usage_primary(self, spark_session: SparkSession) -> None:
        """
        Testing App Usage Primary
        """
        # Read Sample Input Data
        df_partner = spark_session.read.csv(path=self.partner, header=True)
        df_weekly_scaffold = spark_session.read.csv(
            path=self.weekly_scaffold, header=True
        )
        df_app_usage_data = spark_session.read.parquet(self.app_usage_data)

        # Read Sample Output Data
        df_app_usage_partner_data = spark_session.read.parquet(
            self.app_usage_partner_data
        )
        df_app_usage_scaffold_data = spark_session.read.parquet(
            self.app_usage_scaffold_data
        )

        # Call Function
        df_partner_res = filter_partner_data(
            df_partner_msisdn=df_partner, df_data=df_app_usage_data, filter_data=True,
        )
        df_scaffold_res = fill_bcp_time_series(
            df_weekly_scaffold=df_weekly_scaffold, df_data=df_app_usage_partner_data,
        )

        # Compare Sample Output Data & Function Output Data
        df_partner_comp = (df_app_usage_partner_data.exceptAll(df_partner_res)).union(
            df_partner_res.exceptAll(df_app_usage_partner_data)
        )

        df_scaffold_comp = (
            df_app_usage_scaffold_data.exceptAll(df_scaffold_res)
        ).union(df_scaffold_res.exceptAll(df_app_usage_scaffold_data))

        assert df_partner_comp.count() == 0

        assert df_scaffold_comp.count() == 0

    def test_int_usage_primary(self, spark_session: SparkSession) -> None:
        """
        Testing Internet Usage Primary
        """
        # Read Sample Input Data
        df_partner = spark_session.read.csv(path=self.partner, header=True)
        df_weekly_scaffold = spark_session.read.csv(
            path=self.weekly_scaffold, header=True
        )
        df_int_usage_data = spark_session.read.parquet(self.int_usage_data)

        # Read Sample Output Data
        df_int_usage_partner_data = spark_session.read.parquet(
            self.int_usage_partner_data
        )
        df_int_usage_scaffold_data = spark_session.read.parquet(
            self.int_usage_scaffold_data
        )

        # Call Function
        df_partner_res = filter_partner_data(
            df_partner_msisdn=df_partner, df_data=df_int_usage_data, filter_data=True,
        )
        df_scaffold_res = fill_time_series(key="msisdn")(
            df_weekly_scaffold=df_weekly_scaffold, df_data=df_int_usage_partner_data,
        )

        # Compare Sample Output Data & Function Output Data
        df_partner_comp = (df_int_usage_partner_data.exceptAll(df_partner_res)).union(
            df_partner_res.exceptAll(df_int_usage_partner_data)
        )

        df_scaffold_comp = (
            df_int_usage_scaffold_data.exceptAll(df_scaffold_res)
        ).union(df_scaffold_res.exceptAll(df_int_usage_scaffold_data))

        assert df_partner_comp.count() == 0

        assert df_scaffold_comp.count() == 0
