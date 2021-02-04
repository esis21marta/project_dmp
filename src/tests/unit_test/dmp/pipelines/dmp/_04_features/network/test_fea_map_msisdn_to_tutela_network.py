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
import os
from unittest import mock

from pyspark.sql import SparkSession

from src.dmp.pipelines.dmp._04_features.network.fea_map_msisdn_to_tutela_network import (
    fea_map_msisdn_to_tutela_network,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestMapMSISDNtoTutelaFeatures(object):
    """
    Test Case for Map MSISDN to Tutela Features
    """

    # File Paths
    base_path = os.path.join(
        os.getcwd(), "src/tests/unit_test/dmp/pipelines/dmp/data/network"
    )
    l2_customer_profile_weekly_data = "file://{}/customer_profile_weekly_data.csv".format(
        base_path
    )
    l4_fea_network_tutela_data = "file://{}/fea_network_tutela.csv".format(base_path)
    fea_map_msisdn_to_tutela_network_data = "file://{}/map_msisdn_to_tutela_network.csv".format(
        base_path
    )

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.network.fea_map_msisdn_to_tutela_network.get_start_date",
        return_value=datetime.date(2020, 4, 6),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.network.fea_map_msisdn_to_tutela_network.get_end_date",
        return_value=datetime.date(2020, 4, 20),
        autospec=True,
    )
    def test_fea_map_msisdn_to_tutela_network(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ) -> None:
        """
        Testing Customer Expiry Date Features
        """
        # Read Sample Input Data
        df_l2_customer_profile_weekly_data = spark_session.read.csv(
            path=self.l2_customer_profile_weekly_data, header=True
        )
        df_l4_fea_network_tutela_data = spark_session.read.csv(
            path=self.l4_fea_network_tutela_data, header=True
        )

        # Read Sample Output Data
        expected_feature_df = spark_session.read.csv(
            path=self.fea_map_msisdn_to_tutela_network_data, header=True
        )

        # Call Function
        actual_feature_df = fea_map_msisdn_to_tutela_network(
            fea_customer_profile=df_l2_customer_profile_weekly_data,
            fea_network_tutela=df_l4_fea_network_tutela_data,
        )

        assert_df_frame_equal(actual_feature_df, expected_feature_df)
