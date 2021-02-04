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

from src.dmp.pipelines.dmp._04_features.customer_profile.fea_customer_expiry import (
    fea_customer_expiry,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestCustomerExpiryFeatures(object):
    """
    Test Case for Customer Expiry Features
    """

    # File Paths
    base_path = os.path.join(
        os.getcwd(), "src/tests/unit_test/dmp/pipelines/dmp/data/customer_profile"
    )
    customer_expiry_scaffold_data = "file://{}/customer_expiry_partner.csv".format(
        base_path
    )
    fea_customer_expiry_data = "file://{}/fea_customer_expiry.parquet".format(base_path)

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.customer_profile.fea_customer_expiry.get_start_date",
        return_value=datetime.date(2020, 4, 6),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.customer_profile.fea_customer_expiry.get_end_date",
        return_value=datetime.date(2020, 4, 20),
        autospec=True,
    )
    def test_fea_customer_expiry(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ) -> None:
        """
        Testing Customer Expiry Date Features
        """
        # Read Sample Input Data
        df_customer_expiry_data = spark_session.read.csv(
            path=self.customer_expiry_scaffold_data, header=True
        )

        # Read Sample Output Data
        expected_feature_df = spark_session.read.parquet(self.fea_customer_expiry_data)

        # Call Function
        actual_feature_df = fea_customer_expiry(
            df_customer_expiry=df_customer_expiry_data,
            feature_mode="all",
            required_output_features=[],
        )

        assert_df_frame_equal(actual_feature_df, expected_feature_df)
