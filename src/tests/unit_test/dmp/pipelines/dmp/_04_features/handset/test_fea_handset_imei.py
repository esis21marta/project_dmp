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

import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from src.dmp.pipelines.dmp._04_features.handset.fea_handset_imei import fea_handset_imei
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestFeaHandsetImei(object):
    """
    Test Case for Handset imei
    """

    # File Paths
    base_path = os.path.join(
        os.getcwd(), "src/tests/unit_test/dmp/pipelines/dmp/data/handset"
    )
    handset_scaffold = "file://{}/handset_scaffold.csv".format(base_path)
    handset_imei_user_count = "file://{}/handset_imei_user_count.csv".format(base_path)
    fea_msisdn_imei = "file://{}/fea_handset_imei.csv".format(base_path)

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.handset.fea_handset_imei.get_start_date",
        return_value=datetime.date(2020, 1, 6),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.handset.fea_handset_imei.get_end_date",
        return_value=datetime.date(2020, 4, 30),
        autospec=True,
    )
    def test_fea_handset_imei(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ) -> None:
        """
        Testing Internet Usage Weekly Aggregation
        """

        # Read Sample Input Data
        df_handset_imei_user_count = spark_session.read.csv(
            self.handset_imei_user_count, header=True
        )
        df_handset_scaffold = spark_session.read.csv(
            self.handset_scaffold, header=True, sep="|"
        )
        df_handset_scaffold = df_handset_scaffold.withColumn(
            "imeis", f.split(f.regexp_replace(f.col("imeis"), "[\\[\\]]", ""), ",")
        )

        # Read Sample Output Data
        df_fea_msisdn_imei = spark_session.read.csv(self.fea_msisdn_imei, header=True)

        df_res = fea_handset_imei(
            df_handset_scaffold,
            df_handset_imei_user_count,
            sla_date_parameter=8,
            feature_mode="all",
            required_output_features=[],
        )

        assert_df_frame_equal(df_res, df_fea_msisdn_imei)
