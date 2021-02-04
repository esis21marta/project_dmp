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
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._04_features.network.fea_network_tutela import (
    fea_network_tutela,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestFeaNetworkTutela(object):
    """
    Test Case for Internet App Usage Aggregation
    """

    # File Paths
    base_path = os.path.join(
        os.getcwd(), "src/tests/unit_test/dmp/pipelines/dmp/data/network"
    )

    tutela_agg = "file://{}/tutela_agg.csv".format(base_path)

    tutela_features = "file://{}/tutela_features.csv".format(base_path)

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.network.fea_network_tutela.get_start_date",
        return_value=datetime.date(2020, 3, 1),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.network.fea_network_tutela.get_end_date",
        return_value=datetime.date(2020, 4, 30),
        autospec=True,
    )
    def test_fea_network_tutela(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ) -> None:
        """
        Testing Internet Usage Weekly Aggregation
        """

        input_schema = StructType(
            [
                StructField("location", StringType(), True),
                StructField("weekstart", DateType(), True),
                StructField("competitor_2g_3g_download_p10", DoubleType(), True),
                StructField("competitor_2g_3g_download_p25", DoubleType(), True),
                StructField(
                    "competitor_2g_3g_avg_download_throughput", DoubleType(), True
                ),
                StructField("competitor_2g_3g_download_p75", DoubleType(), True),
                StructField("competitor_2g_3g_download_p90", DoubleType(), True),
                StructField("competitor_2g_3g_upload_p10", DoubleType(), True),
                StructField("competitor_2g_3g_upload_p25", DoubleType(), True),
                StructField(
                    "competitor_2g_3g_avg_upload_throughput", DoubleType(), True
                ),
                StructField("competitor_2g_3g_upload_p75", DoubleType(), True),
                StructField("competitor_2g_3g_upload_p90", DoubleType(), True),
                StructField("competitor_2g_3g_latency_p10", DoubleType(), True),
                StructField("competitor_2g_3g_latency_p25", DoubleType(), True),
                StructField("competitor_2g_3g_avg_latency", DoubleType(), True),
                StructField("competitor_2g_3g_latency_p75", DoubleType(), True),
                StructField("competitor_2g_3g_latency_p90", DoubleType(), True),
                StructField(
                    "competitor_2g_3g_coverage_cell_bandwidth", DoubleType(), True
                ),
                StructField("competitor_2g_3g_coverage_km2", DoubleType(), True),
                StructField("competitor_2g_3g_device_share", DoubleType(), True),
                StructField("competitor_2g_3g_enodeb_share", DoubleType(), True),
                StructField(
                    "competitor_2g_3g_avg_excellent_quality", DoubleType(), True
                ),
                StructField("competitor_2g_3g_avg_hd_quality", DoubleType(), True),
                StructField("competitor_2g_3g_avg_good_quality", DoubleType(), True),
                StructField("competitor_2g_3g_avg_game_parameter", DoubleType(), True),
                StructField("competitor_2g_3g_avg_video_score", DoubleType(), True),
                StructField("competitor_2g_3g_signal_good", DoubleType(), True),
                StructField("competitor_2g_3g_signal_fair", DoubleType(), True),
                StructField("competitor_2g_3g_signal_bad", DoubleType(), True),
                StructField("competitor_2g_3g_sample", DoubleType(), True),
                StructField("competitor_4g_download_p10", DoubleType(), True),
                StructField("competitor_4g_download_p25", DoubleType(), True),
                StructField(
                    "competitor_4g_avg_download_throughput", DoubleType(), True
                ),
                StructField("competitor_4g_download_p75", DoubleType(), True),
                StructField("competitor_4g_download_p90", DoubleType(), True),
                StructField("competitor_4g_upload_p10", DoubleType(), True),
                StructField("competitor_4g_upload_p25", DoubleType(), True),
                StructField("competitor_4g_avg_upload_throughput", DoubleType(), True),
                StructField("competitor_4g_upload_p75", DoubleType(), True),
                StructField("competitor_4g_upload_p90", DoubleType(), True),
                StructField("competitor_4g_latency_p10", DoubleType(), True),
                StructField("competitor_4g_latency_p25", DoubleType(), True),
                StructField("competitor_4g_avg_latency", DoubleType(), True),
                StructField("competitor_4g_latency_p75", DoubleType(), True),
                StructField("competitor_4g_latency_p90", DoubleType(), True),
                StructField(
                    "competitor_4g_coverage_cell_bandwidth", DoubleType(), True
                ),
                StructField("competitor_4g_coverage_km2", DoubleType(), True),
                StructField("competitor_4g_device_share", DoubleType(), True),
                StructField("competitor_4g_enodeb_share", DoubleType(), True),
                StructField("competitor_4g_avg_excellent_quality", DoubleType(), True),
                StructField("competitor_4g_avg_hd_quality", DoubleType(), True),
                StructField("competitor_4g_avg_good_quality", DoubleType(), True),
                StructField("competitor_4g_avg_game_parameter", DoubleType(), True),
                StructField("competitor_4g_avg_video_score", DoubleType(), True),
                StructField("competitor_4g_signal_good", DoubleType(), True),
                StructField("competitor_4g_signal_fair", DoubleType(), True),
                StructField("competitor_4g_signal_bad", DoubleType(), True),
                StructField("competitor_4g_sample", DoubleType(), True),
                StructField("tsel_2g_3g_download_p10", DoubleType(), True),
                StructField("tsel_2g_3g_download_p25", DoubleType(), True),
                StructField("tsel_2g_3g_avg_download_throughput", DoubleType(), True),
                StructField("tsel_2g_3g_download_p75", DoubleType(), True),
                StructField("tsel_2g_3g_download_p90", DoubleType(), True),
                StructField("tsel_2g_3g_upload_p10", DoubleType(), True),
                StructField("tsel_2g_3g_upload_p25", DoubleType(), True),
                StructField("tsel_2g_3g_avg_upload_throughput", DoubleType(), True),
                StructField("tsel_2g_3g_upload_p75", DoubleType(), True),
                StructField("tsel_2g_3g_upload_p90", DoubleType(), True),
                StructField("tsel_2g_3g_latency_p10", DoubleType(), True),
                StructField("tsel_2g_3g_latency_p25", DoubleType(), True),
                StructField("tsel_2g_3g_avg_latency", DoubleType(), True),
                StructField("tsel_2g_3g_latency_p75", DoubleType(), True),
                StructField("tsel_2g_3g_latency_p90", DoubleType(), True),
                StructField("tsel_2g_3g_coverage_cell_bandwidth", DoubleType(), True),
                StructField("tsel_2g_3g_coverage_km2", DoubleType(), True),
                StructField("tsel_2g_3g_device_share", DoubleType(), True),
                StructField("tsel_2g_3g_enodeb_share", DoubleType(), True),
                StructField("tsel_2g_3g_avg_excellent_quality", DoubleType(), True),
                StructField("tsel_2g_3g_avg_hd_quality", DoubleType(), True),
                StructField("tsel_2g_3g_avg_good_quality", DoubleType(), True),
                StructField("tsel_2g_3g_avg_game_parameter", DoubleType(), True),
                StructField("tsel_2g_3g_avg_video_score", DoubleType(), True),
                StructField("tsel_2g_3g_signal_good", DoubleType(), True),
                StructField("tsel_2g_3g_signal_fair", DoubleType(), True),
                StructField("tsel_2g_3g_signal_bad", DoubleType(), True),
                StructField("tsel_2g_3g_sample", DoubleType(), True),
                StructField("tsel_4g_download_p10", DoubleType(), True),
                StructField("tsel_4g_download_p25", DoubleType(), True),
                StructField("tsel_4g_avg_download_throughput", DoubleType(), True),
                StructField("tsel_4g_download_p75", DoubleType(), True),
                StructField("tsel_4g_download_p90", DoubleType(), True),
                StructField("tsel_4g_upload_p10", DoubleType(), True),
                StructField("tsel_4g_upload_p25", DoubleType(), True),
                StructField("tsel_4g_avg_upload_throughput", DoubleType(), True),
                StructField("tsel_4g_upload_p75", DoubleType(), True),
                StructField("tsel_4g_upload_p90", DoubleType(), True),
                StructField("tsel_4g_latency_p10", DoubleType(), True),
                StructField("tsel_4g_latency_p25", DoubleType(), True),
                StructField("tsel_4g_avg_latency", DoubleType(), True),
                StructField("tsel_4g_latency_p75", DoubleType(), True),
                StructField("tsel_4g_latency_p90", DoubleType(), True),
                StructField("tsel_4g_coverage_cell_bandwidth", DoubleType(), True),
                StructField("tsel_4g_coverage_km2", DoubleType(), True),
                StructField("tsel_4g_device_share", DoubleType(), True),
                StructField("tsel_4g_enodeb_share", DoubleType(), True),
                StructField("tsel_4g_avg_excellent_quality", DoubleType(), True),
                StructField("tsel_4g_avg_hd_quality", DoubleType(), True),
                StructField("tsel_4g_avg_good_quality", DoubleType(), True),
                StructField("tsel_4g_avg_game_parameter", DoubleType(), True),
                StructField("tsel_4g_avg_video_score", DoubleType(), True),
                StructField("tsel_4g_signal_good", DoubleType(), True),
                StructField("tsel_4g_signal_fair", DoubleType(), True),
                StructField("tsel_4g_signal_bad", DoubleType(), True),
                StructField("tsel_4g_sample", DoubleType(), True),
                StructField("4g_device_share_gap", DoubleType(), True),
                StructField("4g_enodeb_share_gap", DoubleType(), True),
                StructField("4g_coverage_km2_gap", DoubleType(), True),
                StructField("4g_coverage_cell_bandwidth_gap", DoubleType(), True),
                StructField("4g_avg_download_throughput_gap", DoubleType(), True),
                StructField("4g_avg_latency_gap", DoubleType(), True),
                StructField("4g_avg_upload_throughput_gap", DoubleType(), True),
                StructField("2g_3g_device_share_gap", DoubleType(), True),
                StructField("2g_3g_coverage_km2_gap", DoubleType(), True),
                StructField("2g_3g_coverage_cell_bandwidth_gap", DoubleType(), True),
                StructField("2g_3g_avg_download_throughput_gap", DoubleType(), True),
                StructField("2g_3g_avg_upload_throughput_gap", DoubleType(), True),
                StructField("2g_3g_avg_latency_gap", DoubleType(), True),
            ]
        )

        # Read Sample Input Data
        tutela_agg_df = spark_session.read.csv(
            path=self.tutela_agg, header=True, schema=input_schema
        )

        output_schema = StructType(
            [
                StructField("location", StringType(), True),
                StructField("weekstart", DateType(), True),
                StructField(
                    "fea_network_tsel_4g_share_of_signal_fair_01m", DoubleType(), True
                ),
                StructField(
                    "fea_network_2g_3g_download_inflection_pctl_wrt_competitor_01m",
                    StringType(),
                    True,
                ),
                StructField(
                    "fea_network_2g_3g_latency_inflection_pctl_wrt_competitor_01m",
                    StringType(),
                    True,
                ),
                StructField(
                    "fea_network_tsel_2g_3g_share_of_signal_fair_01m",
                    DoubleType(),
                    True,
                ),
                StructField(
                    "fea_network_tsel_4g_share_of_signal_bad_01m", DoubleType(), True
                ),
                StructField(
                    "fea_network_2g_3g_avg_upload_throughput_gap_01m",
                    DoubleType(),
                    True,
                ),
                StructField("fea_network_tsel_4g_enodeb_share_03m", DoubleType(), True),
                StructField(
                    "fea_network_2g_3g_avg_download_throughput_gap_01m",
                    DoubleType(),
                    True,
                ),
                StructField(
                    "fea_network_2g_3g_avg_latency_gap_01m", DoubleType(), True
                ),
                StructField(
                    "fea_network_tsel_4g_avg_good_quality_01m", DoubleType(), True
                ),
                StructField(
                    "fea_network_2g_3g_device_share_gap_03m", DoubleType(), True
                ),
                StructField(
                    "fea_network_4g_latency_inflection_pctl_wrt_competitor_01m",
                    StringType(),
                    True,
                ),
                StructField(
                    "fea_network_tsel_4g_avg_upload_throughput_higher_than_competitor_01m",
                    IntegerType(),
                    True,
                ),
                StructField(
                    "fea_network_tsel_2g_3g_device_share_03m", DoubleType(), True
                ),
                StructField(
                    "fea_network_tsel_4g_avg_game_parameter_01m", DoubleType(), True
                ),
                StructField(
                    "fea_network_tsel_2g_3g_avg_excellent_quality_01m",
                    DoubleType(),
                    True,
                ),
                StructField(
                    "fea_network_4g_download_inflection_pctl_wrt_competitor_01m",
                    StringType(),
                    True,
                ),
                StructField(
                    "fea_network_4g_avg_upload_throughput_gap_01m", DoubleType(), True
                ),
                StructField(
                    "fea_network_tsel_2g_3g_avg_hd_quality_01m", DoubleType(), True
                ),
                StructField(
                    "fea_network_tsel_2g_3g_avg_game_parameter_01m", DoubleType(), True
                ),
                StructField(
                    "fea_network_tsel_2g_3g_avg_video_score_01m", DoubleType(), True
                ),
                StructField("fea_network_4g_coverage_km2_gap_03m", DoubleType(), True),
                StructField(
                    "fea_network_tsel_4g_avg_excellent_quality_01m", DoubleType(), True
                ),
                StructField(
                    "fea_network_tsel_4g_avg_latency_higher_than_competitor_01m",
                    IntegerType(),
                    True,
                ),
                StructField(
                    "fea_network_tsel_2g_3g_avg_upload_throughput_higher_than_competitor_01m",
                    IntegerType(),
                    True,
                ),
                StructField(
                    "fea_network_tsel_2g_3g_avg_latency_higher_than_competitor_01m",
                    IntegerType(),
                    True,
                ),
                StructField(
                    "fea_network_tsel_4g_avg_video_score_01m", DoubleType(), True
                ),
                StructField(
                    "fea_network_4g_coverage_cell_bandwidth_gap_03m", DoubleType(), True
                ),
                StructField(
                    "fea_network_tsel_2g_3g_avg_download_throughput_higher_than_competitor_01m",
                    IntegerType(),
                    True,
                ),
                StructField(
                    "fea_network_tsel_2g_3g_share_of_signal_bad_01m", DoubleType(), True
                ),
                StructField(
                    "fea_network_4g_upload_inflection_pctl_wrt_competitor_01m",
                    StringType(),
                    True,
                ),
                StructField(
                    "fea_network_2g_3g_coverage_cell_bandwidth_gap_03m",
                    DoubleType(),
                    True,
                ),
                StructField(
                    "fea_network_2g_3g_upload_inflection_pctl_wrt_competitor_01m",
                    StringType(),
                    True,
                ),
                StructField("fea_network_4g_avg_latency_gap_01m", DoubleType(), True),
                StructField(
                    "fea_network_2g_3g_coverage_km2_gap_03m", DoubleType(), True
                ),
                StructField(
                    "fea_network_tsel_4g_share_of_signal_good_01m", DoubleType(), True
                ),
                StructField(
                    "fea_network_tsel_2g_3g_avg_good_quality_01m", DoubleType(), True
                ),
                StructField("fea_network_4g_device_share_gap_03m", DoubleType(), True),
                StructField(
                    "fea_network_tsel_4g_avg_download_throughput_higher_than_competitor_01m",
                    IntegerType(),
                    True,
                ),
                StructField(
                    "fea_network_tsel_2g_3g_share_of_signal_good_01m",
                    DoubleType(),
                    True,
                ),
                StructField(
                    "fea_network_tsel_4g_avg_hd_quality_01m", DoubleType(), True
                ),
                StructField(
                    "fea_network_4g_avg_download_throughput_gap_01m", DoubleType(), True
                ),
                StructField("fea_network_4g_enodeb_share_gap_03m", DoubleType(), True),
                StructField("fea_network_tsel_4g_device_share_03m", DoubleType(), True),
            ]
        )

        # Read Sample Output Data
        df_tutela_features = spark_session.read.csv(
            path=self.tutela_features, header=True, schema=output_schema
        )

        df_res = fea_network_tutela(
            tutela_agg_df, feature_mode="all", required_output_features=[]
        )

        assert_df_frame_equal(df_res, df_tutela_features)
