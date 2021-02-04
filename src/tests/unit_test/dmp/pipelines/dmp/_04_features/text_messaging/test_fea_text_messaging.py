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
from unittest import mock

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._04_features.text_messaging.fea_txt_msg_count import (
    fea_txt_msg_count,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestFeaTextMessaging:
    config_feature = yaml.load(open("conf/dmp/training/parameters_feature.yml"))[
        "config_feature"
    ]

    required_output_features = yaml.load(
        open("conf/dmp/training/parameters_feature.yml")
    )["output_features"]

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.text_messaging.fea_txt_msg_count.get_start_date",
        return_value=datetime.date(2018, 10, 10),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.text_messaging.fea_txt_msg_count.get_end_date",
        return_value=datetime.date(2020, 10, 10),
        autospec=True,
    )
    def test_fea_txt_msg_count(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        spark_session.sparkContext

        # Initialise test df
        txt_agg = spark_session.createDataFrame(
            data=[
                ["6281122", datetime.date(2020, 2, 3), 5, 0, 5, 6, 0, 6],
                ["6281123", datetime.date(2020, 2, 10), 8, 9, 17, 0, 7, 7],
                ["6281123", datetime.date(2020, 2, 17), 8, 0, 17, 0, 7, 7],
                ["6281123", datetime.date(2020, 2, 24), 8, None, 17, 0, 7, 7],
                ["6281123", datetime.date(2020, 3, 2), 8, 0, 17, 0, 7, 7],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField("weekstart", DateType(), True),
                    StructField("count_txt_msg_incoming", IntegerType(), True),
                    StructField("count_txt_msg_outgoing", IntegerType(), True),
                    StructField("count_txt_msg_all", IntegerType(), True),
                    StructField("count_voice_incoming", IntegerType(), True),
                    StructField("count_voice_outgoing", IntegerType(), True),
                    StructField("count_voice_all", IntegerType(), True),
                ]
            ),
        )

        # Execution
        actual_result_df = fea_txt_msg_count(
            txt_agg, self.config_feature, "all", self.required_output_features,
        )

        actual_result_df.show(10, False)

        expected_result_df = spark_session.createDataFrame(
            data=[
                [
                    "6281122",
                    datetime.date(2020, 2, 3),
                    5,
                    5,
                    None,
                    None,
                    0,
                    5,
                    5,
                    0,
                    0.45454545454545453,
                    0.45454545454545453,
                    None,
                    0.45454545454545453,
                    0,
                    5,
                    None,
                    0.45454545454545453,
                    5,
                    None,
                    5,
                    99999,
                    0,
                    0.45454545454545453,
                    0.45454545454545453,
                    None,
                    None,
                    0.45454545454545453,
                    0.45454545454545453,
                    None,
                    None,
                    5,
                    5,
                    None,
                    0.45454545454545453,
                    0,
                    None,
                    0,
                    0.45454545454545453,
                    5,
                    None,
                    5,
                    0.45454545454545453,
                    0.45454545454545453,
                    5,
                    0,
                ],
                [
                    "6281123",
                    datetime.date(2020, 2, 10),
                    8,
                    17,
                    None,
                    0.5625,
                    9,
                    8,
                    8,
                    9,
                    1.0,
                    0.7083333333333334,
                    None,
                    0.7083333333333334,
                    9,
                    8,
                    0.5625,
                    1.0,
                    8,
                    0.5625,
                    17,
                    0,
                    0,
                    0.7083333333333334,
                    0.7083333333333334,
                    None,
                    None,
                    0.7083333333333334,
                    0.7083333333333334,
                    None,
                    0.5625,
                    17,
                    17,
                    0.5625,
                    1.0,
                    9,
                    0.5625,
                    9,
                    1.0,
                    17,
                    None,
                    8,
                    1.0,
                    1.0,
                    17,
                    9,
                ],
                [
                    "6281123",
                    datetime.date(2020, 2, 17),
                    8,
                    34,
                    None,
                    0.391304347826087,
                    9,
                    16,
                    16,
                    0,
                    1.0,
                    0.7083333333333334,
                    None,
                    0.7083333333333334,
                    9,
                    16,
                    0.391304347826087,
                    1.0,
                    16,
                    0.391304347826087,
                    17,
                    1,
                    0,
                    0.7083333333333334,
                    0.7083333333333334,
                    0,
                    None,
                    0.7083333333333334,
                    0.7083333333333334,
                    0,
                    0.0,
                    34,
                    34,
                    0.391304347826087,
                    1.0,
                    9,
                    0.391304347826087,
                    9,
                    1.0,
                    34,
                    None,
                    16,
                    1.0,
                    1.0,
                    34,
                    9,
                ],
                [
                    "6281123",
                    datetime.date(2020, 2, 24),
                    8,
                    51,
                    None,
                    0.3,
                    9,
                    24,
                    16,
                    None,
                    1.0,
                    0.7083333333333334,
                    None,
                    0.7083333333333334,
                    9,
                    24,
                    0.3,
                    1.0,
                    24,
                    0.0,
                    17,
                    2,
                    0,
                    0.7083333333333334,
                    0.7083333333333334,
                    1,
                    None,
                    0.7083333333333334,
                    0.7083333333333334,
                    0,
                    None,
                    34,
                    51,
                    0.3,
                    1.0,
                    9,
                    0.3,
                    0,
                    1.0,
                    51,
                    None,
                    24,
                    1.0,
                    1.0,
                    51,
                    9,
                ],
                [
                    "6281123",
                    datetime.date(2020, 3, 2),
                    8,
                    68,
                    None,
                    0.24324324324324326,
                    0,
                    32,
                    16,
                    0,
                    1.0,
                    0.7083333333333334,
                    None,
                    0.7083333333333334,
                    9,
                    32,
                    0.0,
                    1.0,
                    32,
                    0.0,
                    17,
                    3,
                    0,
                    0.7083333333333334,
                    0.7083333333333334,
                    2,
                    None,
                    0.7083333333333334,
                    0.7083333333333334,
                    0,
                    0.0,
                    34,
                    68,
                    0.24324324324324326,
                    1.0,
                    9,
                    0.24324324324324326,
                    0,
                    1.0,
                    51,
                    None,
                    24,
                    1.0,
                    1.0,
                    68,
                    9,
                ],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField("weekstart", DateType(), True),
                    StructField("fea_txt_msg_incoming_count_01w", IntegerType(), True),
                    StructField("fea_txt_msg_count_02m", LongType(), True),
                    StructField(
                        "fea_txt_msg_not_any_out_call_8w_12w", LongType(), True
                    ),
                    StructField(
                        "fea_txt_msg_voice_ratio_outgoing_01m", DoubleType(), True
                    ),
                    StructField("fea_txt_msg_outgoing_count_03w", LongType(), True),
                    StructField("fea_txt_msg_incoming_count_02m", LongType(), True),
                    StructField("fea_txt_msg_incoming_count_02w", LongType(), True),
                    StructField("fea_txt_msg_outgoing_count_01w", IntegerType(), True),
                    StructField(
                        "fea_txt_msg_voice_ratio_incoming_03w", DoubleType(), True
                    ),
                    StructField("fea_txt_msg_voice_ratio_01m", DoubleType(), True),
                    StructField(
                        "fea_txt_msg_not_any_inc_call_8w_12w", LongType(), True
                    ),
                    StructField("fea_txt_msg_voice_ratio_03w", DoubleType(), True),
                    StructField("fea_txt_msg_outgoing_count_03m", LongType(), True),
                    StructField("fea_txt_msg_incoming_count_03m", LongType(), True),
                    StructField(
                        "fea_txt_msg_voice_ratio_outgoing_03w", DoubleType(), True
                    ),
                    StructField(
                        "fea_txt_msg_voice_ratio_incoming_02w", DoubleType(), True
                    ),
                    StructField("fea_txt_msg_incoming_count_01m", LongType(), True),
                    StructField(
                        "fea_txt_msg_voice_ratio_outgoing_02w", DoubleType(), True
                    ),
                    StructField("fea_txt_msg_count_01w", IntegerType(), True),
                    StructField(
                        "fea_txt_msg_weeks_since_last_outgoing_msg", LongType(), True
                    ),
                    StructField(
                        "fea_txt_msg_weeks_since_last_incoming_msg", LongType(), True
                    ),
                    StructField("fea_txt_msg_voice_ratio_01w", DoubleType(), True),
                    StructField("fea_txt_msg_voice_ratio_02w", DoubleType(), True),
                    StructField("fea_txt_msg_not_any_out_call_1w_4w", LongType(), True),
                    StructField("fea_txt_msg_not_any_inc_call_4w_8w", LongType(), True),
                    StructField("fea_txt_msg_voice_ratio_03m", DoubleType(), True),
                    StructField("fea_txt_msg_voice_ratio_02m", DoubleType(), True),
                    StructField("fea_txt_msg_not_any_inc_call_1w_4w", LongType(), True),
                    StructField(
                        "fea_txt_msg_voice_ratio_outgoing_01w", DoubleType(), True
                    ),
                    StructField("fea_txt_msg_count_02w", LongType(), True),
                    StructField("fea_txt_msg_count_01m", LongType(), True),
                    StructField(
                        "fea_txt_msg_voice_ratio_outgoing_03m", DoubleType(), True
                    ),
                    StructField(
                        "fea_txt_msg_voice_ratio_incoming_01m", DoubleType(), True
                    ),
                    StructField("fea_txt_msg_outgoing_count_02m", LongType(), True),
                    StructField(
                        "fea_txt_msg_voice_ratio_outgoing_02m", DoubleType(), True
                    ),
                    StructField("fea_txt_msg_outgoing_count_02w", LongType(), True),
                    StructField(
                        "fea_txt_msg_voice_ratio_incoming_02m", DoubleType(), True
                    ),
                    StructField("fea_txt_msg_count_03w", LongType(), True),
                    StructField("fea_txt_msg_not_any_out_call_4w_8w", LongType(), True),
                    StructField("fea_txt_msg_incoming_count_03w", LongType(), True),
                    StructField(
                        "fea_txt_msg_voice_ratio_incoming_03m", DoubleType(), True
                    ),
                    StructField(
                        "fea_txt_msg_voice_ratio_incoming_01w", DoubleType(), True
                    ),
                    StructField("fea_txt_msg_count_03m", LongType(), True),
                    StructField("fea_txt_msg_outgoing_count_01m", LongType(), True),
                ]
            ),
        )

        assert_df_frame_equal(
            actual_result_df, expected_result_df, order_by=["msisdn", "weekstart"]
        )
