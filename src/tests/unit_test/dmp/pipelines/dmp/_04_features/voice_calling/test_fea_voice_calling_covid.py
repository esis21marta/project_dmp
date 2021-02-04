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
    ArrayType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._04_features.voice_calling.fea_voice_calling_covid import (
    fea_voice_calling_covid,
    fea_voice_calling_covid_final,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestFeatureVoiceCalling:

    params = yaml.load(open("conf/dmp/training/parameters.yml"))

    config_feature = yaml.load(open("conf/dmp/training/parameters_feature.yml"))[
        "config_feature"
    ]
    required_output_features = yaml.load(
        open("conf/dmp/training/parameters_feature.yml")
    )["output_features"]

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.voice_calling.fea_voice_calling_covid.get_start_date",
        return_value=datetime.date(2018, 10, 10),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.voice_calling.fea_voice_calling_covid.get_end_date",
        return_value=datetime.date(2020, 10, 10),
        autospec=True,
    )
    def test_fea_inc_out_calls_usage_covid(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        spark_session.sparkContext

        df_voice_calling_pre_covid = spark_session.createDataFrame(
            data=[
                ["1", datetime.date(2020, 1, 20), [], ["12"]],
                ["1", datetime.date(2020, 1, 27), [], []],
                ["1", datetime.date(2020, 2, 24), ["11"], ["12"]],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("in_call_nums", ArrayType(StringType(), True), True),
                    StructField("out_call_nums", ArrayType(StringType(), True), True),
                ]
            ),
        )

        df_voice_calling_covid = spark_session.createDataFrame(
            data=[
                ["1", datetime.date(2020, 3, 23), ["11"], ["12"]],
                ["1", datetime.date(2020, 4, 27), ["11"], ["12"]],
                ["1", datetime.date(2020, 5, 4), ["11"], ["12"]],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("in_call_nums", ArrayType(StringType(), True), True),
                    StructField("out_call_nums", ArrayType(StringType(), True), True),
                ]
            ),
        )

        df_voice_calling_post_covid = spark_session.createDataFrame(
            data=[
                ["1", datetime.date(2020, 6, 1), ["11"], ["12"]],
                ["1", datetime.date(2020, 7, 20), ["11"], ["12"]],
                ["1", datetime.date(2020, 8, 31), ["11"], ["12"]],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("in_call_nums", ArrayType(StringType(), True), True),
                    StructField("out_call_nums", ArrayType(StringType(), True), True),
                ]
            ),
        )

        actual_result_df = fea_voice_calling_covid(
            df_voice_calling_pre_covid,
            df_voice_calling_covid,
            df_voice_calling_post_covid,
            self.config_feature,
            "all",
            self.required_output_features,
        )

        expected_fea_df = spark_session.createDataFrame(
            data=[["1", 2, 1, 0.5, 0.5],],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField(
                        "fea_voice_tot_not_any_inc_pre_covid", LongType(), True
                    ),
                    StructField(
                        "fea_voice_tot_not_any_out_pre_covid", LongType(), True
                    ),
                    StructField(
                        "fea_voice_uniq_out_call_ratio_pre_covid_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_voice_uniq_out_call_ratio_pre_covid_post_covid",
                        DoubleType(),
                        True,
                    ),
                ]
            ),
        )

        assert_df_frame_equal(actual_result_df, expected_fea_df, order_by=["msisdn"])

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.voice_calling.fea_voice_calling_covid.get_start_date",
        return_value=datetime.date(2018, 10, 10),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.voice_calling.fea_voice_calling_covid.get_end_date",
        return_value=datetime.date(2020, 10, 10),
        autospec=True,
    )
    def test_fea_inc_out_calls_usage_covid_final(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        spark_session.sparkContext

        df_voice_calling = spark_session.createDataFrame(
            data=[
                ["1", datetime.date(2019, 12, 16)],
                ["1", datetime.date(2020, 1, 27)],
                ["1", datetime.date(2020, 9, 21)],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField("weekstart", DateType(), True),
                ]
            ),
        )

        df_voice_calling_covid_final = spark_session.createDataFrame(
            data=[["1", 2, 1, 0.5, 0.5],],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField(
                        "fea_voice_tot_not_any_inc_pre_covid", LongType(), True
                    ),
                    StructField(
                        "fea_voice_tot_not_any_out_pre_covid", LongType(), True
                    ),
                    StructField(
                        "fea_voice_uniq_out_call_ratio_pre_covid_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_voice_uniq_out_call_ratio_pre_covid_post_covid",
                        DoubleType(),
                        True,
                    ),
                ]
            ),
        )

        pre_covid_last_weekstart = datetime.date(2020, 2, 24)
        covid_last_weekstart = datetime.date(2020, 6, 1)
        post_covid_last_weekstart = datetime.date(2020, 8, 31)

        actual_result_df = fea_voice_calling_covid_final(
            df_voice_calling,
            df_voice_calling_covid_final,
            pre_covid_last_weekstart,
            covid_last_weekstart,
            post_covid_last_weekstart,
            self.config_feature,
            "all",
            self.required_output_features,
        )

        expected_fea_df = spark_session.createDataFrame(
            data=[
                ["1", datetime.date(2019, 12, 16), None, None, None, None],
                ["1", datetime.date(2020, 1, 27), None, None, None, None],
                ["1", datetime.date(2020, 9, 21), 2, 1, 0.5, 0.5],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField("weekstart", DateType(), True),
                    StructField(
                        "fea_voice_tot_not_any_inc_pre_covid", LongType(), True
                    ),
                    StructField(
                        "fea_voice_tot_not_any_out_pre_covid", LongType(), True
                    ),
                    StructField(
                        "fea_voice_uniq_out_call_ratio_pre_covid_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_voice_uniq_out_call_ratio_pre_covid_post_covid",
                        DoubleType(),
                        True,
                    ),
                ]
            ),
        )

        assert_df_frame_equal(actual_result_df, expected_fea_df, order_by=["msisdn"])
