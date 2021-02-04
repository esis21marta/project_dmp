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
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._04_features.text_messaging.fea_txt_msg_count_covid import (
    fea_txt_msg_count_covid,
    fea_txt_msg_count_covid_final,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestFeaTextMessagingCovid:

    params = yaml.load(open("conf/dmp/training/parameters.yml"))

    config_feature = yaml.load(open("conf/dmp/training/parameters_feature.yml"))[
        "config_feature"
    ]

    required_output_features = yaml.load(
        open("conf/dmp/training/parameters_feature.yml")
    )["output_features"]

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.text_messaging.fea_txt_msg_count_covid.get_start_date",
        return_value=datetime.date(2018, 10, 10),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.text_messaging.fea_txt_msg_count_covid.get_end_date",
        return_value=datetime.date(2020, 10, 10),
        autospec=True,
    )
    def test_fea_txt_msg_count_covid(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        spark_session.sparkContext

        # Initialise test df
        df_txt_msg_weekly_pre_covid = spark_session.createDataFrame(
            data=[
                ["1", datetime.date(2020, 1, 20), 5, 0],
                ["1", datetime.date(2020, 1, 27), 0, 9],
                ["1", datetime.date(2020, 2, 24), 1, 0],
                ["2", datetime.date(2020, 1, 27), 0, 1],
                ["2", datetime.date(2020, 2, 24), 8, 0],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField("weekstart", DateType(), True),
                    StructField("count_txt_msg_incoming", IntegerType(), True),
                    StructField("count_txt_msg_outgoing", IntegerType(), True),
                ]
            ),
        )

        df_txt_msg_weekly_covid = spark_session.createDataFrame(
            data=[
                ["1", datetime.date(2020, 3, 23), 3, 0],
                ["1", datetime.date(2020, 4, 27), 0, 9],
                ["1", datetime.date(2020, 5, 4), 0, 0],
                ["2", datetime.date(2020, 3, 23), 2, 4],
                ["2", datetime.date(2020, 4, 27), 1, 0],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField("weekstart", DateType(), True),
                    StructField("count_txt_msg_incoming", IntegerType(), True),
                    StructField("count_txt_msg_outgoing", IntegerType(), True),
                ]
            ),
        )

        df_txt_msg_weekly_post_covid = spark_session.createDataFrame(
            data=[
                ["1", datetime.date(2020, 6, 22), 1, 0],
                ["1", datetime.date(2020, 7, 20), 2, 9],
                ["1", datetime.date(2020, 8, 17), 0, 0],
                ["2", datetime.date(2020, 7, 20), 0, 3],
                ["2", datetime.date(2020, 8, 17), 0, 0],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField("weekstart", DateType(), True),
                    StructField("count_txt_msg_incoming", IntegerType(), True),
                    StructField("count_txt_msg_outgoing", IntegerType(), True),
                ]
            ),
        )

        # Execution
        actual_result_df = fea_txt_msg_count_covid(
            df_txt_msg_weekly_pre_covid,
            df_txt_msg_weekly_covid,
            df_txt_msg_weekly_post_covid,
            self.config_feature,
            "all",
            self.required_output_features,
        )

        expected_result_df = spark_session.createDataFrame(
            data=[["1", 1, 2], ["2", 1, 1],],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField(
                        "fea_txt_msg_not_any_inc_msg_pre_covid", LongType(), True
                    ),
                    StructField(
                        "fea_txt_msg_not_any_out_msg_pre_covid", LongType(), True
                    ),
                ]
            ),
        )

        assert_df_frame_equal(actual_result_df, expected_result_df, order_by=["msisdn"])

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.text_messaging.fea_txt_msg_count_covid.get_start_date",
        return_value=datetime.date(2018, 10, 10),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.text_messaging.fea_txt_msg_count_covid.get_end_date",
        return_value=datetime.date(2020, 10, 19),
        autospec=True,
    )
    def test_fea_txt_msg_count_covid_final(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        spark_session.sparkContext

        # Initialise test df
        df_txt_msg = spark_session.createDataFrame(
            data=[
                ["1", datetime.date(2019, 12, 16)],
                ["1", datetime.date(2020, 1, 27)],
                ["1", datetime.date(2020, 9, 21)],
                ["1", datetime.date(2020, 10, 19)],
                ["2", datetime.date(2019, 12, 16)],
                ["2", datetime.date(2020, 1, 27)],
                ["2", datetime.date(2020, 9, 21)],
                ["2", datetime.date(2020, 10, 19)],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField("weekstart", DateType(), True),
                ]
            ),
        )

        df_txt_msg_covid_final = spark_session.createDataFrame(
            data=[["1", 1, 2], ["2", 1, 1],],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField(
                        "fea_txt_msg_not_any_inc_msg_pre_covid", IntegerType(), True
                    ),
                    StructField(
                        "fea_txt_msg_not_any_out_msg_pre_covid", IntegerType(), True
                    ),
                ]
            ),
        )

        pre_covid_last_weekstart = datetime.date(2020, 2, 24)
        covid_last_weekstart = datetime.date(2020, 6, 1)
        post_covid_last_weekstart = datetime.date(2020, 8, 31)

        # Execution
        actual_result_df = fea_txt_msg_count_covid_final(
            df_txt_msg,
            df_txt_msg_covid_final,
            pre_covid_last_weekstart,
            covid_last_weekstart,
            post_covid_last_weekstart,
            self.config_feature,
            "all",
            self.required_output_features,
        )

        expected_result_df = spark_session.createDataFrame(
            data=[
                ["1", datetime.date(2019, 12, 16), None, None],
                ["1", datetime.date(2020, 1, 27), None, None],
                ["1", datetime.date(2020, 9, 21), 1, 2],
                ["1", datetime.date(2020, 10, 19), 1, 2],
                ["2", datetime.date(2019, 12, 16), None, None],
                ["2", datetime.date(2020, 1, 27), None, None],
                ["2", datetime.date(2020, 9, 21), 1, 1],
                ["2", datetime.date(2020, 10, 19), 1, 1],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField("weekstart", DateType(), True),
                    StructField(
                        "fea_txt_msg_not_any_inc_msg_pre_covid", IntegerType(), True
                    ),
                    StructField(
                        "fea_txt_msg_not_any_out_msg_pre_covid", IntegerType(), True
                    ),
                ]
            ),
        )

        assert_df_frame_equal(
            actual_result_df, expected_result_df, order_by=["msisdn", "weekstart"]
        )
