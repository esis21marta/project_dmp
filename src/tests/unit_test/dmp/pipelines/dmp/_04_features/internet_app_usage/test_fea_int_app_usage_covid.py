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

import yaml
from pyspark.sql import SparkSession

from src.dmp.pipelines.dmp._04_features.internet_app_usage.fea_internet_app_usage_covid import (
    fea_int_app_usage_covid_trend,
    generate_fea_int_app_usage_covid_fixed,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestFeatureIntAppUsageCovid:
    # File Paths
    base_path = os.path.join(
        os.getcwd(),
        "src/tests/unit_test/dmp/pipelines/dmp/data/covid/internet_app_usage",
    )
    app_usage_scaffold_data = "file://{}/app_usage_weekly_agg_time_series_filled.parquet".format(
        base_path
    )
    app_usage_fea_covid = "file://{}/fea_int_app_usage_covid.parquet".format(base_path)
    app_usage_fea_covid_prep = "file://{}/prepare_fea_int_app_usage_covid.parquet".format(
        base_path
    )
    app_usage_fea_covid_generate = "file://{}/generate_fea_int_app_usage_covid.parquet".format(
        base_path
    )
    app_usage_fea_covid_fixed = "file://{}/fea_int_app_usage_covid_fixed.parquet".format(
        base_path
    )
    app_usage_fea_covid_trend = "file://{}/fea_int_app_usage_covid_trend.parquet".format(
        base_path
    )
    config_feature = yaml.load(open("conf/dmp/training/parameters_feature.yml"))[
        "config_feature"
    ]

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.internet_app_usage.fea_internet_app_usage_covid.get_start_date",
        return_value=datetime.date(2020, 8, 3),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.internet_app_usage.fea_internet_app_usage_covid.get_end_date",
        return_value=datetime.date(2020, 9, 7),
        autospec=True,
    )
    def test_fea_int_app_usage_covid_trend(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        # Read Sample Input Data
        df_app_usage_fea_covid = spark_session.read.parquet(self.app_usage_fea_covid)
        df_app_usage_fea_covid_fixed = spark_session.read.parquet(
            self.app_usage_fea_covid_fixed
        )
        actual_df = fea_int_app_usage_covid_trend(
            df_fea_int_app_usage=df_app_usage_fea_covid,
            df_fea_int_app_usage_covid_fixed=df_app_usage_fea_covid_fixed,
            config_feature=self.config_feature,
            feature_mode="all",
            required_output_features=[],
        )
        expected_feature_df = spark_session.read.parquet(self.app_usage_fea_covid_trend)

        assert_df_frame_equal(actual_df, expected_feature_df)

    def test_generate_fea_int_app_usage_covid_fixed(self, spark_session: SparkSession):
        # Read Sample Input Data
        df_app_usage_fea_covid_prep = spark_session.read.parquet(
            self.app_usage_fea_covid_prep
        )

        actual_df = generate_fea_int_app_usage_covid_fixed(
            df=df_app_usage_fea_covid_prep, fea_suffix="post_covid",
        )

        expected_feature_df = spark_session.read.parquet(
            self.app_usage_fea_covid_generate
        )

        assert_df_frame_equal(
            actual_df, expected_feature_df, order_by=["msisdn", "weekstart"]
        )
