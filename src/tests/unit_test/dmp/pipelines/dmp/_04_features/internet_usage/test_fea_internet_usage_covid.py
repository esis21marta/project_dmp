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

import yaml
from pyspark.sql import SparkSession

from src.dmp.pipelines.dmp._04_features.internet_usage.fea_internet_usage_covid import (
    fea_int_usage_covid_fixed,
    generate_fea_covid_internet_usage,
    generate_fea_covid_internet_usage_fixed,
    prepare_fea_covid_internet_usage,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestFeatureInternetUsageCovid:
    base_path = os.path.join(
        os.getcwd(), "src/tests/unit_test/dmp/pipelines/dmp/data/covid/internet_usage",
    )
    int_usage_weekly_path = "file://{}/internet_usage_weekly_time_series_filled.parquet".format(
        base_path
    )
    prep_covid_int_usage_path = "file://{}/prep_covid_int_usage.parquet".format(
        base_path
    )
    fea_int_usage_post_covid_path = "file://{}/fea_int_usage_post_covid.parquet".format(
        base_path
    )
    fea_int_usage_pre_covid_fixed_path = "file://{}/fea_int_usage_pre_covid_fixed.parquet".format(
        base_path
    )
    fea_int_usage_covid_fixed_path = "file://{}/fea_int_usage_covid_fixed.parquet".format(
        base_path
    )
    fea_int_usage_post_covid_fixed_path = "file://{}/fea_int_usage_post_covid_fixed.parquet".format(
        base_path
    )
    fea_int_usage_covid_all_fixed_path = "file://{}/fea_int_usage_covid_fixed_all.parquet".format(
        base_path
    )

    config_feature = yaml.load(open("conf/dmp/training/parameters_feature.yml"))[
        "config_feature"
    ]

    def test_fea_int_usage_covid_fixed(self, spark_session: SparkSession):
        _fea_int_usage_covid_fixed = fea_int_usage_covid_fixed(fea_suffix="post_covid")
        df_int_usage_weekly = spark_session.read.parquet(self.int_usage_weekly_path)
        filter_weekstart = datetime.date(2020, 8, 31)

        actual_df = _fea_int_usage_covid_fixed(
            df_int_usage_weekly=df_int_usage_weekly,
            config_feature=self.config_feature,
            filter_weekstart=filter_weekstart,
        )

        expected_fea_df = spark_session.read.parquet(
            self.fea_int_usage_post_covid_fixed_path
        )

        assert_df_frame_equal(actual_df, expected_fea_df)

    def test_generate_fea_covid_internet_usage(self, spark_session: SparkSession):
        df_prep = spark_session.read.parquet(self.prep_covid_int_usage_path)
        actual_df = generate_fea_covid_internet_usage(
            df=df_prep, fea_suffix="post_covid", filter_weekstart="2020-08-31"
        )

        expected_fea_df = spark_session.read.parquet(self.fea_int_usage_post_covid_path)

        assert_df_frame_equal(actual_df, expected_fea_df)

    def test_generate_fea_covid_internet_usage_fixed(self, spark_session: SparkSession):
        df_pre_covid = spark_session.read.parquet(
            self.fea_int_usage_pre_covid_fixed_path
        )
        df_covid = spark_session.read.parquet(self.fea_int_usage_covid_fixed_path)
        df_post_covid = spark_session.read.parquet(
            self.fea_int_usage_post_covid_fixed_path
        )

        df = df_pre_covid.join(df_covid, ["msisdn"], how="left").join(
            df_post_covid, ["msisdn"], how="left"
        )
        actual_df = generate_fea_covid_internet_usage_fixed(df=df)

        expected_fea_df = spark_session.read.parquet(
            self.fea_int_usage_covid_all_fixed_path
        )

        assert_df_frame_equal(actual_df, expected_fea_df)

    def test_prepare_fea_covid_internet_usage(self, spark_session: SparkSession):
        df_int_usage_weekly = spark_session.read.parquet(self.int_usage_weekly_path)
        actual_df = prepare_fea_covid_internet_usage(
            df_int_usage_weekly=df_int_usage_weekly, config_feature=self.config_feature
        )

        expected_fea_df = spark_session.read.parquet(self.prep_covid_int_usage_path)

        assert_df_frame_equal(actual_df, expected_fea_df)
