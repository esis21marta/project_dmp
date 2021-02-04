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
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType

from src.dmp.pipelines.dmp._04_features.internet_app_usage.fea_internet_app_usage_competitor import (
    fea_internet_app_usage_competitor,
)
from src.dmp.pipelines.dmp._04_features.internet_app_usage.fea_internet_connection import (
    fea_internet_connection,
)
from src.dmp.pipelines.dmp._04_features.internet_app_usage.fea_social_media import (
    fea_social_media,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestInternetAppUsageFeatures(object):
    """
    Test Case for Internet App Usage Primary
    """

    # File Paths
    base_path = os.path.join(
        os.getcwd(), "src/tests/unit_test/dmp/pipelines/dmp/data/internet_app_usage"
    )
    int_usage_scaffold_data = "file://{}/int_usage_weekly_agg_time_series_filled.parquet".format(
        base_path
    )
    fea_int_app_usage_data = "file://{}/fea_int_app_usage_v4.parquet".format(base_path)
    fea_int_con_data = "file://{}/fea_int_con.parquet".format(base_path)
    fea_social_media_data = "file://{}/fea_social_media.parquet".format(base_path)
    competitor_app_usage = "file://{}/competitor_app_usage.csv".format(base_path)
    fea_competitor_app_usage = "file://{}/fea_competitor_app_usage.csv".format(
        base_path
    )

    config_feature = yaml.load(open("conf/dmp/training/parameters_feature.yml"))[
        "config_feature"
    ]

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.internet_app_usage.fea_internet_connection.get_start_date",
        return_value=datetime.date(2019, 10, 10),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.internet_app_usage.fea_internet_connection.get_end_date",
        return_value=datetime.date(2020, 10, 10),
        autospec=True,
    )
    def test_fea_internet_connection(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ) -> None:
        """
        Testing Internet Connection Features
        """
        # Read Sample Input Data
        df_int_usage_scaffold_data = spark_session.read.parquet(
            self.int_usage_scaffold_data
        )

        # Read Sample Output Data
        expected_feature_df = spark_session.read.parquet(self.fea_int_con_data)

        # Call Function
        actual_feature_df = fea_internet_connection(
            df_internet_usage_weekly=df_int_usage_scaffold_data,
            config_feature=self.config_feature,
            feature_mode="all",
            required_output_features=[],
        )

        assert_df_frame_equal(actual_feature_df, expected_feature_df)

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.internet_app_usage.fea_social_media.get_start_date",
        return_value=datetime.date(2019, 10, 10),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.internet_app_usage.fea_social_media.get_end_date",
        return_value=datetime.date(2020, 10, 10),
        autospec=True,
    )
    def test_fea_social_media(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ) -> None:
        """
        Testing Social Media Features
        """
        # Read Sample Input Data
        df_fea_int_app_usage = spark_session.read.parquet(self.fea_int_app_usage_data)
        df_fea_int_con = spark_session.read.parquet(self.fea_int_con_data)

        # Read Sample Output Data
        expected_feature_df = spark_session.read.parquet(self.fea_social_media_data)

        # Call Function
        actual_feature_df = fea_social_media(
            df_fea_int_app_usage=df_fea_int_app_usage,
            df_fea_int_con=df_fea_int_con,
            config_feature=self.config_feature,
            feature_mode="all",
            required_output_features=[],
        )

        assert_df_frame_equal(actual_feature_df, expected_feature_df)

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.internet_app_usage.fea_internet_app_usage_competitor.get_start_date",
        return_value=datetime.date(2019, 3, 9),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.internet_app_usage.fea_internet_app_usage_competitor.get_end_date",
        return_value=datetime.date(2020, 6, 22),
        autospec=True,
    )
    def test_fea_internet_app_usage_competitor(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ) -> None:

        """
        Testing Internet App Usage Features
        """

        schema = StructType(
            [
                StructField("msisdn", StringType(), False),
                StructField("mo_id", StringType(), False),
                StructField("comp_websites_visits", LongType(), True),
                StructField("comp_websites_consumption", LongType(), True),
                StructField("comp_apps_websites_visits", LongType(), True),
                StructField("comp_apps_websites_consumption", LongType(), True),
                StructField("month_mapped_dt", DateType(), False),
            ]
        )

        # Read Sample Input Data
        df_app_competitor_data = spark_session.read.csv(
            self.competitor_app_usage, header=True, schema=schema
        )

        # Call Function
        actual_feature_df = fea_internet_app_usage_competitor(
            df_internet_app_usage_competitor=df_app_competitor_data,
            sla_date_parameter=8,
            config_feature=self.config_feature,
            feature_mode="all",
            required_output_features=[],
        )

        schema = StructType(
            [
                StructField("msisdn", StringType(), False),
                StructField("weekstart", StringType(), False),
                StructField(
                    "fea_int_app_usage_comp_websites_visits_01m", LongType(), True
                ),
                StructField(
                    "fea_int_app_usage_comp_websites_visits_02m", LongType(), True,
                ),
                StructField(
                    "fea_int_app_usage_comp_websites_visits_03m", LongType(), True
                ),
                StructField(
                    "fea_int_app_usage_comp_websites_consumption_01m", LongType(), True
                ),
                StructField(
                    "fea_int_app_usage_comp_websites_consumption_02m", LongType(), True
                ),
                StructField(
                    "fea_int_app_usage_comp_websites_consumption_03m", LongType(), True
                ),
                StructField(
                    "fea_int_app_usage_comp_websites_apps_visits_01m", LongType(), True
                ),
                StructField(
                    "fea_int_app_usage_comp_websites_apps_visits_02m", LongType(), True
                ),
                StructField(
                    "fea_int_app_usage_comp_websites_apps_visits_03m", LongType(), True
                ),
                StructField(
                    "fea_int_app_usage_comp_websites_apps_consumption_01m",
                    LongType(),
                    True,
                ),
                StructField(
                    "fea_int_app_usage_comp_websites_apps_consumption_02m",
                    LongType(),
                    True,
                ),
                StructField(
                    "fea_int_app_usage_comp_websites_apps_consumption_03m",
                    LongType(),
                    True,
                ),
            ]
        )

        # Read Sample Output Data
        expected_feature_df = spark_session.read.csv(
            self.fea_competitor_app_usage, sep=",", header=True, schema=schema,
        )

        assert_df_frame_equal(actual_feature_df, expected_feature_df)
