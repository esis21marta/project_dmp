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

import os

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType

from src.dmp.pipelines.dmp._01_aggregation.internet_app_usage.create_internet_app_usage_weekly import (
    _weekly_aggregation as create_internet_app_usage_weekly,
)
from src.dmp.pipelines.dmp._01_aggregation.internet_app_usage.create_internet_competitor_monthly import (
    _monthly_aggregation as create_internet_app_competitor_monthly,
)
from src.dmp.pipelines.dmp._01_aggregation.internet_app_usage.create_internet_connection_weekly import (
    _weekly_aggregation as create_internet_connection_weekly,
)
from src.dmp.pipelines.dmp._01_aggregation.internet_app_usage.feature_mapping import (
    feature_mapping,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestInternetAppUsageAggregation(object):
    """
    Test Case for Internet App Usage Aggregation
    """

    # File Paths
    base_path = os.path.join(
        os.getcwd(), "src/tests/unit_test/dmp/pipelines/dmp/data/internet_app_usage"
    )
    app_mapping = "file://{}/app_mapping.csv".format(base_path)
    category_mapping = "file://{}/category_mapping.csv".format(base_path)
    content_mapping = "file://{}/content_mapping.csv".format(base_path)
    fintech_mapping = "file://{}/fintech_mapping.csv".format(base_path)
    segment_mapping = "file://{}/segment_mapping.csv".format(base_path)
    int_app_mapping = "file://{}/int_app_mapping.parquet".format(base_path)

    bcp_data = "file://{}/bcp_data.csv".format(base_path)
    app_usage_data = "file://{}/app_usage_weekly_agg.parquet".format(base_path)
    int_usage_data = "file://{}/int_usage_weekly_agg.parquet".format(base_path)
    competitor_data = "file://{}/competitor_data.csv".format(base_path)
    int_competitor_data = "file://{}/int_competitor_data.csv".format(base_path)

    def test_feature_mapping(self, spark_session: SparkSession) -> None:
        """
        Testing Feature Mapping
        """
        # Read Sample Input Data
        df_app = spark_session.read.csv(path=self.app_mapping, header=True)
        df_cat = spark_session.read.csv(path=self.category_mapping, header=True)
        df_con = spark_session.read.csv(path=self.content_mapping, header=True)
        df_fin = spark_session.read.csv(path=self.fintech_mapping, header=True)
        df_seg = spark_session.read.csv(path=self.segment_mapping, header=True)

        # Read Sample Output Data
        df_int_app_mapp = spark_session.read.parquet(self.int_app_mapping)

        # Call Function
        df_res = feature_mapping(
            df_app_mapping=df_app,
            df_category_mapping=df_cat,
            df_content_mapping=df_con,
            df_fintech_mapping=df_fin,
            df_segment_mapping=df_seg,
        )

        # Compare Sample Output Data & Function Output Data
        df_comp = (df_int_app_mapp.exceptAll(df_res)).union(
            df_res.exceptAll(df_int_app_mapp)
        )

        assert df_comp.count() == 0

    def test_app_usage_weekly(self, spark_session: SparkSession) -> None:
        """
        Testing App Usage Weekly Aggregation
        """
        # Read Sample Input Data
        df_bcp_data = spark_session.read.csv(path=self.bcp_data, header=True)
        df_int_app_mapp = spark_session.read.parquet(self.int_app_mapping)

        # Read Sample Output Data
        df_app_usage_data = spark_session.read.parquet(self.app_usage_data)

        # Call Function
        df_res = create_internet_app_usage_weekly(
            df_bcp_usage=df_bcp_data, df_app_feature_mapping=df_int_app_mapp
        )

        # Compare Sample Output Data & Function Output Data
        df_comp = (df_app_usage_data.exceptAll(df_res)).union(
            df_res.exceptAll(df_app_usage_data)
        )

        assert df_comp.count() == 0

    def test_int_usage_weekly(self, spark_session: SparkSession) -> None:
        """
        Testing Internet Usage Weekly Aggregation
        """
        # Read Sample Input Data
        df_bcp_data = spark_session.read.csv(path=self.bcp_data, header=True)

        # Read Sample Output Data
        df_int_usage_data = spark_session.read.parquet(self.int_usage_data)

        # Call Function
        df_res = create_internet_connection_weekly(df_bcp_usage=df_bcp_data)

        # Compare Sample Output Data & Function Output Data
        df_comp = (df_int_usage_data.exceptAll(df_res)).union(
            df_res.exceptAll(df_int_usage_data)
        )

        assert df_comp.count() == 0

    def test_create_internet_app_competitor_monthly(
        self, spark_session: SparkSession
    ) -> None:
        """
        Testing Internet Usage Weekly Aggregation
        """

        input_schema = StructType(
            [
                StructField("msisdn", StringType(), False),
                StructField("appscategory", StringType(), True),
                StructField("apps", StringType(), True),
                StructField("component", StringType(), True),
                StructField("trx", LongType(), True),
                StructField("payload", LongType(), True),
                StructField("month", StringType(), False),
            ]
        )

        # Read Sample Input Data
        df_competitor_data = spark_session.read.csv(
            path=self.competitor_data, header=True, schema=input_schema
        )

        output_schema = StructType(
            [
                StructField("msisdn", StringType(), False),
                StructField("month", StringType(), False),
                StructField("comp_websites_visits", LongType(), True),
                StructField("comp_websites_consumption", LongType(), True,),
                StructField("comp_apps_websites_visits", LongType(), True),
                StructField("comp_apps_websites_consumption", LongType(), True),
                StructField("month_mapped_dt", DateType(), False),
            ]
        )

        # Read Sample Output Data
        df_int_competitor_data = spark_session.read.csv(
            self.int_competitor_data, header=True, schema=output_schema
        )

        month_dt_dict = {
            "2020-05": "2000-01-24",
            "2020-06": "2000-01-25",
            "2020-07": "2000-01-26",
        }

        # Call Function
        df_res = create_internet_app_competitor_monthly(
            df_competitor_data, month_dt_dict
        )
        assert_df_frame_equal(df_res, df_int_competitor_data)
