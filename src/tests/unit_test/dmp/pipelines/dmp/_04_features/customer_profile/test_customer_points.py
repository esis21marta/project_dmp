import datetime
import os
from unittest import mock

import yaml
from pyspark.sql import SparkSession

from src.dmp.pipelines.dmp._04_features.customer_profile.fea_customer_points import (
    fea_customer_points,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestCustomerProfilePointsFeatures(object):
    """
    Test Case for Customer Expiry Features
    """

    config_feature = yaml.load(open("conf/dmp/training/parameters_feature.yml"))[
        "config_feature"
    ]
    # File Paths
    base_path = os.path.join(
        os.getcwd(), "src/tests/unit_test/dmp/pipelines/dmp/data/customer_profile"
    )
    customer_profile_scaffold_data = "file://{}/customer_profile_scaffold.csv".format(
        base_path
    )
    customer_redeem_scaffold_data = "file://{}/all_redeem_points_scaffold.csv".format(
        base_path
    )
    fea_customer_profile_points_data = "file://{}/fea_customer_profile_points.parquet".format(
        base_path
    )

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.customer_profile.fea_customer_points.get_start_date",
        return_value=datetime.date(2020, 1, 1),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.customer_profile.fea_customer_points.get_end_date",
        return_value=datetime.date(2020, 12, 1),
        autospec=True,
    )
    def test_customer_profile(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ) -> None:
        """
        Testing Customer Expiry Date Features
        """
        # Read Sample Input Data
        df_customer_profile = spark_session.read.csv(
            path=self.customer_profile_scaffold_data, header=True
        )
        df_customer_point = spark_session.read.csv(
            path=self.customer_redeem_scaffold_data, header=True
        )

        # Read Sample Output Data
        expected_feature_df = spark_session.read.parquet(
            self.fea_customer_profile_points_data
        )

        # Call Function
        actual_feature_df = fea_customer_points(
            df_customer_profile,
            df_customer_point,
            config_feature=self.config_feature,
            feature_mode="all",
            required_output_features=[],
        )

        assert_df_frame_equal(actual_feature_df, expected_feature_df)
