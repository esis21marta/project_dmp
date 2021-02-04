import os

from pyspark.sql import SparkSession

from src.dmp.pipelines.dmp._04_features.customer_profile.customer_profile import (
    customer_profile,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestCustomerProfileFeatures(object):
    """
    Test Case for Customer Expiry Features
    """

    # File Paths
    base_path = os.path.join(
        os.getcwd(), "src/tests/unit_test/dmp/pipelines/dmp/data/customer_profile"
    )
    customer_profile_data = "file://{}/customer_profile.csv".format(base_path)
    fea_customer_profile_data = "file://{}/fea_customer_profile.parquet".format(
        base_path
    )

    def test_customer_profile(self, spark_session: SparkSession) -> None:
        """
        Testing Customer Expiry Date Features
        """
        # Read Sample Input Data
        df_customer_profile = spark_session.read.csv(
            path=self.customer_profile_data, header=True
        )

        # Read Sample Output Data
        expected_feature_df = spark_session.read.parquet(self.fea_customer_profile_data)

        # Call Function
        actual_feature_df = customer_profile(
            df_customer_profile, feature_mode="all", required_output_features=[],
        )

        assert_df_frame_equal(actual_feature_df, expected_feature_df)
