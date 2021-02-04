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
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

from src.dmp.pipelines.dmp._04_features.recharge.fea_recharge_covid import (
    fea_recharge_covid_rename,
    generate_fea_balance_covid,
    generate_fea_pkg_prchse_covid,
    generate_fea_topup_behav_covid,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestFeatureRechargeCovid:
    base_path = os.path.join(
        os.getcwd(), "src/tests/unit_test/dmp/pipelines/dmp/data/covid/recharge",
    )
    rech_weekly_path = "file://{}/df_rech_weekly.parquet".format(base_path)
    digi_rech_weekly_path = "file://{}/df_digi_rech_weekly.parquet".format(base_path)
    acc_bal_weekly_path = "file://{}/acc_bal_weekly.parquet".format(base_path)
    chg_pkg_prchse_weekly_path = "file://{}/chg_pkg_prchse_weekly.parquet".format(
        base_path
    )

    fea_topup_behav_covid_path = "file://{}/fea_topup_behav_covid.parquet".format(
        base_path
    )
    fea_acc_bal_path = "file://{}/fea_acc_bal_covid.parquet".format(base_path)
    fea_pkg_prchse_path = "file://{}/fea_pkg_prchse_covid.parquet".format(base_path)

    config_feature = yaml.load(open("conf/dmp/training/parameters_feature.yml"))[
        "config_feature"
    ]

    def test_fea_recharge_covid_rename(self, spark_session: SparkSession):
        df_fea_rech = spark_session.createDataFrame(
            data=[
                ["1", datetime.date(2020, 3, 23), 3.0,],
                ["2", datetime.date(2020, 4, 27), 1.0,],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField("weekstart", DateType(), True),
                    StructField("fea_rech_amt_weekly_avg_03m", DoubleType(), True),
                ]
            ),
        )

        actual_df = fea_recharge_covid_rename(fea_df=df_fea_rech, suffix="covid")

        expected_fea_df = spark_session.createDataFrame(
            data=[
                ["1", datetime.date(2020, 3, 23), 3.0,],
                ["2", datetime.date(2020, 4, 27), 1.0,],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField("weekstart", DateType(), True),
                    StructField("fea_rech_amt_weekly_avg_covid", DoubleType(), True),
                ]
            ),
        )

        assert_df_frame_equal(actual_df, expected_fea_df)

    def test_generate_fea_topup_behav_covid(self, spark_session: SparkSession):
        df_rech_weekly = spark_session.read.parquet(self.rech_weekly_path)
        df_digi_rech_weekly = spark_session.read.parquet(self.digi_rech_weekly_path)
        df_rech_weekly = df_rech_weekly.join(
            df_digi_rech_weekly, ["msisdn", "weekstart"], how="left"
        )

        config_feature_recharge = self.config_feature["recharge"]
        actual_df = generate_fea_topup_behav_covid(
            df=df_rech_weekly, config_feature_recharge=config_feature_recharge
        )

        expected_fea_df = spark_session.read.parquet(self.fea_topup_behav_covid_path)

        assert_df_frame_equal(actual_df, expected_fea_df)

    def test_generate_fea_balance_covid(self, spark_session: SparkSession):
        df_acc_bal_weekly = spark_session.read.parquet(self.acc_bal_weekly_path)
        config_feature_recharge = self.config_feature["recharge"]
        actual_df = generate_fea_balance_covid(
            df=df_acc_bal_weekly, config_feature_recharge=config_feature_recharge
        )

        expected_fea_df = spark_session.read.parquet(self.fea_acc_bal_path)

        assert_df_frame_equal(actual_df, expected_fea_df)

    def test_generate_fea_pkg_prchse_covid(self, spark_session: SparkSession):
        df_chg_pkg_prchse_weekly = spark_session.read.parquet(
            self.chg_pkg_prchse_weekly_path
        )
        config_feature_recharge = self.config_feature["recharge"]
        actual_df = generate_fea_pkg_prchse_covid(
            df=df_chg_pkg_prchse_weekly, config_feature_recharge=config_feature_recharge
        )

        expected_fea_df = spark_session.read.parquet(self.fea_pkg_prchse_path)

        assert_df_frame_equal(actual_df, expected_fea_df)
