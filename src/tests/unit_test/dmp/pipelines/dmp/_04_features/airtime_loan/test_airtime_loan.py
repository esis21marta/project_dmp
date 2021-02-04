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


from datetime import date
from unittest import mock

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._04_features.airtime_loan.fea_airtime_loan import (
    fea_airtime_loan,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


@mock.patch(
    "src.dmp.pipelines.dmp._04_features.airtime_loan.fea_airtime_loan.get_start_date",
    return_value=date(2019, 10, 7),
    autospec=True,
)
@mock.patch(
    "src.dmp.pipelines.dmp._04_features.airtime_loan.fea_airtime_loan.get_end_date",
    return_value=date(2019, 10, 7),
    autospec=True,
)
class TestAirtimeLoanFeatures(object):
    param_path = "conf/dmp/training/parameters_feature.yml"
    param_stream = open(param_path, "r")
    param_feature = yaml.load(stream=param_stream, Loader=yaml.FullLoader)
    config_feature = param_feature["config_feature"]

    def test_fea_airtime_loan(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):

        cms_tracking_stream_weekly_data = [
            (
                "12345678xyz",
                date(2019, 10, 7),
                10000,
                [date(2019, 10, 3), date(2019, 10, 5)],
                [date(2019, 10, 6)],
                ["AAAAAABB-LOAN-5K-A", "AAAAAABB-LOAN-5K-A"],
                [
                    [date(2019, 10, 3), "AAAAAABB-LOAN-5K-A"],
                    [date(2019, 10, 5), "AAAAAABB-LOAN-5K-A"],
                ],
                ["AAAAAABB-LOAN-5K-A", "AAAAAABB-LOAN-5K-A"],
                [
                    [date(2019, 10, 3), "AAAAAABB-LOAN-5K-A"],
                    [date(2019, 10, 5), "AAAAAABB-LOAN-5K-A"],
                ],
                1,
                1,
                1,
                5000.0,
            )
        ]
        cms_tracking_stream_weekly_schema = StructType(
            [
                StructField("msisdn", StringType(), True),
                StructField("weekstart", DateType(), True),
                StructField("loan_amount", LongType(), True),
                StructField("loan_date", ArrayType(DateType(), True), True),
                StructField("loan_repayment_date", ArrayType(DateType(), True), True),
                StructField(
                    "loan_offers_accepted", ArrayType(StringType(), True), True
                ),
                StructField(
                    "loan_offers_accepted_with_dates",
                    ArrayType(
                        StructType(
                            [
                                StructField("event_date", DateType(), True),
                                StructField("campaignid", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    True,
                ),
                StructField("loan_offers_sent", ArrayType(StringType(), True), True),
                StructField(
                    "loan_offers_sent_with_dates",
                    ArrayType(
                        StructType(
                            [
                                StructField("event_date", DateType(), True),
                                StructField("campaignid", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    True,
                ),
                StructField("loan_offers_notifications", LongType(), False),
                StructField("loan_offers_repaid", LongType(), False),
                StructField("loan_offers_eligible", LongType(), False),
                StructField("avg_eligible_loan_amount", DoubleType(), True),
            ]
        )

        df_cms_tracking_stream_weekly_data = spark_session.createDataFrame(
            data=cms_tracking_stream_weekly_data,
            schema=cms_tracking_stream_weekly_schema,
        )

        df_res = fea_airtime_loan(
            df_loan=df_cms_tracking_stream_weekly_data,
            config_feature={},
            feature_mode="all",
            required_output_features=[],
        )

        fea_data = [
            (
                "12345678xyz",
                date(2019, 10, 7),
                2.0,
                2,
                2.0,
                2.0,
                2.0,
                2.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                5000.0,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
            )
        ]

        fea_schema = StructType(
            [
                StructField("msisdn", StringType(), False),
                StructField("weekstart", DateType(), False),
                StructField("fea_airtime_loan_avg_takeup_time_1m", DoubleType(), False),
                StructField(
                    "fea_airtime_loan_avg_takeup_time_1w", IntegerType(), False
                ),
                StructField("fea_airtime_loan_avg_takeup_time_2m", DoubleType(), False),
                StructField("fea_airtime_loan_avg_takeup_time_2w", DoubleType(), False),
                StructField("fea_airtime_loan_avg_takeup_time_3m", DoubleType(), False),
                StructField("fea_airtime_loan_avg_takeup_time_3w", DoubleType(), False),
                StructField(
                    "fea_airtime_loan_offers_acceptance_ratio_1m", DoubleType(), False
                ),
                StructField(
                    "fea_airtime_loan_offers_acceptance_ratio_1w", DoubleType(), False
                ),
                StructField(
                    "fea_airtime_loan_offers_acceptance_ratio_2m", DoubleType(), False
                ),
                StructField(
                    "fea_airtime_loan_offers_acceptance_ratio_2w", DoubleType(), False
                ),
                StructField(
                    "fea_airtime_loan_offers_acceptance_ratio_3m", DoubleType(), False
                ),
                StructField(
                    "fea_airtime_loan_offers_acceptance_ratio_3w", DoubleType(), False
                ),
                StructField(
                    "fea_airtime_loan_offers_accepted_1m", IntegerType(), False
                ),
                StructField(
                    "fea_airtime_loan_offers_accepted_1w", IntegerType(), False
                ),
                StructField(
                    "fea_airtime_loan_offers_accepted_2m", IntegerType(), False
                ),
                StructField(
                    "fea_airtime_loan_offers_accepted_2w", IntegerType(), False
                ),
                StructField(
                    "fea_airtime_loan_offers_accepted_3m", IntegerType(), False
                ),
                StructField(
                    "fea_airtime_loan_offers_accepted_3w", IntegerType(), False
                ),
                StructField("fea_airtime_loan_offers_eligible_1m", LongType(), False),
                StructField("fea_airtime_loan_offers_eligible_1w", LongType(), False),
                StructField("fea_airtime_loan_offers_eligible_2m", LongType(), False),
                StructField("fea_airtime_loan_offers_eligible_2w", LongType(), False),
                StructField("fea_airtime_loan_offers_eligible_3m", LongType(), False),
                StructField("fea_airtime_loan_offers_eligible_3w", LongType(), False),
                StructField(
                    "fea_airtime_loan_offers_eligible_amount_avg_1w",
                    DoubleType(),
                    False,
                ),
                StructField(
                    "fea_airtime_loan_offers_notifications_1m", LongType(), False
                ),
                StructField(
                    "fea_airtime_loan_offers_notifications_1w", LongType(), False
                ),
                StructField(
                    "fea_airtime_loan_offers_notifications_2m", LongType(), False
                ),
                StructField(
                    "fea_airtime_loan_offers_notifications_2w", LongType(), False
                ),
                StructField(
                    "fea_airtime_loan_offers_notifications_3m", LongType(), False
                ),
                StructField(
                    "fea_airtime_loan_offers_notifications_3w", LongType(), False
                ),
                StructField("fea_airtime_loan_offers_repaid_1m", LongType(), False),
                StructField("fea_airtime_loan_offers_repaid_1w", LongType(), False),
                StructField("fea_airtime_loan_offers_repaid_2m", LongType(), False),
                StructField("fea_airtime_loan_offers_repaid_2w", LongType(), False),
                StructField("fea_airtime_loan_offers_repaid_3m", LongType(), False),
                StructField("fea_airtime_loan_offers_repaid_3w", LongType(), False),
                StructField("fea_airtime_loan_offers_sent_1m", IntegerType(), False),
                StructField("fea_airtime_loan_offers_sent_1w", IntegerType(), False),
                StructField("fea_airtime_loan_offers_sent_2m", IntegerType(), False),
                StructField("fea_airtime_loan_offers_sent_2w", IntegerType(), False),
                StructField("fea_airtime_loan_offers_sent_3m", IntegerType(), False),
                StructField("fea_airtime_loan_offers_sent_3w", IntegerType(), False),
                StructField("fea_airtime_whitelisted_1w", IntegerType(), False),
            ]
        )

        df_exp_res = spark_session.createDataFrame(data=fea_data, schema=fea_schema,)

        assert_df_frame_equal(df_res, df_exp_res)
