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

from src.dmp.pipelines.dmp._01_aggregation.airtime_loan.create_loan_tracker_weekly import (
    _weekly_aggregation as create_loan_tracker_weekly,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


@mock.patch(
    "src.dmp.pipelines.dmp._01_aggregation.airtime_loan.create_loan_tracker_weekly.get_start_date",
    return_value=date(2018, 10, 10),
    autospec=True,
)
@mock.patch(
    "src.dmp.pipelines.dmp._01_aggregation.airtime_loan.create_loan_tracker_weekly.get_end_date",
    return_value=date(2020, 10, 10),
    autospec=True,
)
class TestCreateLoanTrackerWeekly(object):
    def test_create_loan_tracker_weekly(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):

        cms_tracking_stream_data = [
            ("12345678xyz", "AAAAAABB-LOAN-5K-A", "ELIGIBLE", "2019-10-01"),
            ("12345678xyz", "AAAAAABB-LOAN-5K-A", "FULFILLED", "2019-10-03"),
            ("12345678xyz", "AAAAAABB-LOAN-5K-A", "FULFILLED", "2019-10-05"),
        ]

        cms_tracking_stream_schema = StructType(
            [
                StructField("msisdn", StringType(), True),
                StructField("campaignid", StringType(), True),
                StructField("streamtype", StringType(), True),
                StructField("event_date", StringType(), True),
            ]
        )

        df_cms_tracking_stream_dd = spark_session.createDataFrame(
            data=cms_tracking_stream_data, schema=cms_tracking_stream_schema
        )

        df_res = create_loan_tracker_weekly(df_cms=df_cms_tracking_stream_dd)

        cms_tracking_stream_weekly_data = [
            (
                "12345678xyz",
                date(2019, 10, 7),
                10000,
                [date(2019, 10, 3), date(2019, 10, 5)],
                [],
                ["AAAAAABB-LOAN-5K-A", "AAAAAABB-LOAN-5K-A"],
                [
                    [date(2019, 10, 3), "AAAAAABB-LOAN-5K-A"],
                    [date(2019, 10, 5), "AAAAAABB-LOAN-5K-A"],
                ],
                [],
                [],
                0,
                0,
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

        df_exp_res = spark_session.createDataFrame(
            data=cms_tracking_stream_weekly_data,
            schema=cms_tracking_stream_weekly_schema,
        )

        assert_df_frame_equal(df_res, df_exp_res)
