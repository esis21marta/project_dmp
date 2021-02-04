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

import pyspark
from pyspark.sql import functions as f

from utils import get_rolling_window


def fea_revenue(df_rev: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Calculates airtime loan pattern for each msisdn:

    Args:
        df_rev: Merge Revenue Data

    Returns:
        df_fea: Airtime Revenue Features

    """

    df_fea = (
        df_rev.withColumn(
            "fea_rev_alt_non_loan_tot_sum_1w",
            f.sum(f.col("non_loan_revenue")).over(
                get_rolling_window(7 * 1, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_rev_alt_non_loan_tot_sum_2w",
            f.sum(f.col("non_loan_revenue")).over(
                get_rolling_window(7 * 2, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_rev_alt_non_loan_tot_sum_3w",
            f.sum(f.col("non_loan_revenue")).over(
                get_rolling_window(7 * 3, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_rev_alt_non_loan_tot_sum_1m",
            f.sum(f.col("non_loan_revenue")).over(
                get_rolling_window(7 * 4, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_rev_alt_non_loan_tot_sum_2m",
            f.sum(f.col("non_loan_revenue")).over(
                get_rolling_window(7 * 8, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_rev_alt_non_loan_tot_sum_3m",
            f.sum(f.col("non_loan_revenue")).over(
                get_rolling_window(7 * 13, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_rev_alt_loan_tot_sum_1w",
            f.sum(f.col("loan_revenue")).over(
                get_rolling_window(7 * 1, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_rev_alt_loan_tot_sum_2w",
            f.sum(f.col("loan_revenue")).over(
                get_rolling_window(7 * 2, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_rev_alt_loan_tot_sum_3w",
            f.sum(f.col("loan_revenue")).over(
                get_rolling_window(7 * 3, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_rev_alt_loan_tot_sum_1m",
            f.sum(f.col("loan_revenue")).over(
                get_rolling_window(7 * 4, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_rev_alt_loan_tot_sum_2m",
            f.sum(f.col("loan_revenue")).over(
                get_rolling_window(7 * 8, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_rev_alt_loan_tot_sum_3m",
            f.sum(f.col("loan_revenue")).over(
                get_rolling_window(7 * 13, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_rev_alt_airtime_loan_repayment_transactions_1w",
            f.sum(f.col("loan_repayment_transactions")).over(
                get_rolling_window(7 * 1, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_rev_alt_airtime_loan_repayment_transactions_2w",
            f.sum(f.col("loan_repayment_transactions")).over(
                get_rolling_window(7 * 2, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_rev_alt_airtime_loan_repayment_transactions_3w",
            f.sum(f.col("loan_repayment_transactions")).over(
                get_rolling_window(7 * 3, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_rev_alt_airtime_loan_repayment_transactions_1m",
            f.sum(f.col("loan_repayment_transactions")).over(
                get_rolling_window(7 * 4, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_rev_alt_airtime_loan_repayment_transactions_2m",
            f.sum(f.col("loan_repayment_transactions")).over(
                get_rolling_window(7 * 8, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_rev_alt_airtime_loan_repayment_transactions_3m",
            f.sum(f.col("loan_repayment_transactions")).over(
                get_rolling_window(7 * 13, "msisdn", "weekstart")
            ),
        )
    )

    return df_fea
