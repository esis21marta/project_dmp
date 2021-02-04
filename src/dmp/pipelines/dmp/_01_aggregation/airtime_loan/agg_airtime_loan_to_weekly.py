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
import pyspark.sql.functions as F
from pyspark.sql.functions import from_unixtime, unix_timestamp

from utils import next_week_start_day

timestamp_format = "yyyy-MM-dd HH:mm:ss"


def fea_calc_airtime_loan_count_features(
    takers_df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Computes the count of airtime loans received per user and outputs `tot_airtime_loan_count`.

    Args:
        takers_df: Dataframe which contains airtime loan takers information.

    Returns:
        Dataframe with additional column `tot_airtime_loan_count`.
    """

    takers_df_weekstart = takers_df.withColumn(
        "trx_date", from_unixtime(unix_timestamp(F.col("timestamp"), timestamp_format))
    ).withColumn("weekstart", next_week_start_day("trx_date"))

    takers_with_count_df = takers_df_weekstart.groupBy("msisdn", "weekstart").agg(
        F.count(F.when(F.col("campaignid").isNotNull(), True)).alias(
            "tot_airtime_loan_count"
        )
    )

    return takers_df_weekstart.join(
        takers_with_count_df, ["msisdn", "weekstart"], "left"
    )


def compute_dpd_per_weeekstart(
    takers_df: pyspark.sql.DataFrame, payment_df: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    """
    Computes aggregations of DPD counts to a msisdn-weekstart granularity.

    Args:
        takers_df: Dataframe which contains airtime loan takers information.
        payment_df: Dataframe which contains airtime loan payers information.

    Returns:
        Dataframe containing column tot_airtime_loan_count (total airtime loan counts) and xx_dpd_count (day-past-due
        counts of multiple ranges).
    """

    # Left join takers and payment with msisdn, campaignid, month campaign
    takers_payment_joined = takers_df.alias("takers_df").join(
        payment_df.alias("payment_df"),
        (F.col("takers_df.msisdn") == F.col("payment_df.msisdn"))
        & (
            F.col("takers_df.campaignid").substr(22, 4)
            == F.col("payment_df.campaignid").substr(24, 4)
        )
        & (F.col("takers_df.month_campaign") == F.col("payment_df.month_campaign")),
        "left_outer",
    )

    # Convert timestamp columns for takers and payment
    takers_payment_joined_cleaned = takers_payment_joined.select(
        "takers_df.msisdn",
        "takers_df.weekstart",
        "takers_df.trx_date",
        F.col("takers_df.tot_airtime_loan_count"),
        from_unixtime(
            unix_timestamp(F.col("takers_df.timestamp"), timestamp_format)
        ).alias("taker_timestamp"),
        from_unixtime(
            unix_timestamp(F.col("payment_df.timestamp"), timestamp_format)
        ).alias("repayment_timestamp"),
    )

    # Compute duration between dates (excluding start, including end)
    paid_takers_df = (
        takers_payment_joined_cleaned
        # .filter(F.col('repayment_timestamp').isNotNull())
        .withColumn(
            "repayment_duration",
            F.datediff(F.col("repayment_timestamp"), F.col("taker_timestamp")),
        )
    )

    paid_takers_df_weekly = paid_takers_df.groupBy("msisdn", "weekstart").agg(
        F.first(F.col("tot_airtime_loan_count")).alias("tot_airtime_loan_count"),
        F.max(F.col("repayment_duration")).alias("repayment_duration"),
        F.when(F.max(F.col("repayment_duration")).isNull(), None)
        .otherwise(F.count(F.when(F.col("repayment_duration") > 2, True)))
        .alias("2_dpd_count"),
        F.when(F.max(F.col("repayment_duration")).isNull(), None)
        .otherwise(F.count(F.when(F.col("repayment_duration") > 10, True)))
        .alias("10_dpd_count"),
        F.when(F.max(F.col("repayment_duration")).isNull(), None)
        .otherwise(F.count(F.when(F.col("repayment_duration") > 15, True)))
        .alias("15_dpd_count"),
        F.when(F.max(F.col("repayment_duration")).isNull(), None)
        .otherwise(F.count(F.when(F.col("repayment_duration") > 20, True)))
        .alias("20_dpd_count"),
        F.when(F.max(F.col("repayment_duration")).isNull(), None)
        .otherwise(F.count(F.when(F.col("repayment_duration") > 25, True)))
        .alias("25_dpd_count"),
    )

    return paid_takers_df_weekly


def create_weekly_airtime_loan_table(
    df_airtime_loan_takers: pyspark.sql.DataFrame,
    df_airtime_loan_payment: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Creates weekly aggregation for airtime loan tables

    Args:
        df_airtime_loan_takers: Dataframe which contains airtime loan takers information.
        df_airtime_loan_payment: Dataframe which contains airtime loan payers information.

    Returns:
        Dataframe with airtime loan weekly aggregated columns
    """
    takers_df_with_atl_count = fea_calc_airtime_loan_count_features(
        df_airtime_loan_takers
    )
    airtime_loan_weekly = compute_dpd_per_weeekstart(
        takers_df_with_atl_count, df_airtime_loan_payment
    )

    return airtime_loan_weekly
