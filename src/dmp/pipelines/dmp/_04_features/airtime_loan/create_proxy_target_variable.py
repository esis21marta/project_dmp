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

from typing import List

import pyspark
from pyspark.sql import functions as f

from utils import get_end_date, get_required_output_columns, get_start_date


def create_proxy_target_variable(
    df_loan: pyspark.sql.DataFrame,
    df_revenue: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Calculates internal spend pattern revenue for each msisdn:

    :param df_loan: CMS Tracking Data
    :param df_revenue: Revenue Data
    :param config_feature: Features Configurations from Parameters
    :param feature_mode: Features Mode from Parameters [all | list]
    :param required_output_features: Features List from Parameters

    :return: df_target_variable: Proxy Target Variable

    """

    config_feature.get("airtime", None)

    # Loan Repayment Date is 3 months after campaign month end,
    # So if any loan taken in July 2020 campaign month, then due date will be Oct 31, 2020
    df_revenue = df_revenue.withColumn(
        "campaign_month", f.date_trunc("month", f.date_sub(f.col("weekstart"), 7))
    )
    df_loan = df_loan.withColumn(
        "campaign_month", f.date_trunc("month", f.date_sub(f.col("weekstart"), 7))
    ).withColumn(
        "due_month", f.date_trunc("month", f.date_add(f.col("campaign_month"), 93))
    )

    df_repayment = df_loan.join(
        df_revenue,
        (df_revenue["msisdn"] == df_loan["msisdn"])
        & (df_revenue["weekstart"] >= df_loan["weekstart"])
        & (df_revenue["campaign_month"] >= df_loan["campaign_month"])
        & (df_revenue["campaign_month"] <= df_loan["due_month"]),
        how="left",
    ).select(
        df_loan["msisdn"],
        df_loan["weekstart"],
        df_loan["campaign_month"],
        df_loan["loan_amount"],
        df_revenue["loan_repayment_transactions"],
        df_revenue["non_loan_revenue"],
        df_revenue["loan_revenue"],
    )

    df_repayment = (
        df_repayment.groupBy("msisdn", "weekstart", "campaign_month", "loan_amount")
        .agg(
            f.sum("loan_revenue").alias("fea_airtime_loan_loan_revenue_4cm"),
            f.sum("loan_repayment_transactions").alias(
                "fea_airtime_loan_repayment_transactions_4cm"
            ),
        )
        .withColumn(
            "fea_airtime_loan_repayment_flag",
            f.when(
                f.col("fea_airtime_loan_repayment_transactions_4cm") > 0, 1
            ).otherwise(0),
        )
    )

    df_target_variable = (
        df_revenue.select(
            "msisdn",
            "weekstart",
            "campaign_month",
            f.coalesce(f.col("non_loan_revenue"), f.lit(0)).alias(
                "fea_airtime_loan_non_loan_revenue_1w"
            ),
            f.col("loan_revenue").alias("fea_airtime_loan_loan_revenue_1w"),
        )
        .join(df_repayment, ["msisdn", "weekstart", "campaign_month"], how="left")
        .withColumn(
            "fea_airtime_loan_proxy_target_variable",
            f.coalesce(f.col("fea_airtime_loan_non_loan_revenue_1w"), f.lit(0))
            + f.coalesce(
                f.col("loan_amount") * f.col("fea_airtime_loan_repayment_flag"),
                f.lit(0),
            )
            + f.coalesce(
                f.lit(0.08)
                * f.col("loan_amount")
                * (f.col("fea_airtime_loan_repayment_flag") - f.lit(1)),
                f.lit(0),
            ),
        )
        .select(
            "msisdn",
            "weekstart",
            "campaign_month",
            "fea_airtime_loan_loan_revenue_1w",
            "fea_airtime_loan_loan_revenue_4cm",
            "fea_airtime_loan_non_loan_revenue_1w",
            f.col("loan_amount").alias("fea_airtime_loan_loan_amount_1w"),
            "fea_airtime_loan_repayment_flag",
            "fea_airtime_loan_repayment_transactions_4cm",
            "fea_airtime_loan_proxy_target_variable",
        )
    )

    dimension_columns = ["msisdn", "weekstart"]

    features_columns = list(
        set(df_target_variable.schema.names) - set(dimension_columns)
    )

    required_output_columns = get_required_output_columns(
        output_features=features_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=dimension_columns,
    )

    df_target_variable = df_target_variable.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return df_target_variable.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )
