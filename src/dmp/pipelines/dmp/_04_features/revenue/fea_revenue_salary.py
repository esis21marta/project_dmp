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
import pyspark.sql.functions as f

from src.dmp.pipelines.dmp._04_features.revenue.fea_revenue_base_payu_pkg import (
    get_tot_rev,
)
from src.dmp.pipelines.dmp._04_features.revenue.revenue_columns import (
    OUTPUT_COLUMNS_SALARY,
)
from utils import get_end_date, get_required_output_columns, get_start_date


def fea_revenue_salary(
    df_revenue_weekly: pyspark.sql.DataFrame,
    df_revenue_alt_weekly: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Calculates interal revenue aligned with salary spend for each msisdn:

    Args:
        df_revenue_weekly: Revenue weekly data
        df_revenue_alt_weekly: Revenue Alt weekly data

    Returns:
        df_features: Dataframe with features, revenue
            - msisdn: Unique Id
            - weekstart: Weekly obervation point
            - fea_monthly_spend_*_pattern_salary_*_over_03m: monthly spend SMS, Voice, Data, & Roam on 1-5 days to 26-31 days in 3m window
    """
    config_feature_revenue = config_feature["revenue"]

    # selecting specific agg columns to improve performance
    df_revenue_weekly = df_revenue_weekly.join(
        df_revenue_alt_weekly, ["msisdn", "weekstart"], how="outer"
    )
    df_revenue_weekly = df_revenue_weekly.select(SELECT_AGG_COLUMNS)

    df_features_helper = get_tot_rev(df_revenue_weekly, config_feature_revenue)

    df_features = (
        df_features_helper.withColumn(
            "fea_monthly_spend_voice_pattern_salary_1_5_over_03m",
            (f.col("rev_voice_tot_1_5d") / f.col("fea_rev_voice_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_voice_pattern_salary_6_10_over_03m",
            (f.col("rev_voice_tot_6_10d") / f.col("fea_rev_voice_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_voice_pattern_salary_11_15_over_03m",
            (f.col("rev_voice_tot_11_15d") / f.col("fea_rev_voice_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_voice_pattern_salary_16_20_over_03m",
            (f.col("rev_voice_tot_16_20d") / f.col("fea_rev_voice_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_voice_pattern_salary_21_25_over_03m",
            (f.col("rev_voice_tot_21_25d") / f.col("fea_rev_voice_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_voice_pattern_salary_26_31_over_03m",
            (f.col("rev_voice_tot_26_31d") / f.col("fea_rev_voice_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_data_pattern_salary_1_5_over_03m",
            (f.col("rev_data_tot_1_5d") / f.col("fea_rev_data_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_data_pattern_salary_6_10_over_03m",
            (f.col("rev_data_tot_6_10d") / f.col("fea_rev_data_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_data_pattern_salary_11_15_over_03m",
            (f.col("rev_data_tot_11_15d") / f.col("fea_rev_data_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_data_pattern_salary_16_20_over_03m",
            (f.col("rev_data_tot_16_20d") / f.col("fea_rev_data_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_data_pattern_salary_21_25_over_03m",
            (f.col("rev_data_tot_21_25d") / f.col("fea_rev_data_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_data_pattern_salary_26_31_over_03m",
            (f.col("rev_data_tot_26_31d") / f.col("fea_rev_data_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_sms_pattern_salary_1_5_over_03m",
            (f.col("rev_sms_tot_1_5d") / f.col("fea_rev_sms_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_sms_pattern_salary_6_10_over_03m",
            (f.col("rev_sms_tot_6_10d") / f.col("fea_rev_sms_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_sms_pattern_salary_11_15_over_03m",
            (f.col("rev_sms_tot_11_15d") / f.col("fea_rev_sms_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_sms_pattern_salary_16_20_over_03m",
            (f.col("rev_sms_tot_16_20d") / f.col("fea_rev_sms_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_sms_pattern_salary_21_25_over_03m",
            (f.col("rev_sms_tot_21_25d") / f.col("fea_rev_sms_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_sms_pattern_salary_26_31_over_03m",
            (f.col("rev_sms_tot_26_31d") / f.col("fea_rev_sms_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_roam_pattern_salary_1_5_over_03m",
            (f.col("rev_roam_tot_1_5d") / f.col("fea_rev_roam_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_roam_pattern_salary_6_10_over_03m",
            (f.col("rev_roam_tot_6_10d") / f.col("fea_rev_roam_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_roam_pattern_salary_11_15_over_03m",
            (f.col("rev_roam_tot_11_15d") / f.col("fea_rev_roam_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_roam_pattern_salary_16_20_over_03m",
            (f.col("rev_roam_tot_16_20d") / f.col("fea_rev_roam_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_roam_pattern_salary_21_25_over_03m",
            (f.col("rev_roam_tot_21_25d") / f.col("fea_rev_roam_tot_sum_03m")),
        )
        .withColumn(
            "fea_monthly_spend_roam_pattern_salary_26_31_over_03m",
            (f.col("rev_roam_tot_26_31d") / f.col("fea_rev_roam_tot_sum_03m")),
        )
    )

    required_output_columns = get_required_output_columns(
        output_features=OUTPUT_COLUMNS_SALARY,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    df_features = df_features.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return df_features.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )


SELECT_AGG_COLUMNS = [
    "msisdn",
    "weekstart",
    "rev_voice",
    "rev_data",
    "rev_sms",
    "rev_payu_roam",
    "rev_pkg_roam",
    "rev_payu_tot",
    "rev_pkg_tot",
    "rev_voice_tot_1_5d",
    "rev_voice_tot_6_10d",
    "rev_voice_tot_11_15d",
    "rev_voice_tot_16_20d",
    "rev_voice_tot_21_25d",
    "rev_voice_tot_26_31d",
    "rev_data_tot_1_5d",
    "rev_data_tot_6_10d",
    "rev_data_tot_11_15d",
    "rev_data_tot_16_20d",
    "rev_data_tot_21_25d",
    "rev_data_tot_26_31d",
    "rev_sms_tot_1_5d",
    "rev_sms_tot_6_10d",
    "rev_sms_tot_11_15d",
    "rev_sms_tot_16_20d",
    "rev_sms_tot_21_25d",
    "rev_sms_tot_26_31d",
    "rev_roam_tot_1_5d",
    "rev_roam_tot_6_10d",
    "rev_roam_tot_11_15d",
    "rev_roam_tot_16_20d",
    "rev_roam_tot_21_25d",
    "rev_roam_tot_26_31d",
    "rev_alt_total",
    "rev_alt_voice",
    "rev_alt_data",
    "rev_alt_sms",
    "rev_alt_digital",
    "rev_alt_roam",
    "non_loan_revenue",
    "loan_revenue",
    "loan_repayment_transactions",
]
