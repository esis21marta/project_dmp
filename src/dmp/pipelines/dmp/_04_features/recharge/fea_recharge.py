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

import math
from typing import List

import pyspark
import pyspark.sql.functions as f

from utils import (
    avg_monthly_over_weekstart_window,
    avg_over_weekstart_window,
    calculate_weeks_since_last_activity,
    division_of_columns,
    get_config_based_features,
    get_config_based_features_column_names,
    get_end_date,
    get_required_output_columns,
    get_rolling_window,
    get_start_date,
    join_all,
    min_over_weekstart_window,
    ratio_of_columns,
    std_deviation_of_column_over_weekstart_window,
    sum_of_columns_over_weekstart_window,
)


def fea_recharge(
    df_rech_weekly: pyspark.sql.DataFrame,
    df_digi_rech_weekly: pyspark.sql.DataFrame,
    df_acc_bal_weekly: pyspark.sql.DataFrame,
    df_chg_pkg_prchse_weekly: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Calculates features on topup behaviour:
        - df_rech_weekly table is already aggregated on msisdn, weekstart
        - For each msisdn, weekstart each feature is calculated over a window using get_rolling_window(time_period, orderByColumn)
        - A feature at a given weekstart represents the value of that feature looking n weeks till the current week
          (where n represents the size of the window)

    Args:
        df_rech_weekly: Recharges weekly level table
        df_acc_bal_weekly: Account balance table aggregated weekly
        df_chg_pkg_prchse_weekly: charge packs weekly table

    Returns:
        df_topup_trx: Dataframe with following features
            - msisdn: Unique Id
            - weekstart: Weekly obervation point
            - fea_rech_tot_amt_sum_01w: Total amount of recharge in last 1 week
            - fea_rech_tot_trx_sum_01w: Total number of times topped qqqup in last 1 week
            - fea_rech_tot_amt_sum_03m: Total amount recharged in last 3 months
            - fea_rech_tot_amt_sum_02m: Total amount recharged in last 3 months
            - fea_rech_tot_amt_sum_01m: Total amount recharged in last 2 months
            - fea_rech_tot_amt_sum_01w: Total amount recharged in last 1 months
            - fea_rech_tot_trx_sum_03m: Total amount recharged in last 7 days
            - fea_rech_tot_trx_sum_02m: Total number of times recharged in last 3 months
            - fea_rech_tot_trx_sum_01m: Total number of times recharged in last 2 months
            - fea_rech_tot_trx_sum_01w: Total number of times recharged in last 1 months
            - fea_rech_tot_amt_avg_01m: Topup average for last 1 months
            - fea_rech_tot_amt_avg_02m: Topup average for last 2 months
            - fea_rech_tot_amt_avg_03m: Topup average for last 3 months
            - fea_rech_tot_amt_avg_01w: Topup average for last 7 days
            - fea_rech_tot_amt_trd_03m: Topup amount trend for last 3 months
            - fea_rech_tot_trx_trd_03m: Topup transaction (frequency of topup) for last 3 months
            - fea_rech_tot_amt_stddev_03: Standard dev of topup over last 3 months
            - fea_rech_tot_days_no_bal_sum_01w: Count of days with no balance for last 1 week
            - fea_rech_tot_days_no_bal_sum_02w: Count of days with no balance for last 2 weeks
            - fea_rech_tot_days_no_bal_sum_03w: Count of days with no balance for last 3 weeks
            - fea_rech_tot_days_no_bal_sum_01m: Count of days with no balance for last 1 month
            - fea_rech_tot_days_no_bal_sum_02m: Count of days with no balance for last 2 months
            - fea_rech_tot_days_no_bal_sum_03m: Count of days with no balance for last 3 months
            - fea_rech_bal_min_01w: Minimum balance for last 1 week
            - fea_rech_bal_min_02w: Minimum balance for last 2 weeks
            - fea_rech_bal_min_03w: Minimum balance for last 3 weeks
            - fea_rech_bal_min_01m: Minimum balance for last 1 month
            - fea_rech_bal_min_02m: Minimum balance for last 2 months
            - fea_rech_bal_min_03m: Minimum balance for last 3 months
            - fea_rech_bal_weekly_avg_01w: Balance weekly average for last 1 week
            - fea_rech_bal_weekly_avg_02w: Balance weekly average for last 2 weeks
            - fea_rech_bal_weekly_avg_03w: Balance weekly average for last 3 weeks
            - fea_rech_bal_weekly_avg_01m: Balance weekly average for last 1 month
            - fea_rech_bal_weekly_avg_02m: Balance weekly average for last 2 months
            - fea_rech_bal_weekly_avg_03m: Balance weekly average for last 3 months
            - fea_rech_bal_below_500_01w: Balance below 500 for last 1 week
            - fea_rech_bal_below_500_02w: Balance below 500 for last 2 weeks
            - fea_rech_bal_below_500_03w: Balance below 500 for last 3 weeks
            - fea_rech_bal_below_500_01m: Balance below 500 for last 1 month
            - fea_rech_bal_below_500_02m: Balance below 500 for last 2 months
            - fea_rech_bal_below_500_03m: Balance below 500 for last 3 months
            - fea_rech_bal_monthly_avg_01w: Balance monthly average for last 1 week
            - fea_rech_bal_monthly_avg_02w: Balance monthly average for last 2 weeks
            - fea_rech_bal_monthly_avg_03w: Balance monthly average for last 3 weeks
            - fea_rech_bal_monthly_avg_01m: Balance monthly average for last 1 month
            - fea_rech_bal_monthly_avg_02m: Balance monthly average for last 2 months
            - fea_rech_bal_monthly_avg_03m: Balance monthly average for last 3 months
            - fea_rech_tot_days_bal_more_than_5000_01w: Total days with balance more than 5000 for last 1 week
            - fea_rech_tot_days_bal_more_than_5000_02w: Total days with balance more than 5000 for last 2 weeks
            - fea_rech_tot_days_bal_more_than_5000_03w: Total days with balance more than 5000 for last 3 weeks
            - fea_rech_tot_days_bal_more_than_5000_01m: Total days with balance more than 5000 for last 1 month
            - fea_rech_tot_days_bal_more_than_5000_02m: Total days with balance more than 5000 for last 2 months
            - fea_rech_tot_days_bal_more_than_5000_03m: Total days with balance more than 5000 for last 3 months
            - fea_rech_tot_days_bal_more_than_10000_01w: Total days with balance more than 10000 for last 1 week
            - fea_rech_tot_days_bal_more_than_10000_02w: Total days with balance more than 10000 for last 2 weeks
            - fea_rech_tot_days_bal_more_than_10000_03w: Total days with balance more than 10000 for last 3 weeks
            - fea_rech_tot_days_bal_more_than_10000_01m: Total days with balance more than 10000 for last 1 month
            - fea_rech_tot_days_bal_more_than_10000_02m: Total days with balance more than 10000 for last 2 months
            - fea_rech_tot_days_bal_more_than_10000_03m: Total days with balance more than 10000 for last 3 months
            - fea_rech_tot_days_bal_more_than_20000_01w: Total days with balance more than 20000 for last 1 week
            - fea_rech_tot_days_bal_more_than_20000_02w: Total days with balance more than 20000 for last 2 weeks
            - fea_rech_tot_days_bal_more_than_20000_03w: Total days with balance more than 20000 for last 3 weeks
            - fea_rech_tot_days_bal_more_than_20000_01m: Total days with balance more than 20000 for last 1 month
            - fea_rech_tot_days_bal_more_than_20000_02m: Total days with balance more than 20000 for last 2 months
            - fea_rech_tot_days_bal_more_than_20000_03m: Total days with balance more than 20000 for last 3 months
            - fea_rech_tot_days_bal_more_than_50000_01w: Total days with balance more than 50000 for last 1 week
            - fea_rech_tot_days_bal_more_than_50000_02w: Total days with balance more than 50000 for last 2 weeks
            - fea_rech_tot_days_bal_more_than_50000_03w: Total days with balance more than 50000 for last 3 weeks
            - fea_rech_tot_days_bal_more_than_50000_01m: Total days with balance more than 50000 for last 1 month
            - fea_rech_tot_days_bal_more_than_50000_02m: Total days with balance more than 50000 for last 2 months
            - fea_rech_tot_days_bal_more_than_50000_03m: Total days with balance more than 50000 for last 3 months
            - fea_rech_tot_days_bal_below_500_01w: Total days with balance below 500 for last 1 week
            - fea_rech_tot_days_bal_below_500_02w: Total days with balance below 500 for last 2 weeks
            - fea_rech_tot_days_bal_below_500_03w: Total days with balance below 500 for last 3 weeks
            - fea_rech_tot_days_bal_below_500_01m: Total days with balance below 500 for last 1 month
            - fea_rech_tot_days_bal_below_500_02m: Total days with balance below 500 for last 2 months
            - fea_rech_tot_days_bal_below_500_03m: Total days with balance below 500 for last 3 months
            - fea_rech_tot_days_bal_more_than_20000_below_40000_01w: Total days with balance more than 20000 below 40000 for last 1 week
            - fea_rech_tot_days_bal_more_than_20000_below_40000_02w: Total days with balance more than 20000 below 40000 for last 2 weeks
            - fea_rech_tot_days_bal_more_than_20000_below_40000_03w: Total days with balance more than 20000 below 40000 for last 3 weeks
            - fea_rech_tot_days_bal_more_than_20000_below_40000_01m: Total days with balance more than 20000 below 40000 for last 1 month
            - fea_rech_tot_days_bal_more_than_20000_below_40000_02m: Total days with balance more than 20000 below 40000 for last 2 months
            - fea_rech_tot_days_bal_more_than_20000_below_40000_03m: Total days with balance more than 20000 below 40000 for last 3 months
            - fea_rech_tot_days_bal_more_than_40000_below_60000_01w: Total days with balance more than 40000 below 60000 for last 1 week
            - fea_rech_tot_days_bal_more_than_40000_below_60000_02w: Total days with balance more than 40000 below 60000 for last 2 weeks
            - fea_rech_tot_days_bal_more_than_40000_below_60000_03w: Total days with balance more than 40000 below 60000 for last 3 weeks
            - fea_rech_tot_days_bal_more_than_40000_below_60000_01m: Total days with balance more than 40000 below 60000 for last 1 month
            - fea_rech_tot_days_bal_more_than_40000_below_60000_02m: Total days with balance more than 40000 below 60000 for last 2 months
            - fea_rech_tot_days_bal_more_than_40000_below_60000_03m: Total days with balance more than 40000 below 60000 for last 3 months
            - fea_rech_tot_days_bal_more_than_20000_below_60000_01w: Total days with balance more than 20000 below 60000 for last 1 week
            - fea_rech_tot_days_bal_more_than_20000_below_60000_02w: Total days with balance more than 20000 below 60000 for last 2 weeks
            - fea_rech_tot_days_bal_more_than_20000_below_60000_03w: Total days with balance more than 20000 below 60000 for last 3 weeks
            - fea_rech_tot_days_bal_more_than_20000_below_60000_01m: Total days with balance more than 20000 below 60000 for last 1 month
            - fea_rech_tot_days_bal_more_than_20000_below_60000_02m: Total days with balance more than 20000 below 60000 for last 2 months
            - fea_rech_tot_days_bal_more_than_20000_below_60000_03m: Total days with balance more than 20000 below 60000 for last 3 months
            - fea_rech_num_days_past_since_bal_more_than_5000_01w: Number of days past since balance more than 5000 for last 1 week
            - fea_rech_num_days_past_since_bal_more_than_5000_02w: Number of days past since balance more than 5000 for last 2 weeks
            - fea_rech_num_days_past_since_bal_more_than_5000_03w: Number of days past since balance more than 5000 for last 3 weeks
            - fea_rech_num_days_past_since_bal_more_than_5000_01m: Number of days past since balance more than 5000 for last 1 month
            - fea_rech_num_days_past_since_bal_more_than_5000_02m: Number of days past since balance more than 5000 for last 2 months
            - fea_rech_num_days_past_since_bal_more_than_5000_03m: Number of days past since balance more than 5000 for last 3 months
            - fea_rech_num_days_past_since_bal_more_than_10000_01w: Number of days past since balance more than 10000 for last 1 week
            - fea_rech_num_days_past_since_bal_more_than_10000_02w: Number of days past since balance more than 10000 for last 2 weeks
            - fea_rech_num_days_past_since_bal_more_than_10000_03w: Number of days past since balance more than 10000 for last 3 weeks
            - fea_rech_num_days_past_since_bal_more_than_10000_01m: Number of days past since balance more than 10000 for last 1 month
            - fea_rech_num_days_past_since_bal_more_than_10000_02m: Number of days past since balance more than 10000 for last 2 months
            - fea_rech_num_days_past_since_bal_more_than_10000_03m: Number of days past since balance more than 10000 for last 3 months
            - fea_rech_num_days_past_since_bal_more_than_20000_01w: Number of days past since balance more than 20000 for last 1 week
            - fea_rech_num_days_past_since_bal_more_than_20000_02w: Number of days past since balance more than 20000 for last 2 weeks
            - fea_rech_num_days_past_since_bal_more_than_20000_03w: Number of days past since balance more than 20000 for last 3 weeks
            - fea_rech_num_days_past_since_bal_more_than_20000_01m: Number of days past since balance more than 20000 for last 1 month
            - fea_rech_num_days_past_since_bal_more_than_20000_02m: Number of days past since balance more than 20000 for last 2 months
            - fea_rech_num_days_past_since_bal_more_than_20000_03m: Number of days past since balance more than 20000 for last 3 months
            - fea_rech_num_days_past_since_bal_more_than_50000_01w: Number of days past since balance more than 50000 for last 1 week
            - fea_rech_num_days_past_since_bal_more_than_50000_02w: Number of days past since balance more than 50000 for last 2 weeks
            - fea_rech_num_days_past_since_bal_more_than_50000_03w: Number of days past since balance more than 50000 for last 3 weeks
            - fea_rech_num_days_past_since_bal_more_than_50000_01m: Number of days past since balance more than 50000 for last 1 month
            - fea_rech_num_days_past_since_bal_more_than_50000_02m: Number of days past since balance more than 50000 for last 2 months
            - fea_rech_num_days_past_since_bal_more_than_50000_03m: Number of days past since balance more than 50000 for last 3 months
            - fea_rech_num_days_past_since_bal_5000_or_less_01w: Number of days past since balance 5000 or less for last 1 week
            - fea_rech_num_days_past_since_bal_5000_or_less_02w: Number of days past since balance 5000 or less for last 2 weeks
            - fea_rech_num_days_past_since_bal_5000_or_less_03w: Number of days past since balance 5000 or less for last 3 weeks
            - fea_rech_num_days_past_since_bal_5000_or_less_01m: Number of days past since balance 5000 or less for last 1 month
            - fea_rech_num_days_past_since_bal_5000_or_less_02m: Number of days past since balance 5000 or less for last 2 months
            - fea_rech_num_days_past_since_bal_5000_or_less_03m: Number of days past since balance 5000 or less for last 3 months
            - fea_rech_num_days_past_since_bal_10000_or_less_01w: Number of days past since balance 10000 or less for last 1 week
            - fea_rech_num_days_past_since_bal_10000_or_less_02w: Number of days past since balance 10000 or less for last 2 weeks
            - fea_rech_num_days_past_since_bal_10000_or_less_03w: Number of days past since balance 10000 or less for last 3 weeks
            - fea_rech_num_days_past_since_bal_10000_or_less_01m: Number of days past since balance 10000 or less for last 1 month
            - fea_rech_num_days_past_since_bal_10000_or_less_02m: Number of days past since balance 10000 or less for last 2 months
            - fea_rech_num_days_past_since_bal_10000_or_less_03m: Number of days past since balance 10000 or less for last 3 months
            - fea_rech_num_days_past_since_bal_20000_or_less_01w: Number of days past since balance 20000 or less for last 1 week
            - fea_rech_num_days_past_since_bal_20000_or_less_02w: Number of days past since balance 20000 or less for last 2 weeks
            - fea_rech_num_days_past_since_bal_20000_or_less_03w: Number of days past since balance 20000 or less for last 3 weeks
            - fea_rech_num_days_past_since_bal_20000_or_less_01m: Number of days past since balance 20000 or less for last 1 month
            - fea_rech_num_days_past_since_bal_20000_or_less_02m: Number of days past since balance 20000 or less for last 2 months
            - fea_rech_num_days_past_since_bal_20000_or_less_03m: Number of days past since balance 20000 or less for last 3 months
            - fea_rech_num_days_past_since_bal_50000_or_less_01w: Number of days past since balance 50000 or less for last 1 week
            - fea_rech_num_days_past_since_bal_50000_or_less_02w: Number of days past since balance 50000 or less for last 2 weeks
            - fea_rech_num_days_past_since_bal_50000_or_less_03w: Number of days past since balance 50000 or less for last 3 weeks
            - fea_rech_num_days_past_since_bal_50000_or_less_01m: Number of days past since balance 50000 or less for last 1 month
            - fea_rech_num_days_past_since_bal_50000_or_less_02m: Number of days past since balance 50000 or less for last 2 months
            - fea_rech_num_days_past_since_bal_50000_or_less_03m: Number of days past since balance 50000 or less for last 3 months
            - fea_rech_last_bal_before_rech_avg_01w: Average last balance before recharge for last 1 week
            - fea_rech_last_bal_before_rech_avg_02w: Average last balance before recharge for last 2 weeks
            - fea_rech_last_bal_before_rech_avg_03w: Average last balance before recharge for last 3 weeks
            - fea_rech_last_bal_before_rech_avg_01m: Average last balance before recharge for last 1 month
            - fea_rech_last_bal_before_rech_avg_02m: Average last balance before recharge for last 2 months
            - fea_rech_last_bal_before_rech_avg_03m: Average last balance before recharge for last 3 months

    """

    config_feature_recharge = config_feature["recharge"]

    df_rech_weekly = df_rech_weekly.join(
        df_digi_rech_weekly, ["msisdn", "weekstart"], how="left"
    )

    df_rech_weekly.cache()

    df_fea = df_rech_weekly.withColumnRenamed("tot_amt", "recharge")

    #### FEATURE IMPLEMENTATION : Topup Behavior
    df_topup_behav = join_all(
        [
            df_rech_weekly,
            get_config_based_features(
                df=df_rech_weekly,
                feature_config=config_feature_recharge["fea_rech_tot_amt_sum"],
                column_expression=sum_of_columns_over_weekstart_window(["tot_amt"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_rech_weekly,
                feature_config=config_feature_recharge["fea_rech_tot_trx_sum"],
                column_expression=sum_of_columns_over_weekstart_window(["tot_trx"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_rech_weekly,
                feature_config=config_feature_recharge["fea_rech_amt_weekly_avg"],
                column_expression=avg_over_weekstart_window("tot_amt"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_rech_weekly,
                feature_config=config_feature_recharge["fea_rech_amt_monthly_avg"],
                column_expression=avg_monthly_over_weekstart_window("tot_amt"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_rech_weekly,
                feature_config=config_feature_recharge["fea_rech_tot_amt_digi_sum"],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["tot_amt_digi"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            calculate_weeks_since_last_activity(
                df_fea,
                "recharge",
                lookback_weeks=11,
                primary_cols=["msisdn", "weekstart"],
                feature_col="fea_rech_weeks_since_last_recharge",
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    df_topup_behav = join_all(
        [
            df_topup_behav,
            get_config_based_features(
                df=df_topup_behav,
                feature_config=config_feature_recharge["fea_rech_tot_amt_avg"],
                column_expression=division_of_columns(
                    "fea_rech_tot_amt_sum_{period_string}",
                    "fea_rech_tot_trx_sum_{period_string}",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_topup_behav,
                feature_config=config_feature_recharge["fea_rech_tot_amt_digi_ratio"],
                column_expression=division_of_columns(
                    "fea_rech_tot_amt_digi_sum_{period_string}",
                    "fea_rech_tot_amt_sum_{period_string}",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    df_topup_behav = join_all(
        [
            df_topup_behav,
            get_config_based_features(
                df=df_topup_behav,
                feature_config=config_feature_recharge["fea_rech_tot_amt_digi_trends"],
                column_expression=ratio_of_columns(
                    "fea_rech_tot_amt_digi_ratio_{period_string}",
                    "fea_rech_tot_amt_digi_ratio_01w",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_topup_behav,
                feature_config=config_feature_recharge["fea_rech_tot_amt_trd"],
                column_expression=division_of_columns(
                    "fea_rech_tot_amt_avg_01m", "fea_rech_tot_amt_avg_{period_string}"
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_topup_behav,
                feature_config=config_feature_recharge["fea_rech_tot_trx_trd"],
                column_expression=division_of_columns(
                    "fea_rech_tot_trx_sum_01m", "fea_rech_tot_trx_sum_{period_string}"
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_topup_behav,
                feature_config=config_feature_recharge["fea_rech_tot_amt_stddev"],
                column_expression=std_deviation_of_column_over_weekstart_window(
                    "fea_rech_tot_amt_avg_01w"
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    #### FEATURE IMPLEMENTATION : No Balance
    def fea_rech_tot_days_no_bal_avg(period_string, period):
        return f.col(f"fea_rech_tot_days_no_bal_sum_{period_string}") / math.floor(
            period / 28
        )  # It is monthly average

    def fea_rech_num_days_past(column_name):
        def expression(period_string, period):
            if period == 7:
                return f.datediff(
                    f.to_date(f.col("weekstart"), "yyyy-MM-dd"),
                    f.to_date(f.col(eval(f'f"""{column_name}"""')), "yyyy-MM-dd"),
                )
            else:
                return f.datediff(
                    f.to_date(f.col("weekstart"), "yyyy-MM-dd"),
                    f.to_date(
                        f.max(eval(f'f"""{column_name}"""')).over(
                            get_rolling_window(period, oby="weekstart")
                        ),
                        "yyyy-MM-dd",
                    ),
                )

        return expression

    df_bal = join_all(
        [
            df_acc_bal_weekly,
            get_config_based_features(
                df=df_acc_bal_weekly,
                feature_config=config_feature_recharge["fea_rech_tot_days_no_bal_sum"],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["zero_or_neg_count"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_acc_bal_weekly,
                feature_config=config_feature_recharge["fea_rech_bal_min"],
                column_expression=min_over_weekstart_window("account_balance_min"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_acc_bal_weekly,
                feature_config=config_feature_recharge["fea_rech_bal_weekly_avg"],
                column_expression=avg_over_weekstart_window("account_balance_avg"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_acc_bal_weekly,
                feature_config=config_feature_recharge["fea_rech_bal_weekly_below_500"],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["account_balance_below_500_count"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_acc_bal_weekly,
                feature_config=config_feature_recharge["fea_rech_bal_monthly_avg"],
                column_expression=avg_monthly_over_weekstart_window(
                    "account_balance_avg"
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_acc_bal_weekly,
                feature_config=config_feature_recharge[
                    "fea_rech_last_bal_before_rech_avg"
                ],
                column_expression=avg_over_weekstart_window("avg_last_bal_before_rech"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_acc_bal_weekly,
                feature_config=config_feature_recharge[
                    "fea_rech_tot_days_bal_below_500"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["num_days_bal_below_500"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_acc_bal_weekly,
                feature_config=config_feature_recharge[
                    "fea_rech_tot_days_bal_more_than_20000_below_40000"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["num_days_bal_more_than_20000_below_40000"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_acc_bal_weekly,
                feature_config=config_feature_recharge[
                    "fea_rech_tot_days_bal_more_than_40000_below_60000"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["num_days_bal_more_than_40000_below_60000"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_acc_bal_weekly,
                feature_config=config_feature_recharge[
                    "fea_rech_tot_days_bal_more_than_20000_below_60000"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["num_days_bal_more_than_20000_below_60000"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    df_bal = get_config_based_features(
        df=df_bal,
        feature_config=config_feature_recharge["fea_rech_tot_days_no_bal_avg"],
        column_expression=fea_rech_tot_days_no_bal_avg,
    )

    bins_idr = [5000, 10000, 20000, 50000]
    fea_bal_selection_with_bins = []
    for bin in bins_idr:
        df_bal = join_all(
            [
                df_bal,
                get_config_based_features(
                    df=df_acc_bal_weekly,
                    feature_config=config_feature_recharge[
                        f"fea_rech_tot_days_bal_more_than_{bin}"
                    ],
                    column_expression=sum_of_columns_over_weekstart_window(
                        [f"num_days_bal_more_than_{bin}"]
                    ),
                    return_only_feature_columns=True,
                    columns_to_keep_from_original_df=["msisdn", "weekstart"],
                ),
                get_config_based_features(
                    df=df_acc_bal_weekly,
                    feature_config=config_feature_recharge[
                        f"fea_rech_num_days_past_since_bal_more_than_{bin}"
                    ],
                    column_expression=fea_rech_num_days_past(
                        f"max_date_with_bal_more_than_{bin}"
                    ),
                    return_only_feature_columns=True,
                    columns_to_keep_from_original_df=["msisdn", "weekstart"],
                ),
                get_config_based_features(
                    df=df_acc_bal_weekly,
                    feature_config=config_feature_recharge[
                        f"fea_rech_num_days_past_since_bal_{bin}_or_less"
                    ],
                    column_expression=fea_rech_num_days_past(
                        f"max_date_with_bal_{bin}_or_less"
                    ),
                    return_only_feature_columns=True,
                    columns_to_keep_from_original_df=["msisdn", "weekstart"],
                ),
            ],
            on=["msisdn", "weekstart"],
            how="outer",
        )

        fea_bal_selection_with_bins.extend(
            [
                config_feature_recharge[f"fea_rech_tot_days_bal_more_than_{bin}"],
                config_feature_recharge[
                    f"fea_rech_num_days_past_since_bal_more_than_{bin}"
                ],
                config_feature_recharge[
                    f"fea_rech_num_days_past_since_bal_{bin}_or_less"
                ],
            ]
        )

    df_bal = df_bal.withColumn("fea_rech_current_balance", f.col("current_balance"))

    #### FEATURE IMPLEMENTATION : Package Purchase
    def fea_rech_trx_pkg_prchse_avg(period_string, period):
        return f.col(f"fea_rech_trx_pkg_prchse_sum_{period_string}") / (period / 7)

    def fea_rech_rev_pkg_prchse_avg(period_string, period):
        return f.col(f"fea_rech_rev_pkg_prchse_sum_{period_string}") / (period / 7)

    def fea_rech_trx_pkg_prchse_stddev(period_string, period):
        return f.stddev("trx_pkg_prchse").over(
            get_rolling_window(7 * 13, oby="weekstart")
        ) * f.col("fea_rech_annualize_factor")

    def fea_rech_rev_pkg_prchse_stddev(period_string, period):
        return f.stddev("rev_pkg_prchse").over(
            get_rolling_window(7 * 13, oby="weekstart")
        ) * f.col("fea_rech_annualize_factor")

    df_fea_pkg_prchse = get_config_based_features(
        df=df_chg_pkg_prchse_weekly,
        feature_config=config_feature_recharge["fea_rech_trx_pkg_prchse_sum"],
        column_expression=sum_of_columns_over_weekstart_window(["trx_pkg_prchse"]),
    )

    df_fea_pkg_prchse = get_config_based_features(
        df=df_fea_pkg_prchse,
        feature_config=config_feature_recharge["fea_rech_trx_pkg_prchse_avg"],
        column_expression=fea_rech_trx_pkg_prchse_avg,
    )

    df_fea_pkg_prchse = get_config_based_features(
        df=df_fea_pkg_prchse,
        feature_config=config_feature_recharge["fea_rech_rev_pkg_prchse_sum"],
        column_expression=sum_of_columns_over_weekstart_window(["rev_pkg_prchse"]),
    )

    df_fea_pkg_prchse = get_config_based_features(
        df=df_fea_pkg_prchse,
        feature_config=config_feature_recharge["fea_rech_rev_pkg_prchse_avg"],
        column_expression=fea_rech_rev_pkg_prchse_avg,
    )

    df_fea_pkg_prchse = get_config_based_features(
        df=df_fea_pkg_prchse,
        feature_config=config_feature_recharge["fea_rech_rev_pkg_prchse_avg"],
        column_expression=fea_rech_rev_pkg_prchse_avg,
    )

    df_fea_pkg_prchse = df_fea_pkg_prchse.withColumn(
        "fea_rech_annualize_factor", f.lit(math.sqrt(52.0))
    )

    df_fea_pkg_prchse = get_config_based_features(
        df=df_fea_pkg_prchse,
        feature_config=config_feature_recharge["fea_rech_trx_pkg_prchse_stddev"],
        column_expression=fea_rech_trx_pkg_prchse_stddev,
    )

    df_fea_pkg_prchse = get_config_based_features(
        df=df_fea_pkg_prchse,
        feature_config=config_feature_recharge["fea_rech_rev_pkg_prchse_stddev"],
        column_expression=fea_rech_rev_pkg_prchse_stddev,
    )

    df_fea_pkg_prchse = get_config_based_features(
        df=df_fea_pkg_prchse,
        feature_config=config_feature_recharge["fea_rech_rev_pkg_prchse_product_val"],
        column_expression=division_of_columns(
            "fea_rech_rev_pkg_prchse_sum_{period_string}",
            "fea_rech_trx_pkg_prchse_sum_{period_string}",
        ),
    )

    #### FEATURE SELECTION : Topup Behavior
    topup_feature_columns = [
        "fea_rech_weeks_since_last_recharge",
        *get_config_based_features_column_names(
            config_feature_recharge["fea_rech_tot_amt_sum"],
            config_feature_recharge["fea_rech_tot_trx_sum"],
            config_feature_recharge["fea_rech_tot_amt_avg"],
            config_feature_recharge["fea_rech_tot_amt_trd"],
            config_feature_recharge["fea_rech_tot_trx_trd"],
            config_feature_recharge["fea_rech_tot_amt_stddev"],
            config_feature_recharge["fea_rech_amt_weekly_avg"],
            config_feature_recharge["fea_rech_amt_monthly_avg"],
            config_feature_recharge["fea_rech_tot_amt_digi_ratio"],
            config_feature_recharge["fea_rech_tot_amt_digi_trends"],
        ),
    ]

    required_topup_output_columns = get_required_output_columns(
        output_features=topup_feature_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    df_topup_behav = df_topup_behav.select(required_topup_output_columns)

    #### FEATURE SELECTION : Balance
    bal_feature_columns = [
        "fea_rech_current_balance",
        *get_config_based_features_column_names(
            config_feature_recharge["fea_rech_tot_days_no_bal_sum"],
            config_feature_recharge["fea_rech_tot_days_no_bal_avg"],
            config_feature_recharge["fea_rech_bal_min"],
            config_feature_recharge["fea_rech_bal_weekly_avg"],
            config_feature_recharge["fea_rech_bal_weekly_below_500"],
            config_feature_recharge["fea_rech_bal_monthly_avg"],
            config_feature_recharge["fea_rech_last_bal_before_rech_avg"],
            config_feature_recharge["fea_rech_tot_days_bal_below_500"],
            config_feature_recharge[
                "fea_rech_tot_days_bal_more_than_20000_below_40000"
            ],
            config_feature_recharge[
                "fea_rech_tot_days_bal_more_than_40000_below_60000"
            ],
            config_feature_recharge[
                "fea_rech_tot_days_bal_more_than_20000_below_60000"
            ],
            *fea_bal_selection_with_bins,
        ),
    ]

    required_bal_output_columns = get_required_output_columns(
        output_features=bal_feature_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    df_bal = df_bal.select(required_bal_output_columns)

    #### FEATURE SELECTION : Package Purchase
    pkg_prchse_feature_columns = [
        "fea_rech_annualize_factor",
        *get_config_based_features_column_names(
            config_feature_recharge["fea_rech_trx_pkg_prchse_sum"],
            config_feature_recharge["fea_rech_trx_pkg_prchse_avg"],
            config_feature_recharge["fea_rech_rev_pkg_prchse_sum"],
            config_feature_recharge["fea_rech_rev_pkg_prchse_avg"],
            config_feature_recharge["fea_rech_rev_pkg_prchse_avg"],
            config_feature_recharge["fea_rech_trx_pkg_prchse_stddev"],
            config_feature_recharge["fea_rech_rev_pkg_prchse_stddev"],
            config_feature_recharge["fea_rech_rev_pkg_prchse_product_val"],
        ),
    ]

    required_pkg_prchse_output_columns = get_required_output_columns(
        output_features=pkg_prchse_feature_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    df_fea_pkg_prchse = df_fea_pkg_prchse.select(required_pkg_prchse_output_columns)

    #### FILTER WEEKSTART
    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    df_topup_behav = df_topup_behav.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )
    df_bal = df_bal.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )
    df_fea_pkg_prchse = df_fea_pkg_prchse.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )

    #### JOIN
    fea_df = join_all(
        [df_topup_behav, df_bal, df_fea_pkg_prchse],
        on=["msisdn", "weekstart"],
        how="full",
    )

    return fea_df
