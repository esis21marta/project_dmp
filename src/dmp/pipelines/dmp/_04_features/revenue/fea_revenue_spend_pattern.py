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
    get_base_payu_pkg_rev,
)
from utils import (
    calculate_weeks_since_last_activity,
    division_of_columns,
    get_config_based_features,
    get_config_based_features_column_names,
    get_end_date,
    get_required_output_columns,
    get_rolling_window,
    get_start_date,
    get_window_pby_msisdn_oby_trx_date,
    join_all,
)


def fea_revenue_spend_pattern(
    df_revenue_weekly: pyspark.sql.DataFrame,
    df_revenue_alt_weekly: pyspark.sql.DataFrame,
    df_recharge_weekly: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Calculates internal spend pattern revenue for each msisdn:

    Args:
        df_revenue_weekly: Revenue weekly data
        df_revenue_alt_weekly: Revenue Alt weekly data
        df_recharge_weekly: Recharge weekly data

    Returns:
        df_features: Dataframe with features, revenue
            - msisdn: Unique Id
            - weekstart: Weekly obervation point
            - fea_days_since_last_rev_*: days since last payu, package, & total revenue generated
            - fea_ratio_days_since_last_pkg_by_last_rech_*d: ratio over the weekend (Saturday & Sunday) in the window
            - fea_ratio_*_payu_by_pkg_avg_*d: average ratio of revenue payu divided by package in the window
            - fea_ratio_roam_by_tot_avg_*d: average ratio of revenue roam divided by total revenue in the window
    """

    config_feature_revenue = config_feature["revenue"]

    # selecting specific agg columns to improve performance
    df_revenue_weekly = df_revenue_weekly.join(
        df_revenue_alt_weekly, ["msisdn", "weekstart"], how="outer"
    )
    df_revenue_weekly = df_revenue_weekly.select(SELECT_AGG_COLUMNS)

    df_recharge_weekly = df_recharge_weekly.withColumn(
        "last_rech_date", f.when(f.col("tot_amt") > 0, f.col("weekstart"))
    )

    df_join = df_revenue_weekly.join(
        df_recharge_weekly, ["msisdn", "weekstart"], how="left"
    )

    df_features_helper = (
        df_join.withColumn(
            "last_rev_payu",
            f.when(
                f.col("max_date_payu").isNull(),
                f.max(f.col("max_date_payu")).over(
                    get_rolling_window(-1, oby="weekstart")
                ),
            ).otherwise(f.col("max_date_payu")),
        )
        .withColumn(
            "last_rev_pkg",
            f.when(
                f.col("max_date_pkg").isNull(),
                f.max(f.col("max_date_pkg")).over(
                    get_rolling_window(-1, oby="weekstart")
                ),
            ).otherwise(f.col("max_date_pkg")),
        )
        .withColumn(
            "last_rev_tot",
            f.when(
                f.col("max_date_tot").isNull(),
                f.max(f.col("max_date_tot")).over(
                    get_rolling_window(-1, oby="weekstart")
                ),
            ).otherwise(f.col("max_date_tot")),
        )
        .withColumn(
            "max_last_rech_date",
            f.last(f.col("last_rech_date"), ignorenulls=True).over(
                get_window_pby_msisdn_oby_trx_date("msisdn", "weekstart")
            ),
        )
        .withColumn(
            "prev_date_rech",
            f.lag(f.col("max_last_rech_date")).over(
                get_window_pby_msisdn_oby_trx_date("msisdn", "weekstart")
            ),
        )
        .withColumn(
            "days_since_last_rech",
            f.datediff(f.col("weekstart"), f.col("prev_date_rech")),
        )
    )

    df_features_helper = get_base_payu_pkg_rev(
        df_features_helper, config_feature_revenue
    )

    df_features = (
        df_features_helper.withColumn(
            "fea_days_since_last_rev_payu",
            f.when(
                f.col("days_since_last_rev_payu").isNull(),
                f.datediff(f.col("weekstart"), f.col("last_rev_payu")),
            ).otherwise(f.col("days_since_last_rev_payu")),
        )
        .withColumn(
            "fea_days_since_last_rev_pkg",
            f.when(
                f.col("days_since_last_rev_pkg").isNull(),
                f.datediff(f.col("weekstart"), f.col("last_rev_pkg")),
            ).otherwise(f.col("days_since_last_rev_pkg")),
        )
        .withColumn(
            "fea_days_since_last_rev_tot",
            f.when(
                f.col("days_since_last_rev_tot").isNull(),
                f.datediff(f.col("weekstart"), f.col("last_rev_tot")),
            ).otherwise(f.col("days_since_last_rev_tot")),
        )
    )

    def fea_ratio_days_since_last_pkg_by_last_rech(period_string, period):
        if period == 7:
            return f.col("fea_days_since_last_rev_pkg") / f.col("days_since_last_rech")
        else:
            return f.avg(
                f.col("fea_days_since_last_rev_pkg") / f.col("days_since_last_rech")
            ).over(get_rolling_window(period, oby="weekstart"))

    df_features.cache()

    df_features = join_all(
        [
            df_features,
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_ratio_days_since_last_pkg_by_last_rech"],
                fea_ratio_days_since_last_pkg_by_last_rech,
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_ratio_voice_payu_by_pkg_avg"],
                division_of_columns(
                    "fea_rev_voice_payu_sum_{period_string}",
                    "fea_rev_voice_pkg_sum_{period_string}",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_ratio_data_payu_by_pkg_avg"],
                division_of_columns(
                    "fea_rev_data_payu_sum_{period_string}",
                    "fea_rev_data_pkg_sum_{period_string}",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_ratio_sms_payu_by_pkg_avg"],
                division_of_columns(
                    "fea_rev_sms_payu_sum_{period_string}",
                    "fea_rev_sms_pkg_sum_{period_string}",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_ratio_roam_payu_by_pkg_avg"],
                division_of_columns(
                    "fea_rev_roam_payu_sum_{period_string}",
                    "fea_rev_roam_pkg_sum_{period_string}",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_ratio_roam_by_tot_avg"],
                division_of_columns(
                    "fea_rev_roam_tot_sum_{period_string}",
                    "fea_rev_tot_sum_{period_string}",
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["weekstart", "msisdn"],
        how="outer",
    )

    df_fea = df_features.withColumn(
        "rev_total",
        f.when(
            (df_features.rev_payu_tot > 0) | (df_features.rev_pkg_tot > 0), 1
        ).otherwise(0),
    )

    df_features = join_all(
        [
            df_features,
            calculate_weeks_since_last_activity(
                df_fea,
                "rev_total",
                lookback_weeks=11,
                primary_cols=["msisdn", "weekstart"],
                feature_col="fea_rev_weeks_since_last_rev_total",
            ),
            calculate_weeks_since_last_activity(
                df_fea,
                "rev_payu_tot",
                lookback_weeks=11,
                primary_cols=["msisdn", "weekstart"],
                feature_col="fea_rev_weeks_since_last_rev_payu_tot",
            ),
            calculate_weeks_since_last_activity(
                df_fea,
                "rev_pkg_tot",
                lookback_weeks=11,
                primary_cols=["msisdn", "weekstart"],
                feature_col="fea_rev_weeks_since_last_rev_pkg_tot",
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    output_features = [
        "fea_days_since_last_rev_payu",
        "fea_days_since_last_rev_pkg",
        "fea_days_since_last_rev_tot",
        "fea_rev_weeks_since_last_rev_total",
        "fea_rev_weeks_since_last_rev_payu_tot",
        "fea_rev_weeks_since_last_rev_pkg_tot",
        *get_config_based_features_column_names(
            config_feature_revenue["fea_ratio_days_since_last_pkg_by_last_rech"],
            config_feature_revenue["fea_ratio_voice_payu_by_pkg_avg"],
            config_feature_revenue["fea_ratio_data_payu_by_pkg_avg"],
            config_feature_revenue["fea_ratio_sms_payu_by_pkg_avg"],
            config_feature_revenue["fea_ratio_roam_payu_by_pkg_avg"],
            config_feature_revenue["fea_ratio_roam_by_tot_avg"],
        ),
    ]

    required_output_columns = get_required_output_columns(
        output_features=output_features,
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
    "rev_payu_tot",
    "rev_payu_voice",
    "rev_payu_sms",
    "rev_payu_data",
    "rev_payu_roam",
    "rev_pkg_tot",
    "rev_pkg_voice",
    "rev_pkg_sms",
    "rev_pkg_data",
    "rev_pkg_roam",
    "max_date_payu",
    "max_date_pkg",
    "max_date_tot",
    "days_since_last_rev_payu",
    "days_since_last_rev_pkg",
    "days_since_last_rev_tot",
    "rev_alt_total",
    "rev_alt_voice",
    "rev_alt_data",
    "rev_alt_sms",
    "rev_alt_digital",
    "rev_alt_roam",
    "rev_alt_data_pkg",
    "rev_alt_sms_pkg",
    "rev_alt_roam_pkg",
    "rev_alt_voice_pkg",
    "non_loan_revenue",
    "loan_revenue",
    "loan_repayment_transactions",
]
