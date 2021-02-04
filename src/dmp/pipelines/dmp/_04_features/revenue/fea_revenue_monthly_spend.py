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
from src.dmp.pipelines.dmp._04_features.revenue.revenue_columns import (
    OUTPUT_COLUMNS_MONTHLY_SPEND,
)
from utils import (
    get_end_date,
    get_required_output_columns,
    get_rolling_window,
    get_rolling_window_fixed,
    get_start_date,
)


def fea_revenue_monthly_spend(
    df_revenue_weekly: pyspark.sql.DataFrame,
    df_revenue_alt_weekly: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Calculates internal monthly spend revenue for each msisdn:

    Args:
        df_revenue_weekly: Revenue weekly data
        df_revenue_alt_weekly: Revenue weekly data

    Returns:
        df_features: Dataframe with features, revenue
            - msisdn: Unique Id
            - weekstart: Weekly obervation point
            - fea_monthly_spend_*_sum_01m_by_02m: monthly spend SMS, Voice, Data, & Roam revenue 1m divided by 2m (5w - 8w)
            - fea_monthly_spend_*_min_03m: monthly MIN spend SMS, Voice, Data, & Roam revenue in 3m window
            - fea_monthly_spend_*_max_03m: monthly MAX spend SMS, Voice, Data, & Roam revenue in 3m window
            - fea_monthly_spend_*_pattern_*_by_01m_over_03m: monthly spend SMS, Voice, Data, & Roam 1w - 4w divided by 1m in 3m window
    """
    config_feature_revenue = config_feature["revenue"]

    # selecting specific agg columns to improve performance
    df_revenue_weekly = df_revenue_weekly.join(
        df_revenue_alt_weekly, ["msisdn", "weekstart"], how="outer"
    )
    df_revenue_weekly = df_revenue_weekly.select(SELECT_AGG_COLUMNS)

    df_features_helper = (
        df_revenue_weekly.withColumn(
            "rev_voice_tot_sum_5w_8w",
            f.sum(f.col("rev_payu_voice") + f.col("rev_pkg_voice")).over(
                get_rolling_window_fixed(56, 28, oby="weekstart")  # incorrect window
            ),
        )
        .withColumn(
            "rev_data_tot_sum_5w_8w",
            f.sum(f.col("rev_payu_data") + f.col("rev_pkg_data")).over(
                get_rolling_window_fixed(56, 28, oby="weekstart")  # incorrect window
            ),
        )
        .withColumn(
            "rev_sms_tot_sum_5w_8w",
            f.sum(f.col("rev_payu_sms") + f.col("rev_pkg_sms")).over(
                get_rolling_window_fixed(56, 28, oby="weekstart")  # incorrect window
            ),
        )
        .withColumn(
            "rev_roam_tot_sum_5w_8w",
            f.sum(f.col("rev_payu_roam") + f.col("rev_pkg_roam")).over(
                get_rolling_window_fixed(56, 28, oby="weekstart")  # incorrect window
            ),
        )
        .withColumn(
            "rev_voice_tot_max_01m",
            f.max(f.col("rev_payu_voice") + f.col("rev_pkg_voice")).over(
                get_rolling_window(28, oby="weekstart")
            ),
        )
        .withColumn(
            "rev_voice_tot_min_01m",
            f.min(f.col("rev_payu_voice") + f.col("rev_pkg_voice")).over(
                get_rolling_window(28, oby="weekstart")
            ),
        )
        .withColumn(
            "rev_data_tot_max_01m",
            f.max(f.col("rev_payu_data") + f.col("rev_pkg_data")).over(
                get_rolling_window(28, oby="weekstart")
            ),
        )
        .withColumn(
            "rev_data_tot_min_01m",
            f.min(f.col("rev_payu_data") + f.col("rev_pkg_data")).over(
                get_rolling_window(28, oby="weekstart")
            ),
        )
        .withColumn(
            "rev_sms_tot_max_01m",
            f.max(f.col("rev_payu_sms") + f.col("rev_pkg_sms")).over(
                get_rolling_window(28, oby="weekstart")
            ),
        )
        .withColumn(
            "rev_sms_tot_min_01m",
            f.min(f.col("rev_payu_sms") + f.col("rev_pkg_sms")).over(
                get_rolling_window(28, oby="weekstart")
            ),
        )
        .withColumn(
            "rev_roam_tot_max_01m",
            f.max(f.col("rev_payu_roam") + f.col("rev_pkg_roam")).over(
                get_rolling_window(28, oby="weekstart")
            ),
        )
        .withColumn(
            "rev_roam_tot_min_01m",
            f.min(f.col("rev_payu_roam") + f.col("rev_pkg_roam")).over(
                get_rolling_window(28, oby="weekstart")
            ),
        )
        .withColumn(
            "rev_alt_voice_tot_sum_5w_8w",
            f.sum(f.col("rev_alt_voice")).over(
                get_rolling_window_fixed(8 * 7, 5 * 7, oby="weekstart")
            ),
        )
        .withColumn(
            "rev_alt_data_tot_sum_5w_8w",
            f.sum(f.col("rev_alt_data")).over(
                get_rolling_window_fixed(8 * 7, 5 * 7, oby="weekstart")
            ),
        )
        .withColumn(
            "rev_alt_sms_tot_sum_5w_8w",
            f.sum(f.col("rev_alt_sms")).over(
                get_rolling_window_fixed(8 * 7, 5 * 7, oby="weekstart")
            ),
        )
        .withColumn(
            "rev_alt_roam_tot_sum_5w_8w",
            f.sum(f.col("rev_alt_roam")).over(
                get_rolling_window_fixed(8 * 7, 5 * 7, oby="weekstart")
            ),
        )
    )

    df_features_helper = get_base_payu_pkg_rev(
        df_features_helper, config_feature_revenue
    )

    df_features = (
        df_features_helper.withColumn(
            "fea_monthly_spend_voice_sum_01m_by_02m",
            (f.col("fea_rev_voice_tot_sum_01m") / f.col("rev_voice_tot_sum_5w_8w")),
        )
        .withColumn(
            "fea_monthly_spend_data_sum_01m_by_02m",
            (f.col("fea_rev_data_tot_sum_01m") / f.col("rev_data_tot_sum_5w_8w")),
        )
        .withColumn(
            "fea_monthly_spend_sms_sum_01m_by_02m",
            (f.col("fea_rev_sms_tot_sum_01m") / f.col("rev_sms_tot_sum_5w_8w")),
        )
        .withColumn(
            "fea_monthly_spend_roam_sum_01m_by_02m",
            (f.col("fea_rev_roam_tot_sum_01m") / f.col("rev_roam_tot_sum_5w_8w")),
        )
        .withColumn(
            "fea_monthly_spend_voice_min_03m",
            f.min(f.col("rev_voice_tot_min_01m")).over(
                get_rolling_window(63, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_monthly_spend_data_min_03m",
            f.min(f.col("rev_data_tot_min_01m")).over(
                get_rolling_window(63, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_monthly_spend_sms_min_03m",
            f.min(f.col("rev_sms_tot_min_01m")).over(
                get_rolling_window(63, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_monthly_spend_roam_min_03m",
            f.min(f.col("rev_roam_tot_min_01m")).over(
                get_rolling_window(63, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_monthly_spend_voice_max_03m",
            f.max(f.col("rev_voice_tot_max_01m")).over(
                get_rolling_window(63, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_monthly_spend_data_max_03m",
            f.max(f.col("rev_data_tot_max_01m")).over(
                get_rolling_window(63, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_monthly_spend_sms_max_03m",
            f.max(f.col("rev_sms_tot_max_01m")).over(
                get_rolling_window(63, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_monthly_spend_roam_max_03m",
            f.max(f.col("rev_roam_tot_max_01m")).over(
                get_rolling_window(63, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_monthly_spend_voice_pattern_01w_by_01m_over_03m",
            f.avg(
                f.col("fea_rev_voice_tot_sum_01w") / f.col("fea_rev_voice_tot_sum_01m")
            ).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn(
            "fea_monthly_spend_voice_pattern_02w_by_01m_over_03m",
            f.avg(
                (
                    f.col("fea_rev_voice_tot_sum_02w")
                    - f.col("fea_rev_voice_tot_sum_01w")
                )
                / f.col("fea_rev_voice_tot_sum_01m")
            ).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn(
            "fea_monthly_spend_voice_pattern_03w_by_01m_over_03m",
            f.avg(
                (
                    f.col("fea_rev_voice_tot_sum_03w")
                    - f.col("fea_rev_voice_tot_sum_02w")
                )
                / f.col("fea_rev_voice_tot_sum_01m")
            ).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn(
            "fea_monthly_spend_voice_pattern_04w_by_01m_over_03m",
            f.avg(
                (
                    f.col("fea_rev_voice_tot_sum_01m")
                    - f.col("fea_rev_voice_tot_sum_03w")
                )
                / f.col("fea_rev_voice_tot_sum_01m")
            ).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn(
            "fea_monthly_spend_data_pattern_01w_by_01m_over_03m",
            f.avg(
                f.col("fea_rev_data_tot_sum_01w") / f.col("fea_rev_data_tot_sum_01m")
            ).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn(
            "fea_monthly_spend_data_pattern_02w_by_01m_over_03m",
            f.avg(
                (f.col("fea_rev_data_tot_sum_02w") - f.col("fea_rev_data_tot_sum_01w"))
                / f.col("fea_rev_data_tot_sum_01m")
            ).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn(
            "fea_monthly_spend_data_pattern_03w_by_01m_over_03m",
            f.avg(
                (f.col("fea_rev_data_tot_sum_03w") - f.col("fea_rev_data_tot_sum_02w"))
                / f.col("fea_rev_data_tot_sum_01m")
            ).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn(
            "fea_monthly_spend_data_pattern_04w_by_01m_over_03m",
            f.avg(
                (f.col("fea_rev_data_tot_sum_01m") - f.col("fea_rev_data_tot_sum_03w"))
                / f.col("fea_rev_data_tot_sum_01m")
            ).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn(
            "fea_monthly_spend_sms_pattern_01w_by_01m_over_03m",
            f.avg(
                f.col("fea_rev_sms_tot_sum_01w") / f.col("fea_rev_sms_tot_sum_01m")
            ).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn(
            "fea_monthly_spend_sms_pattern_02w_by_01m_over_03m",
            f.avg(
                (f.col("fea_rev_sms_tot_sum_02w") - f.col("fea_rev_sms_tot_sum_01w"))
                / f.col("fea_rev_sms_tot_sum_01m")
            ).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn(
            "fea_monthly_spend_sms_pattern_03w_by_01m_over_03m",
            f.avg(
                (f.col("fea_rev_sms_tot_sum_03w") - f.col("fea_rev_sms_tot_sum_02w"))
                / f.col("fea_rev_sms_tot_sum_01m")
            ).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn(
            "fea_monthly_spend_sms_pattern_04w_by_01m_over_03m",
            f.avg(
                (f.col("fea_rev_sms_tot_sum_01m") - f.col("fea_rev_sms_tot_sum_03w"))
                / f.col("fea_rev_sms_tot_sum_01m")
            ).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn(
            "fea_monthly_spend_roam_pattern_01w_by_01m_over_03m",
            f.avg(
                f.col("fea_rev_roam_tot_sum_01w") / f.col("fea_rev_roam_tot_sum_01m")
            ).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn(
            "fea_monthly_spend_roam_pattern_02w_by_01m_over_03m",
            f.avg(
                (f.col("fea_rev_roam_tot_sum_02w") - f.col("fea_rev_roam_tot_sum_01w"))
                / f.col("fea_rev_roam_tot_sum_01m")
            ).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn(
            "fea_monthly_spend_roam_pattern_03w_by_01m_over_03m",
            f.avg(
                (f.col("fea_rev_roam_tot_sum_03w") - f.col("fea_rev_roam_tot_sum_02w"))
                / f.col("fea_rev_roam_tot_sum_01m")
            ).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn(
            "fea_monthly_spend_roam_pattern_04w_by_01m_over_03m",
            f.avg(
                (f.col("fea_rev_roam_tot_sum_01m") - f.col("fea_rev_roam_tot_sum_03w"))
                / f.col("fea_rev_roam_tot_sum_01m")
            ).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn(
            "fea_rev_alt_monthly_spend_voice_sum_01m_by_02m",
            (
                f.col("fea_rev_alt_voice_tot_sum_01m")
                / f.col("rev_alt_voice_tot_sum_5w_8w")
            ),
        )
        .withColumn(
            "fea_rev_alt_monthly_spend_data_sum_01m_by_02m",
            (
                f.col("fea_rev_alt_data_tot_sum_01m")
                / f.col("rev_alt_data_tot_sum_5w_8w")
            ),
        )
        .withColumn(
            "fea_rev_alt_monthly_spend_sms_sum_01m_by_02m",
            (f.col("fea_rev_alt_sms_tot_sum_01m") / f.col("rev_alt_sms_tot_sum_5w_8w")),
        )
        .withColumn(
            "fea_rev_alt_monthly_spend_roam_sum_01m_by_02m",
            (
                f.col("fea_rev_alt_roam_tot_sum_01m")
                / f.col("rev_alt_roam_tot_sum_5w_8w")
            ),
        )
    )

    required_output_columns = get_required_output_columns(
        output_features=OUTPUT_COLUMNS_MONTHLY_SPEND,
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

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")


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
    "rev_alt_voice",
    "rev_alt_data",
    "rev_alt_sms",
    "rev_alt_roam",
    "rev_alt_total",
    "rev_alt_digital",
    "rev_alt_voice_pkg",
    "rev_alt_sms_pkg",
    "rev_alt_data_pkg",
    "rev_alt_roam_pkg",
    "non_loan_revenue",
    "loan_revenue",
    "loan_repayment_transactions",
]
