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

from utils import (
    get_config_based_features,
    get_config_based_features_column_names,
    get_end_date,
    get_required_output_columns,
    get_start_date,
    join_all,
    sum_of_columns_over_weekstart_window,
)


def fea_revenue_threshold_voice_data(
    df_revenue_weekly: pyspark.sql.DataFrame,
    df_revenue_alt_weekly: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Calculates internal revenue with threshold for each msisdn:

    Args:
        df_revenue_weekly: Revenue weekly data
        df_revenue_alt_weekly: Revenue Alt weekly data

    Returns:
        df_features: Dataframe with features, revenue
            - msisdn: Unique Id
            - weekstart: Weekly obervation point
            - fea_days_with_rev_*_payu_gt_*_sum_*d: revenue SMS, Voice greater than threshold in the window
    """
    config_feature_revenue = config_feature["revenue"]

    # selecting specific agg columns to improve performance
    df_revenue_weekly = df_revenue_weekly.join(
        df_revenue_alt_weekly, ["msisdn", "weekstart"], how="outer"
    )
    df_revenue_weekly = df_revenue_weekly.select(SELECT_AGG_COLUMNS)

    df_revenue_weekly.cache()

    df_features = join_all(
        [
            df_revenue_weekly,
            get_config_based_features(
                df_revenue_weekly,
                config_feature_revenue[
                    "fea_days_with_rev_voice_payu_above_99_below_1000_sum"
                ],
                sum_of_columns_over_weekstart_window(
                    ["days_with_rev_payu_voice_above_99_below_1000"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue_weekly,
                config_feature_revenue[
                    "fea_days_with_rev_voice_payu_above_999_below_5000_sum"
                ],
                sum_of_columns_over_weekstart_window(
                    ["days_with_rev_payu_voice_above_999_below_5000"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue_weekly,
                config_feature_revenue[
                    "fea_days_with_rev_voice_payu_above_4999_below_10000_sum"
                ],
                sum_of_columns_over_weekstart_window(
                    ["days_with_rev_payu_voice_above_4999_below_10000"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue_weekly,
                config_feature_revenue[
                    "fea_days_with_rev_voice_payu_above_9999_below_50000_sum"
                ],
                sum_of_columns_over_weekstart_window(
                    ["days_with_rev_payu_voice_above_9999_below_50000"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue_weekly,
                config_feature_revenue["fea_days_with_rev_voice_payu_above_49999_sum"],
                sum_of_columns_over_weekstart_window(
                    ["days_with_rev_payu_voice_above_49999"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue_weekly,
                config_feature_revenue[
                    "fea_days_with_rev_data_payu_above_99_below_1000_sum"
                ],
                sum_of_columns_over_weekstart_window(
                    ["days_with_rev_payu_data_above_99_below_1000"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue_weekly,
                config_feature_revenue[
                    "fea_days_with_rev_data_payu_above_999_below_5000_sum"
                ],
                sum_of_columns_over_weekstart_window(
                    ["days_with_rev_payu_data_above_999_below_5000"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue_weekly,
                config_feature_revenue[
                    "fea_days_with_rev_data_payu_above_4999_below_10000_sum"
                ],
                sum_of_columns_over_weekstart_window(
                    ["days_with_rev_payu_data_above_4999_below_10000"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue_weekly,
                config_feature_revenue[
                    "fea_days_with_rev_data_payu_above_9999_below_50000_sum"
                ],
                sum_of_columns_over_weekstart_window(
                    ["days_with_rev_payu_data_above_9999_below_50000"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue_weekly,
                config_feature_revenue["fea_days_with_rev_data_payu_above_49999_sum"],
                sum_of_columns_over_weekstart_window(
                    ["days_with_rev_payu_data_above_49999"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue_weekly,
                config_feature_revenue[
                    "fea_rev_alt_days_with_data_above_999_below_5000_sum"
                ],
                sum_of_columns_over_weekstart_window(
                    ["rev_alt_days_with_data_above_999_below_5000"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue_weekly,
                config_feature_revenue[
                    "fea_rev_alt_days_with_data_above_4999_below_10000_sum"
                ],
                sum_of_columns_over_weekstart_window(
                    ["rev_alt_days_with_data_above_4999_below_10000"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue_weekly,
                config_feature_revenue[
                    "fea_rev_alt_days_with_data_above_9999_below_50000_sum"
                ],
                sum_of_columns_over_weekstart_window(
                    ["rev_alt_days_with_data_above_9999_below_50000"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue_weekly,
                config_feature_revenue["fea_rev_alt_days_with_data_above_49999_sum"],
                sum_of_columns_over_weekstart_window(
                    ["rev_alt_days_with_data_above_49999"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue_weekly,
                config_feature_revenue[
                    "fea_rev_alt_days_with_voice_above_999_below_5000_sum"
                ],
                sum_of_columns_over_weekstart_window(
                    ["rev_alt_days_with_voice_above_999_below_5000"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue_weekly,
                config_feature_revenue[
                    "fea_rev_alt_days_with_voice_above_4999_below_10000_sum"
                ],
                sum_of_columns_over_weekstart_window(
                    ["rev_alt_days_with_voice_above_4999_below_10000"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue_weekly,
                config_feature_revenue[
                    "fea_rev_alt_days_with_voice_above_9999_below_50000_sum"
                ],
                sum_of_columns_over_weekstart_window(
                    ["rev_alt_days_with_voice_above_9999_below_50000"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue_weekly,
                config_feature_revenue["fea_rev_alt_days_with_voice_above_49999_sum"],
                sum_of_columns_over_weekstart_window(
                    ["rev_alt_days_with_voice_above_49999"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    output_features = get_config_based_features_column_names(
        config_feature_revenue["fea_days_with_rev_voice_payu_above_99_below_1000_sum"],
        config_feature_revenue["fea_days_with_rev_voice_payu_above_999_below_5000_sum"],
        config_feature_revenue[
            "fea_days_with_rev_voice_payu_above_4999_below_10000_sum"
        ],
        config_feature_revenue[
            "fea_days_with_rev_voice_payu_above_9999_below_50000_sum"
        ],
        config_feature_revenue["fea_days_with_rev_voice_payu_above_49999_sum"],
        config_feature_revenue["fea_days_with_rev_data_payu_above_99_below_1000_sum"],
        config_feature_revenue["fea_days_with_rev_data_payu_above_999_below_5000_sum"],
        config_feature_revenue[
            "fea_days_with_rev_data_payu_above_4999_below_10000_sum"
        ],
        config_feature_revenue[
            "fea_days_with_rev_data_payu_above_9999_below_50000_sum"
        ],
        config_feature_revenue["fea_days_with_rev_data_payu_above_49999_sum"],
        config_feature_revenue["fea_rev_alt_days_with_voice_above_999_below_5000_sum"],
        config_feature_revenue[
            "fea_rev_alt_days_with_voice_above_4999_below_10000_sum"
        ],
        config_feature_revenue[
            "fea_rev_alt_days_with_voice_above_9999_below_50000_sum"
        ],
        config_feature_revenue["fea_rev_alt_days_with_voice_above_49999_sum"],
        config_feature_revenue["fea_rev_alt_days_with_data_above_999_below_5000_sum"],
        config_feature_revenue["fea_rev_alt_days_with_data_above_4999_below_10000_sum"],
        config_feature_revenue["fea_rev_alt_days_with_data_above_9999_below_50000_sum"],
        config_feature_revenue["fea_rev_alt_days_with_data_above_49999_sum"],
    )

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
    "days_with_rev_payu_voice_above_99_below_1000",
    "days_with_rev_payu_voice_above_999_below_5000",
    "days_with_rev_payu_voice_above_4999_below_10000",
    "days_with_rev_payu_voice_above_9999_below_50000",
    "days_with_rev_payu_voice_above_49999",
    "days_with_rev_payu_data_above_99_below_1000",
    "days_with_rev_payu_data_above_999_below_5000",
    "days_with_rev_payu_data_above_4999_below_10000",
    "days_with_rev_payu_data_above_9999_below_50000",
    "days_with_rev_payu_data_above_49999",
    "rev_alt_days_with_voice_above_999_below_5000",
    "rev_alt_days_with_voice_above_4999_below_10000",
    "rev_alt_days_with_voice_above_9999_below_50000",
    "rev_alt_days_with_voice_above_49999",
    "rev_alt_days_with_data_above_999_below_5000",
    "rev_alt_days_with_data_above_4999_below_10000",
    "rev_alt_days_with_data_above_9999_below_50000",
    "rev_alt_days_with_data_above_49999",
]
