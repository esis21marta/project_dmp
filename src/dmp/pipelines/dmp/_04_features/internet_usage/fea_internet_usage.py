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

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from utils import (
    calculate_weeks_since_last_activity,
    first_value_over_weekstart_window,
    get_config_based_features,
    get_config_based_features_column_names,
    get_end_date,
    get_required_output_columns,
    get_rolling_window,
    get_start_date,
    join_all,
    max_over_weekstart_window,
    median_over_weekstart_window,
    min_over_weekstart_window,
    sum_of_columns_over_weekstart_window,
)


def compute_rolling_window_calculations(
    filled_df: DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> DataFrame:
    """
    Returns a dataframe with rolling window features for data usage
    Args:
        filled_df: Input dataframe.

    Returns:
        Dataframe with additional rolling window features.
    """

    primary_layer_columns = filled_df.columns

    config_feature_internet_usage = config_feature["internet_usage"]

    def fea_int_usage_stddev_4g_data_usage(period_string, period):
        if period == 7:
            return f.col("stddev_4g_data_usage_day")
        else:
            return f.stddev(f.col("fea_int_usage_daily_4g_avg_kb_data_usage_01w")).over(
                get_rolling_window(period, oby="weekstart")
            )

    def fea_int_usage_stddev_total_data_usage(period_string, period):
        if period == 7:
            return f.col("stddev_total_data_usage_day")
        else:
            return f.stddev(f.col("fea_int_usage_daily_avg_kb_data_usage_01w")).over(
                get_rolling_window(period, oby="weekstart")
            )

    def fea_int_usage_daily_4g_avg_kb_data_usage(period_string, period):
        return f.col(f"fea_int_usage_4g_kb_data_usage_{period_string}") / period

    def fea_int_usage_daily_avg_kb_data_usage(period_string, period):
        return f.col(f"fea_int_usage_tot_kb_data_usage_{period_string}") / period

    def fea_int_usage_zero_usage_count_to_usage_count_ratio(period_string, period):
        return f.coalesce(
            (
                f.col(f"fea_int_usage_zero_usage_days_{period_string}")
                / f.col(f"fea_int_usage_non_zero_usage_days_{period_string}")
            ),
            f.lit(-1),
        )

    filled_df.cache()

    out_df = join_all(
        [
            filled_df,
            get_config_based_features(
                df=filled_df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_tot_kb_data_usage"
                ],
                column_expression=sum_of_columns_over_weekstart_window(["tot_kb"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=filled_df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_2g_kb_data_usage"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["vol_data_2g_kb"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=filled_df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_3g_kb_data_usage"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["vol_data_3g_kb"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=filled_df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_4g_kb_data_usage"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["vol_data_4g_kb"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=filled_df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_min_total_data_usage"
                ],
                column_expression=min_over_weekstart_window("min_vol_data_tot_kb_day"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=filled_df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_med_total_data_usage"
                ],
                column_expression=median_over_weekstart_window(
                    "med_vol_data_tot_kb_day"
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=filled_df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_max_total_data_usage"
                ],
                column_expression=max_over_weekstart_window("max_vol_data_tot_kb_day"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=filled_df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_min_4g_data_usage"
                ],
                column_expression=min_over_weekstart_window("min_vol_data_4g_kb_day"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=filled_df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_med_4g_data_usage"
                ],
                column_expression=median_over_weekstart_window(
                    "med_vol_data_4g_kb_day"
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=filled_df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_max_4g_data_usage"
                ],
                column_expression=max_over_weekstart_window("max_vol_data_4g_kb_day"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=filled_df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_zero_usage_days"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["count_zero_usage_days"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=filled_df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_non_zero_usage_days"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["count_non_zero_usage_days"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    df_fea = filled_df.withColumnRenamed("tot_kb", "internet_usage")
    out_df = join_all(
        [
            out_df,
            calculate_weeks_since_last_activity(
                df_fea,
                "internet_usage",
                lookback_weeks=11,
                primary_cols=["msisdn", "weekstart"],
                feature_col="fea_int_usage_weeks_since_last_internet_usage",
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    out_df = get_config_based_features(
        df=out_df,
        feature_config=config_feature_internet_usage[
            "fea_int_usage_daily_4g_avg_kb_data_usage"  # Dependency
        ],
        column_expression=fea_int_usage_daily_4g_avg_kb_data_usage,
    )

    out_df = get_config_based_features(
        df=out_df,
        feature_config=config_feature_internet_usage[
            "fea_int_usage_daily_avg_kb_data_usage"
        ],
        column_expression=fea_int_usage_daily_avg_kb_data_usage,
    )
    out_df = get_config_based_features(
        df=out_df,
        feature_config=config_feature_internet_usage[
            "fea_int_usage_zero_usage_count_to_usage_count_ratio"
        ],
        column_expression=fea_int_usage_zero_usage_count_to_usage_count_ratio,
    )

    out_df = get_config_based_features(
        df=out_df,
        feature_config=config_feature_internet_usage[
            "fea_int_usage_stddev_4g_data_usage"
        ],
        column_expression=fea_int_usage_stddev_4g_data_usage,
    )
    out_df = get_config_based_features(
        df=out_df,
        feature_config=config_feature_internet_usage[
            "fea_int_usage_stddev_total_data_usage"
        ],
        column_expression=fea_int_usage_stddev_total_data_usage,
    )

    # Filtering of features and weekstarts
    required_output_columns = get_required_output_columns(
        output_features=out_df.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart", *primary_layer_columns],
    )

    out_df = out_df.select(required_output_columns)

    return out_df


def compute_weekday_weekend_usage(
    df: DataFrame, config_feature_internet_usage: dict
) -> DataFrame:
    """
    Compute a dataframe with additional features for weekend and weekday usage.

    Args:
        df: input dataframe with using column countain weekend, and weekday.

    Returns:
        Returns a dataframe with additional features for weekend and weekday usage.
    """
    df.cache()

    return join_all(
        [
            df,
            get_config_based_features(
                df=df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_weekday_kb_usage"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["tot_kb_weekday"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_weekend_kb_usage"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["tot_kb_weekend"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_tot_kb_data_usage_4g_weekend"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["vol_data_4g_kb_weekend"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_tot_kb_data_usage_3g_weekend"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["vol_data_3g_kb_weekend"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_tot_kb_data_usage_2g_weekend"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["vol_data_2g_kb_weekend"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_tot_kb_data_usage_4g_weekday"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["vol_data_4g_kb_weekday"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_tot_kb_data_usage_3g_weekday"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["vol_data_3g_kb_weekday"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_tot_kb_data_usage_2g_weekday"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["vol_data_2g_kb_weekday"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["weekstart", "msisdn"],
        how="outer",
    )


def compute_days_with_data_usage_above_threshold(
    df: DataFrame, config_feature_internet_usage: dict
) -> DataFrame:
    """
    Returns a dataframe with additional features for data usage above threshold

    Args:
        df: input dataframe with using column *_above_*

    Returns:
        dataframe with additional rolling aggregation columns
    """
    df.cache()

    return join_all(
        [
            df,
            get_config_based_features(
                df=df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_num_days_with_total_data_usage_above_2mb"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["days_with_payload_total_above_2mb"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_num_days_with_total_data_usage_above_50mb"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["days_with_payload_total_above_50mb"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_num_days_with_total_data_usage_above_500mb"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["days_with_payload_total_above_500mb"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_num_days_with_4g_data_usage_above_2mb"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["days_with_payload_4g_above_2mb"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_num_days_with_4g_data_usage_above_50mb"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["days_with_payload_4g_above_50mb"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_num_days_with_4g_data_usage_above_500mb"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["days_with_payload_4g_above_500mb"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["weekstart", "msisdn"],
        how="outer",
    )


def compute_uniq_payload_area_data_usage(
    df: DataFrame, config_feature_internet_usage: dict
) -> DataFrame:
    """
    Returns a dataframe with additional features for data usage area

    Args:
        df: input dataframe with using column sub_district_list_data_usage

    Returns:
        dataframe with additional rolling aggregation columns
    """

    def fea_int_usage_num_unique_payload_sub_district(period_string, period):
        if period == 7:
            return f.size(f.array_distinct("sub_district_list_data_usage"))
        else:
            return f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_list("sub_district_list_data_usage").over(
                            get_rolling_window(period, oby="weekstart")
                        )
                    )
                )
            )

    return get_config_based_features(
        df=df,
        feature_config=config_feature_internet_usage[
            "fea_int_usage_num_unique_payload_sub_district"
        ],
        column_expression=fea_int_usage_num_unique_payload_sub_district,
    )


def compute_day_night_features(
    df: DataFrame, config_feature_internet_usage: dict
) -> DataFrame:
    """
    Returns a dataframe with day and night features.

    Args:
        df: Dataframe.

    Returns:
        Dataframe with day and night features.
    """
    df.cache()

    return join_all(
        [
            df,
            get_config_based_features(
                df=df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_tot_kb_day"
                ],
                column_expression=sum_of_columns_over_weekstart_window(["tot_kb_day"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_tot_trx_day"
                ],
                column_expression=sum_of_columns_over_weekstart_window(["tot_trx_day"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_count_day_sessions"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["count_day_sessions"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_tot_kb_night"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["tot_kb_night"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_tot_trx_night"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["tot_trx_night"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df,
                feature_config=config_feature_internet_usage[
                    "fea_int_usage_count_night_sessions"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["count_night_sessions"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )


def feat_internet_data_usage_full(
    df: DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> DataFrame:
    """
    Returns dataframe with all internet usage features.
    Args:
        df: Dataframe which have prepared computation features.

    Returns:
        Dataframe with all internet usage features.
    """

    config_feature_internet_usage = config_feature["internet_usage"]

    df_weekend = compute_weekday_weekend_usage(df, config_feature_internet_usage)
    df_threshold = compute_days_with_data_usage_above_threshold(
        df_weekend, config_feature_internet_usage
    )
    df_area = compute_uniq_payload_area_data_usage(
        df_threshold, config_feature_internet_usage
    )
    df_full = compute_day_night_features(df_area, config_feature_internet_usage)

    OUTPUT_COLUMNS = [
        *get_config_based_features_column_names(
            config_feature_internet_usage["fea_int_usage_daily_4g_avg_kb_data_usage"],
            config_feature_internet_usage["fea_int_usage_3g_kb_data_usage"],
            config_feature_internet_usage["fea_int_usage_2g_kb_data_usage"],
            config_feature_internet_usage["fea_int_usage_daily_avg_kb_data_usage"],
            config_feature_internet_usage["fea_int_usage_tot_kb_data_usage"],
            config_feature_internet_usage[
                "fea_int_usage_zero_usage_count_to_usage_count_ratio"
            ],
            config_feature_internet_usage["fea_int_usage_non_zero_usage_days"],
            config_feature_internet_usage["fea_int_usage_zero_usage_days"],
            config_feature_internet_usage["fea_int_usage_stddev_total_data_usage"],
            config_feature_internet_usage["fea_int_usage_4g_kb_data_usage"],
            config_feature_internet_usage["fea_int_usage_stddev_4g_data_usage"],
            config_feature_internet_usage["fea_int_usage_min_total_data_usage"],
            config_feature_internet_usage["fea_int_usage_med_total_data_usage"],
            config_feature_internet_usage["fea_int_usage_max_total_data_usage"],
            config_feature_internet_usage["fea_int_usage_min_4g_data_usage"],
            config_feature_internet_usage["fea_int_usage_med_4g_data_usage"],
            config_feature_internet_usage["fea_int_usage_stddev_total_data_usage"],
            config_feature_internet_usage["fea_int_usage_max_4g_data_usage"],
            config_feature_internet_usage["fea_int_usage_weekday_kb_usage"],
            config_feature_internet_usage["fea_int_usage_weekend_kb_usage"],
            config_feature_internet_usage["fea_int_usage_tot_kb_data_usage_4g_weekend"],
            config_feature_internet_usage["fea_int_usage_tot_kb_data_usage_3g_weekend"],
            config_feature_internet_usage["fea_int_usage_tot_kb_data_usage_2g_weekend"],
            config_feature_internet_usage["fea_int_usage_tot_kb_data_usage_4g_weekday"],
            config_feature_internet_usage["fea_int_usage_tot_kb_data_usage_3g_weekday"],
            config_feature_internet_usage["fea_int_usage_tot_kb_data_usage_2g_weekday"],
            config_feature_internet_usage[
                "fea_int_usage_num_days_with_total_data_usage_above_2mb"
            ],
            config_feature_internet_usage[
                "fea_int_usage_num_days_with_total_data_usage_above_50mb"
            ],
            config_feature_internet_usage[
                "fea_int_usage_num_days_with_total_data_usage_above_500mb"
            ],
            config_feature_internet_usage[
                "fea_int_usage_num_days_with_4g_data_usage_above_2mb"
            ],
            config_feature_internet_usage[
                "fea_int_usage_num_days_with_4g_data_usage_above_50mb"
            ],
            config_feature_internet_usage[
                "fea_int_usage_num_days_with_4g_data_usage_above_500mb"
            ],
            config_feature_internet_usage[
                "fea_int_usage_num_unique_payload_sub_district"
            ],
            config_feature_internet_usage["fea_int_usage_tot_kb_day"],
            config_feature_internet_usage["fea_int_usage_tot_trx_day"],
            config_feature_internet_usage["fea_int_usage_count_day_sessions"],
            config_feature_internet_usage["fea_int_usage_tot_kb_night"],
            config_feature_internet_usage["fea_int_usage_tot_trx_night"],
            config_feature_internet_usage["fea_int_usage_count_night_sessions"],
        ),
        "fea_int_usage_weeks_since_last_internet_usage",
    ]

    required_output_columns = get_required_output_columns(
        output_features=OUTPUT_COLUMNS,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    df_full = df_full.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return df_full.filter(f.col("weekstart").between(first_week_start, last_week_start))
