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
    calculate_weeks_since_last_activity,
    get_config_based_features,
    get_config_based_features_column_names,
    get_end_date,
    get_required_output_columns,
    get_rolling_window,
    get_rolling_window_fixed,
    get_start_date,
    join_all,
    sum_of_columns,
    sum_of_columns_over_weekstart_window,
)


def fea_voice_calling(
    df_voice_calling: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Voice Calling Feature Data

    Args:
        df_voice_calling: Voice Calling Weekly Aggregated Data

    Returns:
        df_voice_features: Voice Calling Feature Data
    """

    config_feature_voice = config_feature["voice_calls"]

    def fea_voice_tot_calls_wrapper(call_type):
        def fea_voice_tot_calls(period_string, period):
            if period == 7:
                return f.when(
                    f.col(f"{call_type}_call_nums").isNotNull(),
                    f.size(f.col(f"{call_type}_call_nums")),
                ).otherwise(0)
            else:
                return f.size(
                    f.flatten(
                        f.collect_list(f"{call_type}_call_nums").over(
                            get_rolling_window(period, oby="weekstart")
                        )
                    )
                )

        return fea_voice_tot_calls

    def fea_voice_tot_uniq_calls_wrapper(call_type):
        def fea_voice_tot_uniq_calls(period_string, period):
            if period == 7:
                return f.size(f.array_distinct(f.col(f"{call_type}_call_nums")))
            else:
                return f.size(
                    f.array_distinct(
                        f.flatten(
                            f.collect_list(f"{call_type}_call_nums").over(
                                get_rolling_window(period, oby="weekstart")
                            )
                        )
                    )
                )

        return fea_voice_tot_uniq_calls

    df_voice_calling.cache()

    df_voice_calling = df_voice_calling.withColumn(
        "not_any_out_call", f.when(f.size(f.col("out_call_nums")) > 0, 0).otherwise(1)
    ).withColumn(
        "not_any_inc_call", f.when(f.size(f.col("in_call_nums")) > 0, 0).otherwise(1)
    )

    df_voice_features = join_all(
        [
            df_voice_calling,
            get_config_based_features(
                df_voice_calling,
                config_feature_voice["fea_voice_inc_call_dur"],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["inc_call_dur"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_voice_calling,
                config_feature_voice["fea_voice_out_call_dur"],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["out_call_dur"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    df_voice_features.cache()

    df_voice_features = join_all(
        [
            df_voice_features,
            get_config_based_features(
                df_voice_features,
                config_feature_voice["fea_voice_tot_call_dur"],
                column_expression=sum_of_columns(
                    [
                        "fea_voice_inc_call_dur_{period_string}",
                        "fea_voice_out_call_dur_{period_string}",
                    ]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_voice_features,
                config_feature_voice["fea_voice_tot_inc_calls"],
                fea_voice_tot_calls_wrapper("in"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_voice_features,
                config_feature_voice["fea_voice_tot_out_calls"],
                fea_voice_tot_calls_wrapper("out"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_voice_features,
                config_feature_voice["fea_voice_tot_calls"],
                fea_voice_tot_calls_wrapper("inc_out"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_voice_features,
                config_feature_voice["fea_voice_tot_uniq_inc_calls"],
                fea_voice_tot_uniq_calls_wrapper("in"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_voice_features,
                config_feature_voice["fea_voice_tot_uniq_out_calls"],
                fea_voice_tot_uniq_calls_wrapper("out"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_voice_features,
                config_feature_voice["fea_voice_tot_uniq_calls"],
                fea_voice_tot_uniq_calls_wrapper("inc_out"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    df_voice_features = (
        df_voice_features.withColumn(
            "fea_voice_inc_to_tot_calls_num_03m",
            f.coalesce(
                (
                    f.col("fea_voice_tot_inc_calls_03m")
                    / f.col("fea_voice_tot_calls_03m")
                ),
                f.lit(float("nan")),
            ),
        )
        .withColumn(
            "fea_voice_inc_to_tot_calls_dur_03m",
            f.coalesce(
                (
                    f.col("fea_voice_inc_call_dur_03m")
                    / f.col("fea_voice_tot_call_dur_03m")
                ),
                f.lit(float("nan")),
            ),
        )
        .withColumn(
            "fea_voice_avg_uniq_inc_call_03m",
            (f.col("fea_voice_tot_uniq_inc_calls_03m") / 3),
        )
        .withColumn(
            "fea_voice_avg_uniq_out_call_03m",
            (f.col("fea_voice_tot_uniq_out_calls_03m") / 3),
        )
        .withColumn(
            "fea_voice_avg_uniq_tot_call_03m",
            (f.col("fea_voice_tot_uniq_calls_03m") / 3),
        )
        .withColumn(
            "fea_voice_tot_dur_12_5_am_03m",
            f.sum(f.col("total_dur_12_5_am")).over(
                get_rolling_window(7 * 13, oby="weekstart")
            ),
        )
    )

    df_fea = df_voice_calling.withColumnRenamed(
        "out_call_dur", "outgoing_call"
    ).withColumnRenamed("inc_call_dur", "incoming_call")
    df_voice_features = join_all(
        [
            df_voice_features,
            calculate_weeks_since_last_activity(
                df_fea,
                "incoming_call",
                lookback_weeks=11,
                primary_cols=["msisdn", "weekstart"],
                feature_col="fea_voice_weeks_since_last_incoming_call",
            ),
            calculate_weeks_since_last_activity(
                df_fea,
                "outgoing_call",
                lookback_weeks=11,
                primary_cols=["msisdn", "weekstart"],
                feature_col="fea_voice_weeks_since_last_outgoing_call",
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    df_voice_features = join_all(
        [
            df_voice_features,
            df_voice_calling.withColumn(
                "fea_voice_not_any_out_call_1w_4w",
                f.sum(f.col("not_any_out_call")).over(
                    get_rolling_window_fixed(4 * 7, 1 * 7, oby="weekstart")
                ),
            ).select("msisdn", "weekstart", "fea_voice_not_any_out_call_1w_4w"),
            df_voice_calling.withColumn(
                "fea_voice_not_any_out_call_4w_8w",
                f.sum(f.col("not_any_out_call")).over(
                    get_rolling_window_fixed(8 * 7, 4 * 7, oby="weekstart")
                ),
            ).select("msisdn", "weekstart", "fea_voice_not_any_out_call_4w_8w"),
            df_voice_calling.withColumn(
                "fea_voice_not_any_out_call_8w_12w",
                f.sum(f.col("not_any_out_call")).over(
                    get_rolling_window_fixed(12 * 7, 8 * 7, oby="weekstart")
                ),
            ).select("msisdn", "weekstart", "fea_voice_not_any_out_call_8w_12w"),
            df_voice_calling.withColumn(
                "fea_voice_not_any_inc_call_1w_4w",
                f.sum(f.col("not_any_inc_call")).over(
                    get_rolling_window_fixed(4 * 7, 1 * 7, oby="weekstart")
                ),
            ).select("msisdn", "weekstart", "fea_voice_not_any_inc_call_1w_4w"),
            df_voice_calling.withColumn(
                "fea_voice_not_any_inc_call_4w_8w",
                f.sum(f.col("not_any_inc_call")).over(
                    get_rolling_window_fixed(8 * 7, 4 * 7, oby="weekstart")
                ),
            ).select("msisdn", "weekstart", "fea_voice_not_any_inc_call_4w_8w"),
            df_voice_calling.withColumn(
                "fea_voice_not_any_inc_call_8w_12w",
                f.sum(f.col("not_any_inc_call")).over(
                    get_rolling_window_fixed(12 * 7, 8 * 7, oby="weekstart")
                ),
            ).select("msisdn", "weekstart", "fea_voice_not_any_inc_call_8w_12w"),
        ],
        on=["msisdn", "weekstart"],
        how="left",
    )

    output_features = [
        "msisdn",
        "weekstart",
        *get_config_based_features_column_names(
            config_feature_voice["fea_voice_inc_call_dur"],
            config_feature_voice["fea_voice_out_call_dur"],
            config_feature_voice["fea_voice_tot_call_dur"],
            config_feature_voice["fea_voice_tot_inc_calls"],
            config_feature_voice["fea_voice_tot_out_calls"],
            config_feature_voice["fea_voice_tot_calls"],
            config_feature_voice["fea_voice_tot_uniq_inc_calls"],
            config_feature_voice["fea_voice_tot_uniq_out_calls"],
            config_feature_voice["fea_voice_tot_uniq_calls"],
        ),
        "fea_voice_inc_to_tot_calls_num_03m",
        "fea_voice_inc_to_tot_calls_dur_03m",
        "fea_voice_avg_uniq_inc_call_03m",
        "fea_voice_avg_uniq_out_call_03m",
        "fea_voice_avg_uniq_tot_call_03m",
        "fea_voice_tot_dur_12_5_am_03m",
        "fea_voice_weeks_since_last_incoming_call",
        "fea_voice_weeks_since_last_outgoing_call",
        "fea_voice_not_any_out_call_1w_4w",
        "fea_voice_not_any_out_call_4w_8w",
        "fea_voice_not_any_out_call_8w_12w",
        "fea_voice_not_any_inc_call_1w_4w",
        "fea_voice_not_any_inc_call_4w_8w",
        "fea_voice_not_any_inc_call_8w_12w",
    ]

    required_output_columns = get_required_output_columns(
        output_features=output_features,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    df_voice_features = df_voice_features.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return df_voice_features.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )
