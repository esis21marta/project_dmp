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
    get_end_date,
    get_required_output_columns,
    get_rolling_window,
    get_rolling_window_fixed,
    get_start_date,
    join_all,
    sum_of_columns_over_weekstart_window,
)


def fea_txt_msg_count(
    df_txt_msg_weekly: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    create features base on weekly aggregated dataframe

    Args:
        df_txt_msg_weekly: data frame for text message base on weekly aggregation dataframe

    Returns:
        dataframe base on weekly aggregated dataframe
    """

    config_feature_sms = config_feature["sms"]

    def fea_txt_msg_voice_ratio(period_string, period):
        return f.col(f"fea_txt_msg_count_{period_string}") / (
            f.col(f"fea_txt_msg_count_{period_string}")
            + f.sum("count_voice_all").over(get_rolling_window(period, oby="weekstart"))
        )

    def fea_txt_msg_voice_ratio_incoming(period_string, period):
        if period == 7:
            return f.col("count_txt_msg_incoming") / (
                f.col("count_txt_msg_incoming") + f.col("count_voice_incoming")
            )
        else:
            return f.col(f"fea_txt_msg_incoming_count_{period_string}") / (
                f.col(f"fea_txt_msg_incoming_count_{period_string}")
                + f.sum("count_voice_incoming").over(
                    get_rolling_window(period, oby="weekstart")
                )
            )

    def fea_txt_msg_voice_ratio_outgoing(period_string, period):
        if period == 7:
            return f.col("count_txt_msg_outgoing") / (
                f.col("count_txt_msg_outgoing") + f.col("count_voice_outgoing")
            )
        else:
            return f.col(f"fea_txt_msg_outgoing_count_{period_string}") / (
                f.col(f"fea_txt_msg_outgoing_count_{period_string}")
                + f.sum("count_voice_outgoing").over(
                    get_rolling_window(period, oby="weekstart")
                )
            )

    df_txt_msg_weekly.cache()

    df_txt_msg_weekly = df_txt_msg_weekly.withColumn(
        "not_any_out_msg", f.when(f.col("count_txt_msg_outgoing") > 0, 0).otherwise(1)
    ).withColumn(
        "not_any_inc_msg", f.when(f.col("count_txt_msg_incoming") > 0, 0).otherwise(1)
    )

    df_final = join_all(
        [
            df_txt_msg_weekly,
            get_config_based_features(
                df_txt_msg_weekly,
                config_feature_sms["fea_txt_msg_incoming_count"],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["count_txt_msg_incoming"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_txt_msg_weekly,
                config_feature_sms["fea_txt_msg_outgoing_count"],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["count_txt_msg_outgoing"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_txt_msg_weekly,
                config_feature_sms["fea_txt_msg_count"],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["count_txt_msg_all"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    df_final.cache()

    df_final = join_all(
        [
            df_final,
            get_config_based_features(
                df_final,
                config_feature_sms["fea_txt_msg_voice_ratio_incoming"],
                column_expression=fea_txt_msg_voice_ratio_incoming,
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_final,
                config_feature_sms["fea_txt_msg_voice_ratio_outgoing"],
                column_expression=fea_txt_msg_voice_ratio_outgoing,
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_final,
                config_feature_sms["fea_txt_msg_voice_ratio"],
                column_expression=fea_txt_msg_voice_ratio,
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    df_fea = df_txt_msg_weekly.withColumnRenamed(
        "count_txt_msg_outgoing", "outgoing_msg"
    ).withColumnRenamed("count_txt_msg_incoming", "incoming_msg")
    df_final = join_all(
        [
            df_final,
            calculate_weeks_since_last_activity(
                df_fea,
                "outgoing_msg",
                lookback_weeks=11,
                primary_cols=["msisdn", "weekstart"],
                feature_col="fea_txt_msg_weeks_since_last_outgoing_msg",
            ),
            calculate_weeks_since_last_activity(
                df_fea,
                "incoming_msg",
                lookback_weeks=11,
                primary_cols=["msisdn", "weekstart"],
                feature_col="fea_txt_msg_weeks_since_last_incoming_msg",
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    df_final = join_all(
        [
            df_final,
            df_txt_msg_weekly.withColumn(
                "fea_txt_msg_not_any_out_call_1w_4w",
                f.sum(f.col("not_any_out_msg")).over(
                    get_rolling_window_fixed(4 * 7, 1 * 7, oby="weekstart")
                ),
            ).select("msisdn", "weekstart", "fea_txt_msg_not_any_out_call_1w_4w"),
            df_txt_msg_weekly.withColumn(
                "fea_txt_msg_not_any_out_call_4w_8w",
                f.sum(f.col("not_any_out_msg")).over(
                    get_rolling_window_fixed(8 * 7, 4 * 7, oby="weekstart")
                ),
            ).select("msisdn", "weekstart", "fea_txt_msg_not_any_out_call_4w_8w"),
            df_txt_msg_weekly.withColumn(
                "fea_txt_msg_not_any_out_call_8w_12w",
                f.sum(f.col("not_any_out_msg")).over(
                    get_rolling_window_fixed(12 * 7, 8 * 7, oby="weekstart")
                ),
            ).select("msisdn", "weekstart", "fea_txt_msg_not_any_out_call_8w_12w"),
            df_txt_msg_weekly.withColumn(
                "fea_txt_msg_not_any_inc_call_1w_4w",
                f.sum(f.col("not_any_inc_msg")).over(
                    get_rolling_window_fixed(4 * 7, 1 * 7, oby="weekstart")
                ),
            ).select("msisdn", "weekstart", "fea_txt_msg_not_any_inc_call_1w_4w"),
            df_txt_msg_weekly.withColumn(
                "fea_txt_msg_not_any_inc_call_4w_8w",
                f.sum(f.col("not_any_inc_msg")).over(
                    get_rolling_window_fixed(8 * 7, 4 * 7, oby="weekstart")
                ),
            ).select("msisdn", "weekstart", "fea_txt_msg_not_any_inc_call_4w_8w"),
            df_txt_msg_weekly.withColumn(
                "fea_txt_msg_not_any_inc_call_8w_12w",
                f.sum(f.col("not_any_inc_msg")).over(
                    get_rolling_window_fixed(12 * 7, 8 * 7, oby="weekstart")
                ),
            ).select("msisdn", "weekstart", "fea_txt_msg_not_any_inc_call_8w_12w"),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    df_final = df_final.drop(
        "count_txt_msg_all",
        "count_txt_msg_incoming",
        "count_txt_msg_outgoing",
        "count_voice_all",
        "count_voice_incoming",
        "count_voice_outgoing",
        "not_any_inc_msg",
        "not_any_out_msg",
    )

    required_output_columns = get_required_output_columns(
        output_features=df_final.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    df_final = df_final.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return df_final.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )
