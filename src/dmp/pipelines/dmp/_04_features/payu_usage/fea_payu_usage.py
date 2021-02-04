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

import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from utils import (
    get_config_based_features,
    get_end_date,
    get_required_output_columns,
    get_start_date,
    join_all,
    sum_of_columns_over_weekstart_window,
)


def fea_payu_usage(
    df_payu: DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> DataFrame:
    """
    create feature for PayU from weekly data

    Args:
        df_payu: PAYU Usage dataframe.
    Returns:
        fea_df: PayU Usage Features
    """

    config_feature_payu_usage = config_feature["payu_usage"]

    out = join_all(
        [
            get_config_based_features(
                df=df_payu,
                feature_config=config_feature_payu_usage[
                    "fea_payu_usage_weekend_kb_usage"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["vol_data_kb_weekends"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_payu,
                feature_config=config_feature_payu_usage[
                    "fea_payu_usage_weekday_kb_usage"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["vol_data_kb_weekdays"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_payu,
                feature_config=config_feature_payu_usage["fea_payu_usage_kb_usage"],
                column_expression=sum_of_columns_over_weekstart_window(["vol_data_kb"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_payu,
                feature_config=config_feature_payu_usage[
                    "fea_payu_usage_day_time_kb_usage"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["vol_data_day_kb"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_payu,
                feature_config=config_feature_payu_usage[
                    "fea_payu_usage_night_time_kb_usage"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["vol_data_night_kb"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_payu,
                feature_config=config_feature_payu_usage[
                    "fea_payu_usage_weekend_day_time_kb_usage"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["vol_data_day_kb_weekends"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_payu,
                feature_config=config_feature_payu_usage[
                    "fea_payu_usage_weekend_night_time_kb_usage"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["vol_data_night_kb_weekends"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_payu,
                feature_config=config_feature_payu_usage[
                    "fea_payu_usage_weekday_day_time_kb_usage"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["vol_data_day_kb_weekdays"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_payu,
                feature_config=config_feature_payu_usage[
                    "fea_payu_usage_weekday_night_time_kb_usage"
                ],
                column_expression=sum_of_columns_over_weekstart_window(
                    ["vol_data_night_kb_weekdays"]
                ),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    required_output_columns = get_required_output_columns(
        output_features=out.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    out = out.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return out.filter(f.col("weekstart").between(first_week_start, last_week_start))
