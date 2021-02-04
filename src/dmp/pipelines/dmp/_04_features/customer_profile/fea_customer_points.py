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

from utils import (
    avg_over_weekstart_window,
    get_config_based_features,
    get_config_based_features_column_names,
    get_end_date,
    get_required_output_columns,
    get_start_date,
    join_all,
    max_over_weekstart_window,
    min_over_weekstart_window,
    sum_of_columns_over_weekstart_window,
)


def fea_customer_points(
    df_customer_profile_scaffold_weekly: pyspark.sql.DataFrame,
    df_redeemed_points_scaffold_weekly: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:

    config_feature_customer_profile = config_feature["customer_profile"]

    df_features = join_all(
        [
            df_customer_profile_scaffold_weekly,
            get_config_based_features(
                df=df_customer_profile_scaffold_weekly,
                feature_config=config_feature_customer_profile[
                    "fea_custprof_tsel_poin_avg"
                ],
                column_expression=avg_over_weekstart_window("custprof_tsel_poin_avg"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_customer_profile_scaffold_weekly,
                feature_config=config_feature_customer_profile[
                    "fea_custprof_tsel_poin_min"
                ],
                column_expression=min_over_weekstart_window("custprof_tsel_poin_min"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_customer_profile_scaffold_weekly,
                feature_config=config_feature_customer_profile[
                    "fea_custprof_tsel_poin_max"
                ],
                column_expression=max_over_weekstart_window("custprof_tsel_poin_max"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=df_redeemed_points_scaffold_weekly,
                feature_config=config_feature_customer_profile[
                    "fea_custprof_redeem_poin"
                ],
                column_expression=sum_of_columns_over_weekstart_window(["redeem_poin"]),
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    output_features_columns = get_config_based_features_column_names(
        config_feature_customer_profile["fea_custprof_tsel_poin_avg"],
        config_feature_customer_profile["fea_custprof_tsel_poin_min"],
        config_feature_customer_profile["fea_custprof_tsel_poin_max"],
        config_feature_customer_profile["fea_custprof_redeem_poin"],
    )

    required_output_columns = get_required_output_columns(
        output_features=output_features_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    df_features = df_features.select(required_output_columns)

    return df_features.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )
