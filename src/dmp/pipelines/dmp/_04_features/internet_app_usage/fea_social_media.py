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
)


def fea_social_media(
    df_fea_int_app_usage: pyspark.sql.DataFrame,
    df_fea_int_con: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Calculates the Social Media Features for each msisdn:

    Args:
        df_fea_int_app_usage: Internet App Usage Features
        df_fea_int_con: Internet Connection Features

    Returns:
        fea_df: DataFrame with features, app internet usage
    """

    fea_df = df_fea_int_app_usage.join(
        df_fea_int_con, ["msisdn", "weekstart"], how="full"
    )

    config_feature_internet_app_usage = config_feature["internet_apps_usage"]

    def fea_int_app_usage_data_ratio_wrapper(app_name):
        def fea_int_app_usage_data_ratio(period_string, period):
            return f.col(
                f"fea_int_app_usage_{app_name}_data_vol_{period_string}"
            ) / f.col(f"fea_int_app_usage_total_download_{period_string}") + f.col(
                f"fea_int_app_usage_total_upload_{period_string}"
            )

        return fea_int_app_usage_data_ratio

    fea_df = join_all(
        [
            fea_df,
            get_config_based_features(
                df=fea_df,
                feature_config=config_feature_internet_app_usage[
                    "fea_int_app_usage_facebook_data_ratio"
                ],
                column_expression=fea_int_app_usage_data_ratio_wrapper("facebook"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=fea_df,
                feature_config=config_feature_internet_app_usage[
                    "fea_int_app_usage_instagram_data_ratio"
                ],
                column_expression=fea_int_app_usage_data_ratio_wrapper("instagram"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=fea_df,
                feature_config=config_feature_internet_app_usage[
                    "fea_int_app_usage_linkedin_data_ratio"
                ],
                column_expression=fea_int_app_usage_data_ratio_wrapper("linkedin"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=fea_df,
                feature_config=config_feature_internet_app_usage[
                    "fea_int_app_usage_youtube_data_ratio"
                ],
                column_expression=fea_int_app_usage_data_ratio_wrapper("youtube"),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    output_features_columns = get_config_based_features_column_names(
        config_feature_internet_app_usage["fea_int_app_usage_facebook_data_ratio"],
        config_feature_internet_app_usage["fea_int_app_usage_instagram_data_ratio"],
        config_feature_internet_app_usage["fea_int_app_usage_linkedin_data_ratio"],
        config_feature_internet_app_usage["fea_int_app_usage_youtube_data_ratio"],
    )

    required_output_columns = get_required_output_columns(
        output_features=output_features_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    fea_df = fea_df.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return fea_df.filter(f.col("weekstart").between(first_week_start, last_week_start))
