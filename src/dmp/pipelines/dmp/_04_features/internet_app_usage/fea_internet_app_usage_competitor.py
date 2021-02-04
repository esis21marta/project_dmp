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

from utils import (
    get_config_based_features,
    get_config_based_features_column_names,
    get_end_date,
    get_month_id_bw_sd_ed,
    get_required_output_columns,
    get_start_date,
    join_all,
    sum_of_columns_over_month_mapped_dt_window,
)


def fea_internet_app_usage_competitor(
    df_internet_app_usage_competitor: pyspark.sql.DataFrame,
    sla_date_parameter,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:

    config_feature_internet_app_usage = config_feature["internet_apps_usage"]

    fea_df = join_all(
        [
            get_config_based_features(
                df=df_internet_app_usage_competitor,
                feature_config=config_feature_internet_app_usage[
                    "fea_int_app_usage_comp_websites_visits"
                ],
                column_expression=sum_of_columns_over_month_mapped_dt_window(
                    ["comp_websites_visits"]
                ),
                return_only_feature_columns=True,
                is_month_mapped_to_date=True,
                columns_to_keep_from_original_df=["msisdn", "mo_id"],
            ),
            get_config_based_features(
                df=df_internet_app_usage_competitor,
                feature_config=config_feature_internet_app_usage[
                    "fea_int_app_usage_comp_websites_consumption"
                ],
                column_expression=sum_of_columns_over_month_mapped_dt_window(
                    ["comp_websites_consumption"]
                ),
                return_only_feature_columns=True,
                is_month_mapped_to_date=True,
                columns_to_keep_from_original_df=["msisdn", "mo_id"],
            ),
            get_config_based_features(
                df=df_internet_app_usage_competitor,
                feature_config=config_feature_internet_app_usage[
                    "fea_int_app_usage_comp_websites_apps_visits"
                ],
                column_expression=sum_of_columns_over_month_mapped_dt_window(
                    ["comp_apps_websites_visits"]
                ),
                return_only_feature_columns=True,
                is_month_mapped_to_date=True,
                columns_to_keep_from_original_df=["msisdn", "mo_id"],
            ),
            get_config_based_features(
                df=df_internet_app_usage_competitor,
                feature_config=config_feature_internet_app_usage[
                    "fea_int_app_usage_comp_websites_apps_consumption"
                ],
                column_expression=sum_of_columns_over_month_mapped_dt_window(
                    ["comp_apps_websites_consumption"]
                ),
                return_only_feature_columns=True,
                is_month_mapped_to_date=True,
                columns_to_keep_from_original_df=["msisdn", "mo_id"],
            ),
        ],
        on=["msisdn", "mo_id"],
        how="outer",
    )

    output_features_columns = get_config_based_features_column_names(
        config_feature_internet_app_usage["fea_int_app_usage_comp_websites_visits"],
        config_feature_internet_app_usage[
            "fea_int_app_usage_comp_websites_consumption"
        ],
        config_feature_internet_app_usage[
            "fea_int_app_usage_comp_websites_apps_visits"
        ],
        config_feature_internet_app_usage[
            "fea_int_app_usage_comp_websites_apps_consumption"
        ],
    )

    required_output_columns = get_required_output_columns(
        output_features=output_features_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "mo_id"],
    )

    df = fea_df.select(required_output_columns)

    start_date = get_start_date()
    end_date = get_end_date()

    df_month = get_month_id_bw_sd_ed(start_date, end_date, sla_date_parameter)

    df_feature = df.join(df_month, on=["mo_id"]).drop("mo_id")

    return df_feature
