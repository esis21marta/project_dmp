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
    division_of_sum_of_columns_over_weekstart_window,
    get_config_based_features,
    get_config_based_features_column_names,
    get_end_date,
    get_required_output_columns,
    get_start_date,
    sum_of_columns_over_weekstart_window,
)


def fea_internet_connection(
    df_internet_usage_weekly: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Calculates the internet connection usage for each msisdn:

    Args:
        df_internet_usage_weekly: Internet App Weekly Aggregated data (cadence: msisdn, weekstart, feature)

    Returns:
        fea_df: DataFrame with features, app internet usage
            - msisdn: Unique Id
            - weekstart: Weekly observation point
            - fea_int_app_usage_*_data_vol_*d: Data used in the window
            - fea_int_app_usage_*_accessed_apps_*d : App count used in the window
            - fea_int_app_usage_*_days_since_first_usage: Days since first usage
            - fea_int_app_usage_*_days_since_lsst_usage: Days since last usage
    """

    config_feature_internet_app_usage = config_feature["internet_apps_usage"]

    df_internet_usage_weekly = (
        df_internet_usage_weekly.withColumn(
            "4g_total_volume_in",
            f.col("4g_volume_in_weekday") + f.col("4g_volume_in_weekend"),
        )
        .withColumn(
            "4g_total_volume_out",
            f.col("4g_volume_out_weekday") + f.col("4g_volume_out_weekend"),
        )
        .withColumn(
            "4g_total_volume_weekday",
            f.col("4g_volume_in_weekday") + f.col("4g_volume_out_weekday"),
        )
        .withColumn(
            "4g_total_volume_weekend",
            f.col("4g_volume_in_weekend") + f.col("4g_volume_out_weekend"),
        )
        .withColumn(
            "4g_total_volume",
            f.col("4g_total_volume_in") + f.col("4g_total_volume_out"),
        )
        .withColumn(
            "3g_total_volume_in",
            f.col("3g_volume_in_weekday") + f.col("3g_volume_in_weekend"),
        )
        .withColumn(
            "3g_total_volume_out",
            f.col("3g_volume_out_weekday") + f.col("3g_volume_out_weekend"),
        )
        .withColumn(
            "3g_total_volume_weekday",
            f.col("3g_volume_in_weekday") + f.col("3g_volume_out_weekday"),
        )
        .withColumn(
            "3g_total_volume_weekend",
            f.col("3g_volume_in_weekend") + f.col("3g_volume_out_weekend"),
        )
        .withColumn(
            "3g_total_volume",
            f.col("3g_total_volume_in") + f.col("3g_total_volume_out"),
        )
        .withColumn(
            "2g_total_volume_in",
            f.col("2g_volume_in_weekday") + f.col("2g_volume_in_weekend"),
        )
        .withColumn(
            "2g_total_volume_out",
            f.col("2g_volume_out_weekday") + f.col("2g_volume_out_weekend"),
        )
        .withColumn(
            "2g_total_volume_weekday",
            f.col("2g_volume_in_weekday") + f.col("2g_volume_out_weekday"),
        )
        .withColumn(
            "2g_total_volume_weekend",
            f.col("2g_volume_in_weekend") + f.col("2g_volume_out_weekend"),
        )
        .withColumn(
            "2g_total_volume",
            f.col("2g_total_volume_in") + f.col("2g_total_volume_out"),
        )
        .withColumn(
            "total_volume_in_weekend",
            f.col("4g_volume_in_weekend")
            + f.col("3g_volume_in_weekend")
            + f.col("2g_volume_in_weekend"),
        )
        .withColumn(
            "total_volume_out_weekend",
            f.col("4g_volume_out_weekend")
            + f.col("3g_volume_out_weekend")
            + f.col("2g_volume_out_weekend"),
        )
        .withColumn(
            "total_volume_weekend",
            f.col("4g_total_volume_weekend")
            + f.col("3g_total_volume_weekend")
            + f.col("2g_total_volume_weekend"),
        )
        .withColumn(
            "total_volume_in_weekday",
            f.col("4g_volume_in_weekday")
            + f.col("3g_volume_in_weekday")
            + f.col("2g_volume_in_weekday"),
        )
        .withColumn(
            "total_volume_out_weekday",
            f.col("4g_volume_out_weekday")
            + f.col("3g_volume_out_weekday")
            + f.col("2g_volume_out_weekday"),
        )
        .withColumn(
            "total_volume_weekday",
            f.col("4g_total_volume_weekday")
            + f.col("3g_total_volume_weekday")
            + f.col("2g_total_volume_weekday"),
        )
        .withColumn(
            "total_volume_in",
            f.col("4g_total_volume_in")
            + f.col("3g_total_volume_in")
            + f.col("2g_total_volume_in"),
        )
        .withColumn(
            "total_volume_out",
            f.col("4g_total_volume_out")
            + f.col("3g_total_volume_out")
            + f.col("2g_total_volume_out"),
        )
        .withColumn(
            "total_volume",
            f.col("4g_total_volume")
            + f.col("3g_total_volume")
            + f.col("2g_total_volume"),
        )
        .withColumn(
            "external_latency_weekday",
            f.col("4g_external_latency_weekday")
            + f.col("3g_external_latency_weekday")
            + f.col("2g_external_latency_weekday"),
        )
        .withColumn(
            "external_latency_weekend",
            f.col("4g_external_latency_weekend")
            + f.col("3g_external_latency_weekend")
            + f.col("2g_external_latency_weekend"),
        )
        .withColumn(
            "external_latency_count_weekday",
            f.col("4g_external_latency_count_weekday")
            + f.col("3g_external_latency_count_weekday")
            + f.col("2g_external_latency_count_weekday"),
        )
        .withColumn(
            "external_latency_count_weekend",
            f.col("4g_external_latency_count_weekend")
            + f.col("3g_external_latency_count_weekend")
            + f.col("2g_external_latency_count_weekend"),
        )
        .withColumn(
            "internal_latency_weekday",
            f.col("4g_internal_latency_weekday")
            + f.col("3g_internal_latency_weekday")
            + f.col("2g_internal_latency_weekday"),
        )
        .withColumn(
            "internal_latency_weekend",
            f.col("4g_internal_latency_weekend")
            + f.col("3g_internal_latency_weekend")
            + f.col("2g_internal_latency_weekend"),
        )
        .withColumn(
            "internal_latency_count_weekday",
            f.col("4g_internal_latency_count_weekday")
            + f.col("3g_internal_latency_count_weekday")
            + f.col("2g_internal_latency_count_weekday"),
        )
        .withColumn(
            "internal_latency_count_weekend",
            f.col("4g_internal_latency_count_weekend")
            + f.col("3g_internal_latency_count_weekend")
            + f.col("2g_internal_latency_count_weekend"),
        )
        .withColumn(
            "4g_latency_weekday",
            f.col("4g_internal_latency_weekday") + f.col("4g_external_latency_weekday"),
        )
        .withColumn(
            "4g_latency_weekend",
            f.col("4g_internal_latency_weekend") + f.col("4g_external_latency_weekend"),
        )
        .withColumn(
            "4g_latency_count_weekday",
            f.col("4g_internal_latency_count_weekday")
            + f.col("4g_external_latency_count_weekday"),
        )
        .withColumn(
            "4g_latency_count_weekend",
            f.col("4g_internal_latency_count_weekend")
            + f.col("4g_external_latency_count_weekend"),
        )
        .withColumn(
            "latency_weekday",
            f.col("internal_latency_weekday") + f.col("external_latency_weekday"),
        )
        .withColumn(
            "latency_weekend",
            f.col("internal_latency_weekend") + f.col("external_latency_weekend"),
        )
        .withColumn(
            "latency_count_weekday",
            f.col("internal_latency_count_weekday")
            + f.col("external_latency_count_weekday"),
        )
        .withColumn(
            "latency_count_weekend",
            f.col("internal_latency_count_weekend")
            + f.col("external_latency_count_weekend"),
        )
        .withColumn(
            "duration_weekday",
            f.col("4g_duration_weekday")
            + f.col("3g_duration_weekday")
            + f.col("2g_duration_weekday"),
        )
        .withColumn(
            "duration_weekend",
            f.col("4g_duration_weekend")
            + f.col("3g_duration_weekend")
            + f.col("2g_duration_weekend"),
        )
    )

    fea_df = get_config_based_features(
        df=df_internet_usage_weekly,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_total_download"
        ],
        column_expression=sum_of_columns_over_weekstart_window(["total_volume_in"]),
    )
    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_total_upload"
        ],
        column_expression=sum_of_columns_over_weekstart_window(["total_volume_out"]),
    )
    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_4g_download_share"
        ],
        column_expression=division_of_sum_of_columns_over_weekstart_window(
            "4g_total_volume_in", "total_volume_in"
        ),
    )
    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_3g_download_share"
        ],
        column_expression=division_of_sum_of_columns_over_weekstart_window(
            "3g_total_volume_in", "total_volume_in"
        ),
    )
    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_4g_upload_share"
        ],
        column_expression=division_of_sum_of_columns_over_weekstart_window(
            "4g_total_volume_out", "total_volume_out"
        ),
    )
    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_3g_upload_share"
        ],
        column_expression=division_of_sum_of_columns_over_weekstart_window(
            "3g_total_volume_out", "total_volume_out"
        ),
    )
    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_ext_latency_4g_weekdays"
        ],
        column_expression=division_of_sum_of_columns_over_weekstart_window(
            "4g_external_latency_weekday", "4g_external_latency_count_weekday"
        ),
    )
    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_ext_latency_4g_weekends"
        ],
        column_expression=division_of_sum_of_columns_over_weekstart_window(
            "4g_external_latency_weekend", "4g_external_latency_count_weekend"
        ),
    )
    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_ext_latency_weekdays"
        ],
        column_expression=division_of_sum_of_columns_over_weekstart_window(
            "external_latency_weekday", "external_latency_count_weekday"
        ),
    )
    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_ext_latency_weekends"
        ],
        column_expression=division_of_sum_of_columns_over_weekstart_window(
            "external_latency_weekend", "external_latency_count_weekend"
        ),
    )
    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_latency_4g_weekdays"
        ],
        column_expression=division_of_sum_of_columns_over_weekstart_window(
            "4g_latency_weekday", "4g_latency_count_weekday"
        ),
    )
    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_latency_4g_weekends"
        ],
        column_expression=division_of_sum_of_columns_over_weekstart_window(
            "4g_latency_weekend", "4g_latency_count_weekend"
        ),
    )
    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_latency_weekdays"
        ],
        column_expression=division_of_sum_of_columns_over_weekstart_window(
            "latency_weekday", "latency_count_weekday"
        ),
    )
    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_latency_weekends"
        ],
        column_expression=division_of_sum_of_columns_over_weekstart_window(
            "latency_weekend", "latency_count_weekend"
        ),
    )
    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_download_speed_4g_weekdays"
        ],
        column_expression=division_of_sum_of_columns_over_weekstart_window(
            "4g_volume_in_weekday", "4g_duration_weekday"
        ),
    )
    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_download_speed_4g_weekends"
        ],
        column_expression=division_of_sum_of_columns_over_weekstart_window(
            "4g_volume_in_weekend", "4g_duration_weekend"
        ),
    )
    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_download_speed_weekdays"
        ],
        column_expression=division_of_sum_of_columns_over_weekstart_window(
            "total_volume_in_weekday", "duration_weekday"
        ),
    )
    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_download_speed_weekends"
        ],
        column_expression=division_of_sum_of_columns_over_weekstart_window(
            "total_volume_in_weekend", "duration_weekend"
        ),
    )
    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_speed_4g_weekdays"
        ],
        column_expression=division_of_sum_of_columns_over_weekstart_window(
            "4g_total_volume_weekday", "4g_duration_weekday"
        ),
    )
    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_speed_4g_weekends"
        ],
        column_expression=division_of_sum_of_columns_over_weekstart_window(
            "4g_total_volume_weekend", "4g_duration_weekend"
        ),
    )
    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_speed_weekdays"
        ],
        column_expression=division_of_sum_of_columns_over_weekstart_window(
            "total_volume_weekday", "duration_weekday"
        ),
    )
    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_internet_app_usage[
            "fea_int_app_usage_speed_weekends"
        ],
        column_expression=division_of_sum_of_columns_over_weekstart_window(
            "total_volume_weekend", "duration_weekend"
        ),
    )

    output_features_columns = get_config_based_features_column_names(
        config_feature_internet_app_usage["fea_int_app_usage_total_download"],
        config_feature_internet_app_usage["fea_int_app_usage_total_upload"],
        config_feature_internet_app_usage["fea_int_app_usage_4g_download_share"],
        config_feature_internet_app_usage["fea_int_app_usage_3g_download_share"],
        config_feature_internet_app_usage["fea_int_app_usage_4g_upload_share"],
        config_feature_internet_app_usage["fea_int_app_usage_3g_upload_share"],
        config_feature_internet_app_usage["fea_int_app_usage_ext_latency_4g_weekdays"],
        config_feature_internet_app_usage["fea_int_app_usage_ext_latency_4g_weekends"],
        config_feature_internet_app_usage["fea_int_app_usage_ext_latency_weekdays"],
        config_feature_internet_app_usage["fea_int_app_usage_ext_latency_weekends"],
        config_feature_internet_app_usage["fea_int_app_usage_latency_4g_weekdays"],
        config_feature_internet_app_usage["fea_int_app_usage_latency_4g_weekends"],
        config_feature_internet_app_usage["fea_int_app_usage_latency_weekdays"],
        config_feature_internet_app_usage["fea_int_app_usage_latency_weekends"],
        config_feature_internet_app_usage[
            "fea_int_app_usage_download_speed_4g_weekdays"
        ],
        config_feature_internet_app_usage[
            "fea_int_app_usage_download_speed_4g_weekends"
        ],
        config_feature_internet_app_usage["fea_int_app_usage_download_speed_weekdays"],
        config_feature_internet_app_usage["fea_int_app_usage_download_speed_weekends"],
        config_feature_internet_app_usage["fea_int_app_usage_speed_4g_weekdays"],
        config_feature_internet_app_usage["fea_int_app_usage_speed_4g_weekends"],
        config_feature_internet_app_usage["fea_int_app_usage_speed_weekdays"],
        config_feature_internet_app_usage["fea_int_app_usage_speed_weekends"],
    )

    required_output_columns = get_required_output_columns(
        output_features=output_features_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=[
            "msisdn",
            "weekstart",
            "fea_int_app_usage_total_upload_01w",  # This feature is used in social media features calculation
            "fea_int_app_usage_total_upload_01m",  # This feature is used in social media features calculation
            "fea_int_app_usage_total_upload_02m",  # This feature is used in social media features calculation
            "fea_int_app_usage_total_download_01w",  # This feature is used in social media features calculation
            "fea_int_app_usage_total_download_01m",  # This feature is used in social media features calculation
            "fea_int_app_usage_total_download_02m",  # This feature is used in social media features calculation
        ],
    )

    fea_df = fea_df.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return fea_df.filter(f.col("weekstart").between(first_week_start, last_week_start))
