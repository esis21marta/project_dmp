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
from pyspark.sql.window import Window

from utils import (
    get_config_based_features,
    get_config_based_features_column_names,
    get_end_date,
    get_required_output_columns,
    get_rolling_window,
    get_start_date,
    join_all,
)


def fea_handset(
    df_handset_weekly: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    create feature for handset from weekly data

    Args:
        df_handset_weekly: Handset Weekly Data

    Returns:
        fea_df: Handset Features
    """

    config_feature_handset = config_feature["handset"]

    window = (
        Window.partitionBy("msisdn")
        .orderBy(f.col("weekstart"))
        .rowsBetween(Window.unboundedPreceding, -1)
    )

    fea_df = (
        df_handset_weekly.withColumn("month", f.trunc("weekstart", "month"))
        .withColumn("fea_handset_make_cur", f.col("manufacturer")[0])
        .withColumn(
            "fea_handset_make_cur", f.last("fea_handset_make_cur", True).over(window)
        )
        .withColumn("fea_handset_market_name", f.col("market_names")[0])
        .withColumn(
            "fea_handset_market_name",
            f.last("fea_handset_market_name", True).over(window),
        )
        .withColumn("fea_handset_type", f.col("device_types")[0])
        .withColumn("fea_handset_type", f.last("fea_handset_type", True).over(window))
        .withColumn(
            "imeis",
            f.array_distinct(f.expr("transform(imeis, x-> substring(x, 1, 14))")),
        )
    )

    def fea_handset_make(period_string, period):
        if period == 7:
            return f.col("fea_handset_make_cur")
        else:
            return f.first(f.col("fea_handset_make_cur")).over(
                get_rolling_window(period, oby="weekstart")
            )

    def fea_handset_count(period_string, period):
        return f.size(
            f.array_distinct(
                f.flatten(
                    f.collect_set("imeis").over(
                        get_rolling_window(period, oby="weekstart")
                    )
                )
            )
        )

    def fea_handset_changed_count(period_string, period):
        return f.when(
            f.col(f"fea_handset_count_{period_string}") > 0,
            f.col(f"fea_handset_count_{period_string}") - 1,
        ).otherwise(0)

    df_handset = join_all(
        [
            fea_df,
            get_config_based_features(
                fea_df,
                config_feature_handset["fea_handset_make"],
                fea_handset_make,
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                fea_df,
                config_feature_handset["fea_handset_count"],
                fea_handset_count,
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="outer",
    )

    df_handset = get_config_based_features(
        df_handset,
        config_feature_handset["fea_handset_changed_count"],
        fea_handset_changed_count,
    )

    df_handset = df_handset.withColumn("fea_handset_imeis", f.col("imeis"))

    output_features_columns = [
        "fea_handset_market_name",
        "fea_handset_type",
        "fea_handset_imeis",
        "fea_handset_make_cur",
        *get_config_based_features_column_names(
            config_feature_handset["fea_handset_make"],
            config_feature_handset["fea_handset_count"],
            config_feature_handset["fea_handset_changed_count"],
        ),
    ]

    required_output_columns = get_required_output_columns(
        output_features=output_features_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    df_handset = df_handset.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return df_handset.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )
