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
from deprecated import deprecated
from pyspark.sql.window import Window

from src.dmp.pipelines.dmp._04_features.product.constants import WINDOWS_MAPPING
from utils import (
    get_end_date,
    get_required_output_columns,
    get_rolling_window,
    get_start_date,
)


@deprecated(version="0.1", reason="Some of product tables not productionized")
def create_product_features(
    df_macroproduct_weekly: pyspark.sql.DataFrame,
    macroproduct_list: list,
    days: int,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Base function for monthly and quarterly features

    Args:
        df_macroproduct_weekly: Macroproduct Aggregation Dataframe
        macroproduct_list: List of macroproduct
        days: Number of days for the rolling window

    Returns:
        Dataframe of Macroproduct Features
    """
    input_cols = [
        ("fea_product_min_validity_period_days_01w", "min_validity_period_days"),
        ("fea_product_max_validity_period_days_01w", "max_validity_period_days"),
        ("fea_product_count_validity_1_to_4_days_01w", "count_validity_1_to_4_days"),
        ("fea_product_count_validity_5_to_10_days_01w", "count_validity_5_to_10_days"),
        (
            "fea_product_count_validity_11_to_22_days_01w",
            "count_validity_11_to_22_days",
        ),
        (
            "fea_product_count_validity_23_to_45_days_01w",
            "count_validity_23_to_45_days",
        ),
        ("fea_product_count_validity_46plus_days_01w", "count_validity_46plus_days"),
    ]
    input_cols = input_cols + [
        ("fea_product_count_{}_01w".format(i), ("count_{}").format(i))
        for i in macroproduct_list
    ]
    for fea_01w, cols in input_cols:
        fea_name = "fea_product_{}_{}".format(cols, WINDOWS_MAPPING[days])
        if cols.startswith("min_"):
            df_macroproduct_weekly = df_macroproduct_weekly.withColumn(
                fea_name,
                f.min(fea_01w).over(get_rolling_window(days, oby="weekstart")),
            )
        elif cols.startswith("max_"):
            df_macroproduct_weekly = df_macroproduct_weekly.withColumn(
                fea_name,
                f.max(fea_01w).over(get_rolling_window(days, oby="weekstart")),
            )
        elif cols.startswith("count_"):
            df_macroproduct_weekly = df_macroproduct_weekly.withColumn(
                fea_name,
                f.sum(fea_01w).over(get_rolling_window(days, oby="weekstart")),
            )

    drop_cols = [
        i
        for i in df_macroproduct_weekly.columns
        if (not i.startswith("fea") and not i in ("msisdn", "weekstart"))
        or ("01w" in i)
    ]

    output_features_columns = set(df_macroproduct_weekly.columns) - set(drop_cols)

    required_output_columns = get_required_output_columns(
        output_features=output_features_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    df_macroproduct_weekly = df_macroproduct_weekly.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return df_macroproduct_weekly.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )


@deprecated(version="0.1", reason="Some of product tables not productionized")
def create_product_features_monthly(
    df_macroproduct_weekly: pyspark.sql.DataFrame,
    macroproduct_list: list,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Generate feature for product Domain, specifically for Macroproduct features
    in monthly window

    Args:
        df_macroproduct_weekly: Macroproduct Aggregation Dataframe
        macroproduct_list: List of macroproduct

    Returns:
        Dataframe of Monthly Macroproduct Features
    """
    days = 28
    return create_product_features(
        df_macroproduct_weekly,
        macroproduct_list,
        days,
        feature_mode,
        required_output_features,
    )


@deprecated(version="0.1", reason="Some of product tables not productionized")
def create_product_features_quarterly(
    df_macroproduct_weekly: pyspark.sql.DataFrame,
    macroproduct_list: list,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Generate feature for product Domain, specifically for Macroproduct features
    in quarterly window

    Args:
        df_macroproduct_weekly: Macroproduct Aggregation Dataframe
        macroproduct_list: List of macroproduct

    Returns:
        Dataframe of Quarterly Macroproduct Features
    """
    days = 91
    return create_product_features(
        df_macroproduct_weekly,
        macroproduct_list,
        days,
        feature_mode,
        required_output_features,
    )


@deprecated(version="0.1", reason="Some of product tables not productionized")
def create_macroproduct_features(
    df_macroproduct_weekly: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Generate feature for product Domain, specifically for Macroproduct features
    that requires exploding the macroproduct_count_list

    Args:
        df_macroproduct_weekly: Macroproduct Aggregation Dataframe

    Returns:
        Dataframe of Feature Macroproduct
    """
    df_macroproduct_weekly = df_macroproduct_weekly.select(
        "msisdn", "weekstart", "macroproduct_count_list"
    )

    fea_macroproduct = (
        df_macroproduct_weekly.withColumn(
            "fea_product_total_activated_product_01w",
            f.size(
                f.flatten(
                    f.collect_list(f.col("macroproduct_count_list")).over(
                        get_rolling_window(7, oby="weekstart")
                    )
                )
            ),
        )
        .withColumn(
            "fea_product_total_activated_product_01m",
            f.size(
                f.flatten(
                    f.collect_list(f.col("macroproduct_count_list")).over(
                        get_rolling_window(28, oby="weekstart")
                    )
                )
            ),
        )
        .withColumn(
            "fea_product_total_activated_product_03m",
            f.size(
                f.flatten(
                    f.collect_list(f.col("macroproduct_count_list")).over(
                        get_rolling_window(91, oby="weekstart")
                    )
                )
            ),
        )
        .withColumn("macroproduct_list", f.explode(f.col("macroproduct_count_list")))
        .withColumn("macroproduct", f.col("macroproduct_list").getItem(0))
        .withColumn("macroproduct_count", f.col("macroproduct_list").getItem(1))
        .withColumn("macroproduct_validity", f.col("macroproduct_list").getItem(2))
        .withColumn(
            "weekstarts",
            f.collect_list(f.col("weekstart")).over(
                get_rolling_window(-1, oby="weekstart")
            ),
        )
        .withColumn(
            "validities",
            f.collect_list(f.col("macroproduct_validity")).over(
                get_rolling_window(-1, oby="weekstart")
            ),
        )
        .withColumn(
            "date_diff", f.expr("transform(weekstarts, x -> datediff(weekstart, x))")
        )
        .withColumn("datediff_validity", f.arrays_zip("date_diff", "validities"))
        .withColumn(
            "fea_product_total_active_product",
            f.size(
                f.expr(
                    "filter(transform(datediff_validity, x->x.validities-x.date_diff), x->x>-1)"
                )
            ),
        )
        .withColumn(
            "prev_macroproduct",
            f.lag(f.col("macroproduct")).over(
                Window.partitionBy("msisdn").orderBy(["weekstart"])
            ),
        )
        .withColumn(
            "change_flag",
            f.when(
                (f.col("prev_macroproduct").isNotNull())
                & (f.col("macroproduct") != f.col("prev_macroproduct")),
                f.lit(1),
            ).otherwise(0),
        )
        .withColumn(
            "fea_product_total_product_changes_01w",
            f.sum(f.col("change_flag")).over(get_rolling_window(7, oby="weekstart")),
        )
        .withColumn(
            "fea_product_total_product_changes_01m",
            f.sum(f.col("change_flag")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_product_total_product_changes_03m",
            f.sum(f.col("change_flag")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn(
            "total_macro_01w",
            f.sum("macroproduct_count").over(
                get_rolling_window(7, oby="weekstart", optional_keys=["macroproduct"])
            ),
        )
        .withColumn(
            "macro_rank_01w",
            f.row_number().over(
                Window()
                .partitionBy(f.col("msisdn"), f.col("weekstart"))
                .orderBy(f.col("total_macro_01w").desc())
            ),
        )
        .withColumn(
            "total_macro_01m",
            f.sum("macroproduct_count").over(
                get_rolling_window(28, oby="weekstart", optional_keys=["macroproduct"])
            ),
        )
        .withColumn(
            "macro_rank_01m",
            f.row_number().over(
                Window()
                .partitionBy(f.col("msisdn"), f.col("weekstart"))
                .orderBy(f.col("total_macro_01m").desc())
            ),
        )
        .withColumn(
            "total_macro_03m",
            f.sum("macroproduct_count").over(
                get_rolling_window(91, oby="weekstart", optional_keys=["macroproduct"])
            ),
        )
        .withColumn(
            "macro_rank_03m",
            f.row_number().over(
                Window()
                .partitionBy(f.col("msisdn"), f.col("weekstart"))
                .orderBy(f.col("total_macro_03m").desc())
            ),
        )
        .withColumn(
            "fea_product_most_common_category_01w",
            f.when(f.col("macro_rank_01w") == 1, f.col("macroproduct")).otherwise(None),
        )
        .withColumn(
            "fea_product_most_common_category_01m",
            f.when(f.col("macro_rank_01m") == 1, f.col("macroproduct")).otherwise(None),
        )
        .withColumn(
            "fea_product_most_common_category_03m",
            f.when(f.col("macro_rank_03m") == 1, f.col("macroproduct")).otherwise(None),
        )
        .withColumn(
            "fea_product_length_active_most_common_category_01w",
            f.when(
                f.col("macro_rank_01w") == 1, f.col("macroproduct_validity")
            ).otherwise(None),
        )
        .withColumn(
            "fea_product_length_active_most_common_category_01m",
            f.when(
                f.col("macro_rank_01m") == 1, f.col("macroproduct_validity")
            ).otherwise(None),
        )
        .withColumn(
            "fea_product_length_active_most_common_category_03m",
            f.when(
                f.col("macro_rank_03m") == 1, f.col("macroproduct_validity")
            ).otherwise(None),
        )
    )
    fea_df = (
        fea_macroproduct.groupBy("msisdn", "weekstart").agg(
            f.max("fea_product_total_activated_product_01w").alias(
                "fea_product_total_activated_product_01w"
            ),
            f.max("fea_product_total_activated_product_01m").alias(
                "fea_product_total_activated_product_01m"
            ),
            f.max("fea_product_total_activated_product_03m").alias(
                "fea_product_total_activated_product_03m"
            ),
            f.max("fea_product_total_active_product").alias(
                "fea_product_total_active_product"
            ),
            f.max("fea_product_total_product_changes_01w").alias(
                "fea_product_total_product_changes_01w"
            ),
            f.max("fea_product_total_product_changes_01m").alias(
                "fea_product_total_product_changes_01m"
            ),
            f.max("fea_product_total_product_changes_03m").alias(
                "fea_product_total_product_changes_03m"
            ),
            f.max("fea_product_most_common_category_01w").alias(
                "fea_product_most_common_category_01w"
            ),
            f.max("fea_product_most_common_category_01m").alias(
                "fea_product_most_common_category_01m"
            ),
            f.max("fea_product_most_common_category_03m").alias(
                "fea_product_most_common_category_03m"
            ),
            f.max("fea_product_length_active_most_common_category_01w").alias(
                "fea_product_length_active_most_common_category_01w"
            ),
            f.max("fea_product_length_active_most_common_category_01m").alias(
                "fea_product_length_active_most_common_category_01m"
            ),
            f.max("fea_product_length_active_most_common_category_03m").alias(
                "fea_product_length_active_most_common_category_03m"
            ),
        )
    ).filter(
        (f.col("fea_product_most_common_category_01w").isNotNull())
        & (f.col("fea_product_most_common_category_01m").isNotNull())
        & (f.col("fea_product_most_common_category_03m").isNotNull())
    )

    required_output_columns = get_required_output_columns(
        output_features=fea_df.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    fea_df = fea_df.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")
    return fea_df.filter(f.col("weekstart").between(first_week_start, last_week_start))


@deprecated(version="0.1", reason="Some of product tables not productionized")
def fea_macroproduct_all(
    df_macroproduct_weekly: pyspark.sql.DataFrame,
    df_macroproduct_monthly: pyspark.sql.DataFrame,
    df_macroproduct_quarterly: pyspark.sql.DataFrame,
    df_macroproduct_explode: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Join all the features related to macroproduct

    Args:
        df_macroproduct_weekly: Macroproduct Aggregation Dataframe
        df_macroproduct_monthly: Macroproduct Monthly Features Dataframe
        df_macroproduct_quarterly: Macroproduct Quarterly Features Dataframe
        df_macroproduct_explode: Macroproduct Exploded Features Dataframe

    Returns:
        Dataframe of Macroproduct Features
    """

    drop_cols = [
        i
        for i in df_macroproduct_weekly.columns
        if not i.startswith("fea") and not i in ("msisdn", "weekstart")
    ]

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    df_macroproduct_weekly = df_macroproduct_weekly.drop(*drop_cols)

    df_macroproduct_weekly = df_macroproduct_weekly.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )

    fea_df = (
        df_macroproduct_weekly.join(
            df_macroproduct_monthly, ["msisdn", "weekstart"], how="left"
        )
        .join(df_macroproduct_quarterly, ["msisdn", "weekstart"], how="left")
        .join(df_macroproduct_explode, ["msisdn", "weekstart"], how="left")
    )
    fillna_cols = [i for i in fea_df.columns if "count" in i]

    fea_df = fea_df.fillna(0, subset=fillna_cols)

    output_features_columns = set(fea_df.columns) - set(drop_cols)

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
