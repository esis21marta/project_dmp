from typing import Any, Dict, List

import pyspark
import pyspark.sql.functions as f
from pyspark.sql.window import Window

from utils import (
    get_config_based_features,
    get_config_based_features_column_names,
    get_end_date,
    get_period_in_days,
    get_required_output_columns,
    get_rolling_window,
    get_start_date,
    join_all,
    remove_prefix_from_period_string,
    sum_of_columns_over_weekstart_window,
)


def mode_over_weekstart_window(
    column_name: str, partitions: List[str], orderby_columns: List[Dict[str, Any]]
):
    def expression(period_string, period):
        window = Window().partitionBy(*partitions)
        orderBy = []
        for item in orderby_columns:
            if item["ascending"]:
                orderBy.append(f.col(eval(f'f"""{item["column_name"]}"""')))
            else:
                orderBy.append(f.col(eval(f'f"""{item["column_name"]}"""')).desc())
        window = window.orderBy(orderBy)
        return f.when(
            f.isnull(
                f.first(
                    f.col(eval(f'f"""{orderby_columns[0]["column_name"]}"""'))
                ).over(window)
            ),
            None,
        ).otherwise(f.first(f.col(column_name)).over(window))

    return expression


def get_dominant_feature(
    df: pyspark.sql.DataFrame,
    column_name: str,
    config_feature_by_rev: Dict[str, Any],
    config_feature_by_trx: Dict[str, Any],
) -> pyspark.sql.DataFrame:
    common_periods_for_dominant_feature_by_rev_and_trx = list(
        set(config_feature_by_rev["periods"] + config_feature_by_trx["periods"])
    )
    max_period_in_days = max(
        [
            get_period_in_days(remove_prefix_from_period_string(period_string))
            for period_string in common_periods_for_dominant_feature_by_rev_and_trx
        ]
    )
    df_msisdn_weekstart_feature = (
        df.withColumn(
            f"{column_name}_{max_period_in_days}_days",
            f.collect_set(f.col(column_name)).over(
                get_rolling_window(max_period_in_days, oby="weekstart")
            ),
        )
        .select(["msisdn", "weekstart", f"{column_name}_{max_period_in_days}_days",])
        .drop_duplicates()
        .withColumn(
            column_name, f.explode_outer(f"{column_name}_{max_period_in_days}_days",)
        )
        .select("msisdn", "weekstart", column_name)
    )

    df = df_msisdn_weekstart_feature.join(
        df, on=["msisdn", "weekstart", column_name], how="left"
    )

    df = get_config_based_features(
        df,
        feature_config={
            "feature_name": "fea_rev_mytsel_tot_rev_{period_string}",
            "periods": common_periods_for_dominant_feature_by_rev_and_trx,
        },
        column_expression=sum_of_columns_over_weekstart_window(
            ["total_rev"], window_partition_optional_key=[column_name]
        ),
    )
    df = get_config_based_features(
        df,
        feature_config={
            "feature_name": "fea_rev_mytsel_tot_trx_{period_string}",
            "periods": common_periods_for_dominant_feature_by_rev_and_trx,
        },
        column_expression=sum_of_columns_over_weekstart_window(
            ["total_trx"], window_partition_optional_key=[column_name]
        ),
    )

    df = get_config_based_features(
        df,
        feature_config=config_feature_by_rev,
        column_expression=mode_over_weekstart_window(
            column_name=column_name,
            partitions=["msisdn", "weekstart"],
            orderby_columns=[
                {
                    "column_name": "fea_rev_mytsel_tot_rev_{period_string}",
                    "ascending": False,
                },
                {
                    "column_name": "fea_rev_mytsel_tot_trx_{period_string}",
                    "ascending": False,
                },
            ],
        ),
    )
    df = get_config_based_features(
        df,
        feature_config=config_feature_by_trx,
        column_expression=mode_over_weekstart_window(
            column_name=column_name,
            partitions=["msisdn", "weekstart"],
            orderby_columns=[
                {
                    "column_name": "fea_rev_mytsel_tot_trx_{period_string}",
                    "ascending": False,
                },
                {
                    "column_name": "fea_rev_mytsel_tot_rev_{period_string}",
                    "ascending": False,
                },
            ],
        ),
    )
    df = df.withColumn(
        "rownum",
        f.row_number().over(
            Window.partitionBy("msisdn", "weekstart").orderBy("weekstart")
        ),
    ).filter(f.col("rownum") == 1)
    df = df.select(
        [
            "msisdn",
            "weekstart",
            *get_config_based_features_column_names(
                config_feature_by_rev, config_feature_by_trx
            ),
        ]
    )
    return df


def fea_revenue_mytsel(
    df: pyspark.sql.DataFrame,
    config_feature: Dict[str, Any],
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    df.cache()
    weekly_rev_and_trx = df.groupby(["msisdn", "weekstart"]).agg(
        f.sum(f.col("total_rev")).alias("total_rev"),
        f.sum(f.col("total_trx")).alias("total_trx"),
    )
    weekly_rev_and_trx.cache()
    df_fea_revenue_mytsel = join_all(
        [
            get_config_based_features(
                weekly_rev_and_trx,
                feature_config=config_feature["fea_rev_mytsel_tot_rev"],
                column_expression=sum_of_columns_over_weekstart_window(["total_rev"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                weekly_rev_and_trx,
                feature_config=config_feature["fea_rev_mytsel_tot_trx"],
                column_expression=sum_of_columns_over_weekstart_window(["total_trx"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_dominant_feature(
                df,
                "product_name",
                config_feature["fea_rev_mytsel_product_mode_by_rev"],
                config_feature["fea_rev_mytsel_product_mode_by_trx"],
            ),
            get_dominant_feature(
                df,
                "service",
                config_feature["fea_rev_mytsel_service_mode_by_rev"],
                config_feature["fea_rev_mytsel_service_mode_by_trx"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="left",
    )
    output_columns = [
        "msisdn",
        "weekstart",
        *get_config_based_features_column_names(
            config_feature["fea_rev_mytsel_tot_rev"],
            config_feature["fea_rev_mytsel_tot_trx"],
            config_feature["fea_rev_mytsel_product_mode_by_rev"],
            config_feature["fea_rev_mytsel_product_mode_by_trx"],
            config_feature["fea_rev_mytsel_service_mode_by_rev"],
            config_feature["fea_rev_mytsel_service_mode_by_trx"],
        ),
    ]

    required_output_columns = get_required_output_columns(
        output_features=output_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    df_fea_revenue_mytsel = df_fea_revenue_mytsel.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return df_fea_revenue_mytsel.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )
