from typing import List

import pyspark
import pyspark.sql.functions as f

from utils import (
    get_config_based_features,
    get_config_based_features_column_names,
    get_end_date,
    get_required_output_columns,
    get_start_date,
    sum_of_columns_over_weekstart_window,
)


def check_has_login(period_string: str, period: int):
    return f.col(f"fea_mytsel_login_count_days_{period_string}") > 0


def fea_mytsel(
    df: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    fea_mytsel = df.fillna(value=0, subset=["mytsel_login_count_days"])
    fea_mytsel = get_config_based_features(
        df=fea_mytsel,
        feature_config=config_feature["fea_mytsel_login_count_days"],
        column_expression=sum_of_columns_over_weekstart_window(
            ["mytsel_login_count_days"]
        ),
    )
    fea_mytsel = get_config_based_features(
        df=fea_mytsel,
        feature_config=config_feature["fea_mytsel_has_login"],
        column_expression=check_has_login,
    )

    output_columns = [
        "msisdn",
        "weekstart",
        *get_config_based_features_column_names(
            config_feature["fea_mytsel_login_count_days"],
            config_feature["fea_mytsel_has_login"],
        ),
    ]

    required_output_columns = get_required_output_columns(
        output_features=output_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    fea_mytsel = fea_mytsel.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return fea_mytsel.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )
