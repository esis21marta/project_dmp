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

from typing import Any, Dict, List

import pyspark
import pyspark.sql.functions as f
from pyspark.sql import Window

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
    config_feature_by_value: Dict[str, Any],
    config_feature_by_trx: Dict[str, Any],
) -> pyspark.sql.DataFrame:
    common_periods_for_dominant_feature_by_value_and_trx = list(
        set(config_feature_by_value["periods"] + config_feature_by_trx["periods"])
    )

    max_period_in_days = max(
        [
            get_period_in_days(remove_prefix_from_period_string(period_string))
            for period_string in common_periods_for_dominant_feature_by_value_and_trx
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

    df = df.fillna(value=0, subset=["tot_amt_sum", "tot_trx_sum"])

    df = get_config_based_features(
        df,
        feature_config={
            "feature_name": "fea_rech_mytsel_tot_amt_sum_{period_string}",
            "periods": common_periods_for_dominant_feature_by_value_and_trx,
        },
        column_expression=sum_of_columns_over_weekstart_window(
            ["tot_amt_sum"], window_partition_optional_key=[column_name]
        ),
    )
    df = get_config_based_features(
        df,
        feature_config={
            "feature_name": "fea_rech_mytsel_tot_trx_sum_{period_string}",
            "periods": common_periods_for_dominant_feature_by_value_and_trx,
        },
        column_expression=sum_of_columns_over_weekstart_window(
            ["tot_trx_sum"], window_partition_optional_key=[column_name]
        ),
    )
    df = get_config_based_features(
        df,
        feature_config=config_feature_by_value,
        column_expression=mode_over_weekstart_window(
            column_name=column_name,
            partitions=["msisdn", "weekstart"],
            orderby_columns=[
                {
                    "column_name": "fea_rech_mytsel_tot_amt_sum_{period_string}",
                    "ascending": False,
                },
                {
                    "column_name": "fea_rech_mytsel_tot_trx_sum_{period_string}",
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
                    "column_name": "fea_rech_mytsel_tot_trx_sum_{period_string}",
                    "ascending": False,
                },
                {
                    "column_name": "fea_rech_mytsel_tot_amt_sum_{period_string}",
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
                config_feature_by_value, config_feature_by_trx
            ),
        ]
    )
    return df


def fea_recharge_mytsel(
    df_rech_mytsel_weekly: pyspark.sql.DataFrame,
    config_feature: Dict[str, Any],
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Calculates features on topup on mytsel channel behavior:
        - df_rech_mytsel_weekly table is already aggregated on msisdn, weekstart,
        - For each msisdn, weekstart each feature is calculated over a window using get_rolling_window(time_period, orderByColumn)
        - A feature at a given weekstart represents the value of that feature looking n weeks till the current week
          (where n represents the size of the window)

    Args:
        df_rech_mytsel_weekly: Recharges weekly level table

    Returns:
        df_rech_mytsel: Dataframe with following features
            - msisdn: Unique Id
            - weekstart: Weekly observation point
            - fea_rech_mytsel_tot_amt_sum_01w : sum of recharge amount from mytsel channel in 1 week window
            - fea_rech_mytsel_tot_amt_sum_02w : sum of recharge amount from mytsel channel in 2 weeks window
            - fea_rech_mytsel_tot_amt_sum_03w : sum of recharge amount from mytsel channel in 3 weeks window
            - fea_rech_mytsel_tot_amt_sum_01m : sum of recharge amount from mytsel channel in 1 month window
            - fea_rech_mytsel_tot_amt_sum_02m : sum of recharge amount from mytsel channel in 2 months window
            - fea_rech_mytsel_tot_amt_sum_03m : sum of recharge amount from mytsel channel in 3 months window
            - fea_rech_mytsel_tot_trx_sum_01w : sum of recharge transaction from mytsel channel in 1 week window
            - fea_rech_mytsel_tot_trx_sum_02w : sum of recharge transaction from mytsel channel in 2 weeks window
            - fea_rech_mytsel_tot_trx_sum_03w : sum of recharge transaction from mytsel channel in 3 weeks window
            - fea_rech_mytsel_tot_trx_sum_01m : sum of recharge transaction from mytsel channel in 1 month window
            - fea_rech_mytsel_tot_trx_sum_02m : sum of recharge transaction from mytsel channel in 2 months window
            - fea_rech_mytsel_tot_trx_sum_03m : sum of recharge transaction from mytsel channel in 3 months window
            - fea_rech_mytsel_payment_mode_by_trx_01w : mode of recharge payment by transaction in 1 week windows
            - fea_rech_mytsel_payment_mode_by_trx_02w : mode of recharge payment by transaction in 2 weeks windows
            - fea_rech_mytsel_payment_mode_by_trx_03w : mode of recharge payment by transaction in 3 weeks windows
            - fea_rech_mytsel_payment_mode_by_trx_01m : mode of recharge payment by transaction in 1 month windows
            - fea_rech_mytsel_payment_mode_by_trx_02m : mode of recharge payment by transaction in 2 months windows
            - fea_rech_mytsel_payment_mode_by_trx_03m : mode of recharge payment by transaction in 3 months windows
            - fea_rech_mytsel_payment_mode_by_value_01w : mode of recharge payment by value in 1 week windows
            - fea_rech_mytsel_payment_mode_by_value_02w : mode of recharge payment by value in 2 weeks windows
            - fea_rech_mytsel_payment_mode_by_value_03w : mode of recharge payment by value in 3 weeks windows
            - fea_rech_mytsel_payment_mode_by_value_01m : mode of recharge payment by value in 1 month windows
            - fea_rech_mytsel_payment_mode_by_value_02m : mode of recharge payment by value in 2 months windows
            - fea_rech_mytsel_payment_mode_by_value_03m : mode of recharge payment by value in 3 months windows

    """

    weekly_rech_and_trx = df_rech_mytsel_weekly.groupby(["msisdn", "weekstart"]).agg(
        f.sum(f.col("tot_amt_sum")).alias("tot_amt_sum"),
        f.sum(f.col("tot_trx_sum")).alias("tot_trx_sum"),
    )

    weekly_rech_and_trx.cache()

    #### FEATURE IMPLEMENTATION : Rech Trx and Amt
    df_rech_mytsel = join_all(
        [
            get_config_based_features(
                df=weekly_rech_and_trx,
                feature_config=config_feature["fea_rech_mytsel_tot_amt_sum"],
                column_expression=sum_of_columns_over_weekstart_window(["tot_amt_sum"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df=weekly_rech_and_trx,
                feature_config=config_feature["fea_rech_mytsel_tot_trx_sum"],
                column_expression=sum_of_columns_over_weekstart_window(["tot_trx_sum"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_dominant_feature(
                df_rech_mytsel_weekly,
                "store_location",
                config_feature["fea_rech_mytsel_payment_mode_by_value"],
                config_feature["fea_rech_mytsel_payment_mode_by_trx"],
            ),
        ],
        on=["msisdn", "weekstart"],
        how="left",
    )

    df_rech_mytsel_columns = get_config_based_features_column_names(
        config_feature["fea_rech_mytsel_tot_amt_sum"],
        config_feature["fea_rech_mytsel_tot_trx_sum"],
        config_feature["fea_rech_mytsel_payment_mode_by_trx"],
        config_feature["fea_rech_mytsel_payment_mode_by_value"],
    )

    required_output_columns = get_required_output_columns(
        output_features=df_rech_mytsel_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    df_rech_mytsel = df_rech_mytsel.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    df_rech_mytsel = df_rech_mytsel.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )

    return df_rech_mytsel
