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
import math
from typing import List

import pyspark
import pyspark.sql.functions as f

from .helpers import get_period_in_days, remove_prefix_from_period_string
from .windows import get_rolling_window


def get_periods_dict(periods: list, is_month_mapped_to_date=None) -> dict:
    """
    Return period_string and period_days dictionary for a list of period_string

    Args:
        periods (list): period_string list

    Returns:
        dict: period_string and period_days dictionary for a list of period_string
    """
    return {
        period_string: get_period_in_days(
            remove_prefix_from_period_string(period_string), is_month_mapped_to_date
        )
        for period_string in periods
    }


def get_config_based_features(
    df: pyspark.sql.DataFrame,
    feature_config: dict,
    column_expression,
    return_only_feature_columns=False,
    is_month_mapped_to_date=None,
    columns_to_keep_from_original_df=None,
) -> pyspark.sql.DataFrame:
    if columns_to_keep_from_original_df is None:
        columns_to_keep_from_original_df = []
    feature_name = feature_config.get("feature_name")
    periods = feature_config.get("periods", [])
    periods_dict = get_periods_dict(periods, is_month_mapped_to_date)
    for period_string, period in periods_dict.items():
        df = df.withColumn(
            eval(f'f"""{feature_name}"""'), column_expression(period_string, period)
        )

    if return_only_feature_columns:
        return df.select(
            [
                *get_config_based_features_column_names(feature_config),
                *columns_to_keep_from_original_df,
            ]
        )

    return df


def get_config_based_features_column_names(*feature_config_list: list):
    columns = []
    for feature_config in feature_config_list:
        feature_name = feature_config.get("feature_name")
        periods = feature_config.get("periods", [])
        periods_dict = get_periods_dict(periods)
        columns = columns + [
            eval(f'f"""{feature_name}"""')
            for period_string, period in periods_dict.items()
        ]
    return columns


def sum_of_columns_over_weekstart_window(
    column_names: List[str], window_partition_optional_key: List[str] = None
):
    def expression(period_string, period):
        column_sums = []
        for column_name in column_names:
            column_sums.append(eval(f'f"""{column_name}"""'))
        if period == 7:
            return f.expr("+".join(column_sums))
        else:
            return f.sum(f.expr("+".join(column_sums))).over(
                get_rolling_window(
                    period, oby="weekstart", optional_keys=window_partition_optional_key
                )
            )

    return expression


def sum_of_columns_over_month_mapped_dt_window(column_names: list):
    def expression(period_string, period):
        column_sums = []
        for column_name in column_names:
            column_sums.append(eval(f'f"""{column_name}"""'))
        if period == 1:
            return f.expr("+".join(column_sums))
        else:
            return f.sum(f.expr("+".join(column_sums))).over(
                get_rolling_window(period, oby="month_mapped_dt")
            )

    return expression


def avg_over_weekstart_window(column_name):
    def expression(period_string, period):
        if period == 7:
            return f.col(eval(f'f"""{column_name}"""'))
        else:
            return f.avg(eval(f'f"""{column_name}"""')).over(
                get_rolling_window(period, oby="weekstart")
            )

    return expression


def avg_monthly_over_weekstart_window(column_name):
    def expression(period_string, period):
        return f.sum(eval(f'f"""{column_name}"""')).over(
            get_rolling_window(period, oby="weekstart")
        ) / math.floor(period / 28)

    return expression


def min_over_weekstart_window(column_name):
    def expression(period_string, period):
        if period == 7:
            return f.col(eval(f'f"""{column_name}"""'))
        else:
            return f.min(eval(f'f"""{column_name}"""')).over(
                get_rolling_window(period, oby="weekstart")
            )

    return expression


def max_over_weekstart_window(column_name):
    def expression(period_string, period):
        if period == 7:
            return f.col(eval(f'f"""{column_name}"""'))
        else:
            return f.max(eval(f'f"""{column_name}"""')).over(
                get_rolling_window(period, oby="weekstart")
            )

    return expression


def first_value_over_weekstart_window(column_name):
    def expression(period_string, period):
        if period == 7:
            return f.col(eval(f'f"""{column_name}"""'))
        else:
            return f.first(eval(f'f"""{column_name}"""')).over(
                get_rolling_window(period, oby="weekstart")
            )

    return expression


def diff_of_columns(col_a, col_b):
    return lambda period_string, period: f.col(eval(f'f"""{col_a}"""')) - f.col(
        eval(f'f"""{col_b}"""')
    )


def median_over_weekstart_window(column_name):
    def expression(period_string, period):
        if period == 7:
            return f.col(eval(f'f"""{column_name}"""'))
        else:
            return f.expr(eval(f'f"""percentile_approx({column_name}, 0.5)"""')).over(
                get_rolling_window(period, oby="weekstart")
            )

    return expression


def division_of_columns(col_a, col_b):
    return lambda period_string, period: f.col(eval(f'f"""{col_a}"""')) / f.col(
        eval(f'f"""{col_b}"""')
    )


def ratio_of_columns(col_a, col_b):
    return lambda period_string, period: f.col(eval(f'f"""{col_a}"""')) / (
        f.col(eval(f'f"""{col_b}"""')) + f.col(eval(f'f"""{col_a}"""'))
    )


def division_of_sum_of_columns_over_weekstart_window(
    column_numerator, column_denominator
):
    def expression(period_string, period):
        if period == 7:
            return f.col(eval(f'f"""{column_numerator}"""')) / f.col(
                eval(f'f"""{column_denominator}"""')
            )
        else:
            return f.sum(eval(f'f"""{column_numerator}"""')).over(
                get_rolling_window(period, oby="weekstart")
            ) / f.sum(eval(f'f"""{column_denominator}"""')).over(
                get_rolling_window(period, oby="weekstart")
            )

    return expression


def sum_of_columns(column_names: list):
    def expression(period_string, period):
        column_sums = []
        for column_name in column_names:
            column_sums.append(f.col(eval(f'f"""{column_name}"""')))
        return sum(column_sums)

    return expression


def std_deviation_of_column_over_weekstart_window(column_name):
    return lambda period_string, period: f.stddev(eval(f'f"""{column_name}"""')).over(
        get_rolling_window(period, oby="weekstart")
    )


def get_required_output_columns(
    output_features, feature_mode, feature_list, extra_columns_to_keep=None
):
    if extra_columns_to_keep is None:
        extra_columns_to_keep = []
    if feature_mode == "all":
        required_output_columns = set(output_features)
    else:
        required_output_columns = set(output_features).intersection(set(feature_list))
    required_output_columns = required_output_columns.union(set(extra_columns_to_keep))
    return list(required_output_columns)
