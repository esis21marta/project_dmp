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

from .windows import get_rolling_window_fixed, get_window_to_pick_last_row


def get_df_with_mode_cols(
    df: pyspark.sql.DataFrame, rolled_col_names: List[str], primary_column: str
) -> Dict[str, Any]:
    """
    Given a dataframe and a list of columns (with list datatype), return the dataframe with the mode of each columns.

    Args:
        df: Dataframe to compute modes on
        rolled_col_names: List of columns (with list datatype)
        primary_column: Columns to groupby on, most of the times it's [msisdn, weekstart]

    Returns: Dictionary with two elements:
        - dataframe: Dataframe with mode of each rolled_col_names (ending with _mode)
        - mode_columns: Newly generated mode columns
    """

    mode_columns = []
    mode_df = df
    primary_columns = [primary_column, "weekstart"]

    for rolled_col_name in rolled_col_names:
        output_col_name = f"mode_{rolled_col_name}"

        agg_cols = rolled_col_names + mode_columns
        agg_exprs = [
            f.first(f.col(col_name)).alias(col_name) for col_name in agg_cols
        ] + [f.count("exploded").alias("count")]

        mode_df = (
            mode_df.withColumn("exploded", f.explode_outer(rolled_col_name))
            .groupBy(*primary_columns, "exploded")
            .agg(*agg_exprs)
            .withColumn(
                "order",
                f.row_number().over(
                    get_window_to_pick_last_row(key=primary_column, order_by="count")
                ),
            )
            .where(f.col("order") == 1)
            .select(
                primary_columns
                + rolled_col_names
                + mode_columns
                + [f.col("exploded").alias(output_col_name)]
            )
        )

        mode_columns.append(output_col_name)

    mode_df = mode_df.select(primary_columns + mode_columns)
    output_df = df.join(mode_df, primary_columns, "left")

    return {"dataframe": output_df, "mode_columns": mode_columns}


def append_different_windows(
    df: pyspark.sql.DataFrame, col_names: List[str], windows: List[List[int]]
) -> Dict[str, Any]:
    """
    Given a dataframe, column names (type: list) and windows (2d-list)

    Args:
        df: Dataframe
        col_names: List of columns which you want to apply windows upon to collect as list
        windows: 2d-list of window start-end days

    Returns: Dictionary with two elements:
               - dataframe: Dataframe with new colummns of lists of each rolled_col_names
               - window_columns: Newly generated rolling window columns of list
    """

    day_mapping = {
        0: "00w",
        7: "01w",
        14: "02w",
        21: "03w",
        28: "01m",
        42: "06w",
        56: "02m",
        91: "03m",
        105: "15w",
        119: "04m",
    }

    out_cols = []

    for col_name in col_names:
        for window in windows:
            start_window = window[0]
            end_window = window[1]
            start_str = day_mapping.get(start_window)
            end_str = day_mapping.get(end_window)

            out_col = f"{col_name}_{end_str}_to_{start_str}"

            df = df.withColumn(
                out_col,
                f.collect_list(f.col(col_name)).over(
                    get_rolling_window_fixed(start_window, end_window, oby="weekstart")
                ),
            )

            out_cols.append(out_col)

    return {"dataframe": df, "window_columns": out_cols}
