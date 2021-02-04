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

from typing import Dict, List

import pyspark
import pyspark.sql.functions as F

from utils import (
    append_different_windows,
    get_df_with_mode_cols,
    get_end_date,
    get_required_output_columns,
    get_start_date,
)


def retrieve_week_windows(
    df_device_lj_scaff: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Create new rolling WEEK window columns for (type: list) which are week_windows * columns_of_interest permutations.

    Args:
        df_device_lj_scaff: Dataframe to compute rolling windows.

    Returns:
        Dataframe with additional WEEK columns with a list type which are rolling week_windows flattened list
        for each of the columns_of_interest.
    """
    if "feature_list" == feature_mode:
        required_output_features = [
            fea[26:]
            for fea in required_output_features
            if fea.startswith("fea_handset_internal_mode_")
        ]

    week_windows = [
        [7, 0],
        [28, 14],
    ]

    columns_of_interest = [
        "manufacture",
        "design_type",
        "device_type",
        "os",
        "network",
        "volte",
        "multisim",
        "card_type",
        "tac",
    ]

    output = append_different_windows(
        df_device_lj_scaff, columns_of_interest, week_windows
    )
    output_df = output["dataframe"]
    output_features_columns = output["window_columns"]

    required_output_columns = get_required_output_columns(
        output_features=output_features_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    output_df = output_df.select(required_output_columns)
    return output_df


def retrieve_month_windows(
    df_device_lj_scaff: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Create new rolling MONTH window columns for (type: list) which are week_windows * columns_of_interest permutations.

    Args:
        df_device_lj_scaff: Dataframe to compute rolling windows.

    Returns:
        Dataframe with additional MONTH columns with a list type which are rolling week_windows flattened list
        for each of the columns_of_interest.
    """
    if "feature_list" == feature_mode:
        required_output_features = [
            fea[26:]
            for fea in required_output_features
            if fea.startswith("fea_handset_internal_mode_")
        ]

    month_windows = [[56, 42], [119, 105]]

    columns_of_interest = [
        "manufacture",
        "design_type",
        "device_type",
        "os",
        "network",
        "volte",
        "multisim",
        "card_type",
        "tac",
    ]

    output = append_different_windows(
        df_device_lj_scaff, columns_of_interest, month_windows
    )
    output_df = output["dataframe"]
    output_features_columns = output["window_columns"]

    required_output_columns = get_required_output_columns(
        output_features=output_features_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    output_df = output_df.select(required_output_columns)
    return output_df


def retrieve_mode(
    df_windows_df: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Given a dataframe with columns of list, create new columns with the mode of each columns.

    Args:
        df_windows_df: Dataframe with columns (type: list) to compute mode upon.

    Returns:
        Dataframe with columns of mode.
    """
    window_columns = df_windows_df.drop("msisdn", "weekstart").columns

    output = get_df_with_mode_cols(
        df=df_windows_df, rolled_col_names=window_columns, primary_column="msisdn"
    )

    output_df: pyspark.sql.DataFrame = output["dataframe"]

    output_mode_cols = []
    for col in output["mode_columns"]:
        fea_col_name = f"fea_handset_internal_{col}"
        output_df = output_df.withColumnRenamed(col, fea_col_name)
        output_mode_cols.append(fea_col_name)

    required_output_columns = get_required_output_columns(
        output_features=output_mode_cols,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    output_df = output_df.select(required_output_columns)

    return output_df


def calculate_count_distincts(
    df_device_dd: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: List[str],
) -> Dict[str, pyspark.sql.DataFrame]:
    """
    Calculate count of same tac and manufacturer for each weekstart across other MSISDNs.

    Args:
        df_device_dd: Dataframe to compute count upon.

    Returns:
        Dictionary of two elements:
            - tac_count_df: Dataframe with three columns weekstart, tac, fea_handset_int_same_tac_count
            - man_count_df: Dataframe with three columns weekstart, manufacture, fea_handset_int_same_manufacture_count
    """
    df_device_tac_cnt = df_device_dd.groupBy("weekstart", "tac").agg(
        F.countDistinct("msisdn").alias("fea_handset_int_same_tac_count")
    )

    df_device_man_cnt = df_device_dd.groupBy("weekstart", "manufacture").agg(
        F.countDistinct("msisdn").alias("fea_handset_int_same_manufacture_count")
    )

    device_tac_cnt_required_output_columns = get_required_output_columns(
        output_features=df_device_tac_cnt.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["weekstart", "tac"],
    )

    device_man_cnt_required_output_columns = get_required_output_columns(
        output_features=df_device_man_cnt.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["weekstart", "manufacture"],
    )

    df_device_tac_cnt = df_device_tac_cnt.select(device_tac_cnt_required_output_columns)
    df_device_man_cnt = df_device_man_cnt.select(device_man_cnt_required_output_columns)

    return {
        "tac_count_df": df_device_tac_cnt,
        "man_count_df": df_device_man_cnt,
    }


def fea_device_modes(
    df_device_lj_scaff: pyspark.sql.DataFrame,
    df_mode_week: pyspark.sql.DataFrame,
    df_mode_month: pyspark.sql.DataFrame,
    tac_count_df: pyspark.sql.DataFrame,
    man_count_df: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: List[str],
):
    """
    Create feature columns for modes, tac and man counts by joining all dataframes

    Args:
        df_device_lj_scaff: Handset dataframe daily.
        df_mode_week: Dataframe of weekly mode columns.
        df_mode_month: Dataframe of monthly mode columns.
        tac_count_df: Dataframe of TAC counts.
        man_count_df: Dataframe of manufacturer counts.

    Returns:
        Dataframe with appended handset features
    """
    output_df = (
        df_device_lj_scaff.join(df_mode_week, ["msisdn", "weekstart"])
        .join(df_mode_month, ["msisdn", "weekstart"])
        .join(tac_count_df, ["weekstart", "tac"], "left_outer")
        .join(man_count_df, ["weekstart", "manufacture"], "left_outer")
    )

    required_output_columns = get_required_output_columns(
        output_features=output_df.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart", "tac", "manufacture"],
    )

    output_df = output_df.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return output_df.filter(
        F.col("weekstart").between(first_week_start, last_week_start)
    )
