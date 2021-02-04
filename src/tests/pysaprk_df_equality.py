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

import pyspark


def assert_df_frame_equal(
    left_df: pyspark.sql.DataFrame,
    right_df: pyspark.sql.DataFrame,
    check_column_names: bool = False,
    check_dtype: bool = True,
    check_columns_in_order: bool = False,
    order_by: list = None,
) -> None:
    """
    Used to test if two dataframes are same or not

    Args:
        left_df (pyspark.sql.DataFrame): Left Dataframe
        right_df (pyspark.sql.DataFrame): Right Dataframe
        check_column_names (bool, optional): Comapare both dataframes have same column or not. Defaults to False.
        check_dtype (bool, optional): Comapred both dataframe have same column and colum type or not. If using check_dtype then check_column_names is not required. Defaults to True.
        check_columns_in_order (bool, optional): Check columns in order. Defaults to False.
    """
    # Check Column Names
    if check_column_names:
        if check_columns_in_order:
            assert left_df.columns == right_df.columns
        else:
            assert sorted(left_df.columns) == sorted(right_df.columns)

    # Check Column Data Types
    if check_dtype:
        if check_columns_in_order:
            assert left_df.dtypes == right_df.dtypes, "data types don't match"
        else:
            assert sorted(left_df.dtypes, key=lambda x: x[0]) == sorted(
                right_df.dtypes, key=lambda x: x[0]
            ), "data types don't match"

    # Compare Rows
    if check_columns_in_order:
        left_df_columns = left_df.columns
        right_df_columns = right_df.columns
    else:
        left_df_columns = sorted(left_df.columns)
        right_df_columns = sorted(right_df.columns)

    if order_by:
        left_df = left_df.orderBy(order_by)
        right_df = right_df.orderBy(order_by)

    left_df_list = []
    for row in left_df.select(left_df_columns).collect():
        left_df_list.append([cell for cell in row])

    right_df_list = []
    for row in right_df.select(right_df_columns).collect():
        right_df_list.append([cell for cell in row])

    for row_index in range(len(left_df_list)):
        for col_index, column_name in enumerate(left_df_columns):
            left_cell = left_df_list[row_index][col_index]
            right_cell = right_df_list[row_index][col_index]
            if left_cell == right_cell:
                assert True
            elif left_cell is None and right_cell is None:
                assert True
            elif math.isnan(left_cell) and math.isnan(right_cell):
                assert True
            else:
                assert (
                    False
                ), f"\nRow = {row_index + 1} : Column = {column_name}\n\nACTUAL: {left_cell} \nEXPECTED: {right_cell}"
