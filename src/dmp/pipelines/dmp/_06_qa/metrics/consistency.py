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

import pyspark
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType

from src.dmp.pipelines.dmp._06_qa.metrics.metrics_helper import melt_qa_result
from utils import add_prefix_suffix_to_df_columns

_is_same = lambda x: (
    (f.isnull(f.col(f"{x}_old")) & f.isnull(f.col(f"{x}_new")))
    | (f.col(f"{x}_old") == f.col(f"{x}_new"))
).cast(IntegerType())


def get_consistency(
    df_new: pyspark.sql.DataFrame, df_old: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    """
    Count number of matching values
    Args:
        df_new (pyspark.sql.DataFrame): Old dataframe
        df_old (pyspark.sql.DataFrame): New dataframe

    Returns:
        df (pyspark.sql.DataFrame)
    """

    # Get same columns in old and new
    df_new_cols = df_new.columns
    df_old_cols = df_old.columns
    columns_same_in_old_to_new = set(df_new_cols).intersection(set(df_old_cols))

    # If none of the columns are same throw error
    if len(columns_same_in_old_to_new) == 0:
        raise ValueError("No matching columns between old and new master table")

    # If msisdn and weekstart is missing raise error
    if not all(
        var in list(columns_same_in_old_to_new) for var in ["msisdn", "weekstart"]
    ):
        raise ValueError("MSISDN or weekstart is missing!")

    # Select common columns
    common_columns = list(columns_same_in_old_to_new)
    df_new = df_new.select(common_columns)
    df_old = df_old.select(common_columns)

    common_columns_without_weekstart_and_msisdn = list(
        columns_same_in_old_to_new - set(("weekstart", "msisdn"))
    )

    # Add old and new suffix to columns and join for same msisdn and weekstart
    df_new = add_prefix_suffix_to_df_columns(
        df_new, suffix="_new", columns=common_columns_without_weekstart_and_msisdn
    )
    df_old = add_prefix_suffix_to_df_columns(
        df_old, suffix="_old", columns=common_columns_without_weekstart_and_msisdn
    )
    df_old_new_merged = df_new.join(df_old, on=["weekstart", "msisdn"], how="inner")

    # Check if old and new value are same or null
    df_old_new_merged = df_old_new_merged.select(
        *df_old_new_merged.columns,
        *[
            _is_same(col).alias(f"{col}_is_eq")
            for col in common_columns_without_weekstart_and_msisdn
        ],
    )

    # Select only weekstart, msisdn and _is_eq columns
    df_old_new_merged = df_old_new_merged.select(
        ["weekstart", "msisdn"]
        + [
            f.col(col).alias(col.replace("_is_eq", ""))
            for col in df_old_new_merged.columns
            if col.endswith("_is_eq")
        ]
    )

    df_old_new_merged = df_old_new_merged.na.fill(0)

    # Count the number or same records for each column
    df_same_percent = df_old_new_merged.groupby("weekstart").agg(
        *[
            f.mean(col).alias(f"{col}__same_percent")
            for col in common_columns_without_weekstart_and_msisdn
        ]
    )

    return melt_qa_result(df_same_percent)
