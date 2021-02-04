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

from utils import generate_qc_summaries


def profile_dataset(dfs):
    """
    calculate the quality data summary

    Args:
        dfs: dataframe which need to check the quality summary

    Returns:
        dataframe which contain information abount quality of data
    """
    return generate_qc_summaries(dfs)


def change_column_names_for_prm_tables(dfs):
    """
    change the column name from primary dataframe

    Args:
        dfs: dataframe which need to change column names

    Returns:
        dataframe which change of column name
    """
    df_all = []
    for df in dfs:
        df_cols = ["prm_" + str(col) for col in df.columns]
        df = df.rdd.toDF(df_cols)
        df_all.append(df)
    return df_all


def profile_primary(
    df_chg_pkg_prchse_weekly: pyspark.sql.DataFrame,
    df_acc_bal_weekly: pyspark.sql.DataFrame,
    df_rech_weekly: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    get the profile of primary dataframe from dataframes

    Args:
        df_*: dataframe which need to get the quality profile

    Returns:
        base_df: primary dataframe which have the quality profile
    """
    df_list = [df_chg_pkg_prchse_weekly, df_acc_bal_weekly, df_rech_weekly]
    dfs = change_column_names_for_prm_tables(df_list)
    dfs_profiled = profile_dataset(dfs)
    base_df = dfs_profiled[0]
    for df in dfs_profiled[1:]:
        base_df.union(df)
    return base_df


def profile_master(df_master: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    get the profile of master dataframe

    Args:
        df_master: dataframe which need to get the quality profile

    Returns:
        df_profiled: master dataframe which have the quality profile
    """
    df_profiled = profile_dataset([df_master])
    return df_profiled[0]


def union_all_profiled_datasets(
    df_prm: pyspark.sql.DataFrame, df_master: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    """
    union between profile of master and primary dataframe

    Args:
        df_prm: dataframe primary
        df_master: dataframe master

    Returns:
        union between primary and master dataframe which already have the quality profile
    """
    return df_prm.union(df_master)
