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
from pyspark.sql.functions import array_contains, col, concat, lit, substring_index


def select_specific_partner_from_master_table(
    master_table: pyspark.sql.DataFrame,
    weekstart: str,
    use_partner: pyspark.sql.DataFrame = False,
    partner: str = "internal-random-sample",
) -> pyspark.sql.DataFrame:
    """
    Two master tables were used in different moments of this exercise.
    At one point, we used the training master table
        Select the "partner" that interests the 4g usecase, which is, by default "internal-random-sample".
    In a second moment, we used the scoring master table designed for the external workstream.
        This master table is provided for a single week, but scores the whole subscriber base.
        Because there is no "weekstart" column, we have to add it

    :param partner:
    :param weekstart:
    :param use_partner: Tag on whether to user the partner column to select msisdns or not
    :param master_table: Master table provided by DMP
    :return: select a partner OR adds a weekstart column
    """
    columns_to_drop = [
        "fea_network_cssr_ps_avg_3m",
        "fea_network_broadband_revenue_avg_3w",
        "fea_network_capable_4g_avg_3m",
        "fea_network_cssr_cs_avg_3m",
        "fea_network_total_throughput_kbps_avg_1w",
        "fea_network_broadband_revenue_avg_3m",
    ]
    master_table = master_table.drop(*columns_to_drop)
    master_table.drop(*columns_to_drop)
    master_table = master_table.withColumn(
        "fea_rech_annualize_factor", f.lit(math.sqrt(52.0))
    )

    if use_partner:
        return master_table.filter(array_contains(master_table.partner, partner))
    else:
        return master_table.withColumn("weekstart", lit(weekstart))


def add_month_to_master_table(
    master_table: pyspark.sql.DataFrame, time_unit_of_merging: str = "month"
) -> pyspark.sql.DataFrame:
    """
    The master table is on weekly level, while some joins have to be done on monthly level
    #TODO should we select the last week of the month only or all the weeks?

    ------
    # This function exists to match the time unit in `lte_map` (which maps msisdn and week to segments)

    :param time_unit_of_merging: month or week
    :param master_table: master table from DMP
    :return:
    """

    assert time_unit_of_merging in ("month", "week")

    if time_unit_of_merging == "month":
        s = substring_index(master_table.weekstart, "-", 2)
        column = concat(substring_index(s, "-", 1), substring_index(s, "-", -1))
    else:
        # TODO
        raise NotImplementedError()

    res = master_table.withColumn(time_unit_of_merging, column)
    return res


def select_desired_weekstart(
    weekstart: str, *tables: pyspark.sql.DataFrame
) -> List[pyspark.sql.DataFrame]:
    """
    The analysis are made in a monthly basis. The pipeline will be run each month
    :param weekstart: given week
    :param tables: tables from which select a certain week
    :return:
    """
    res = []
    for table in tables:
        res.append(table.filter(col("weekstart") == weekstart))
    return res


def subselect_desired_columns(
    *tables_and_columns: pyspark.sql.DataFrame,
) -> List[pyspark.sql.DataFrame]:
    """
    We pass a list of tables and lists of columns and we return the initial tables subset to the columns given
    The input is provided as follows. we provide the pyspark dataframe, then the colums we wish to select, and so on

    Examples of input :
    subselect_desired_columns(df_1, columns_to_select_1,df_2,cols_to_select_2,...)

    subselect_desired_columns(master_table, [msisdn,weekstart,los,data_total_kb], lte_map, [msisdn,segment], ...)
    :param tables_and_columns: tables and columns, as the example above
    :return:
    """
    assert len(tables_and_columns) % 2 == 0

    tables = [el for i, el in enumerate(tables_and_columns) if i % 2 == 0]
    list_of_column_sets = [el for i, el in enumerate(tables_and_columns) if i % 2 == 1]

    res = []
    for table, list_of_column in zip(tables, list_of_column_sets):
        res.append(table.select(list_of_column))
    return res


def join_wrapper(
    table_a: pyspark.sql.DataFrame,
    table_b: pyspark.sql.DataFrame,
    cols=None,
    how: str = "inner",
) -> pyspark.sql.DataFrame:
    """
    This is a wrapper for the pyspark join function
    :param how: how argumnent of pyspark join
    :param table_a: fist table
    :param table_b: second table
    :param cols: "on" argument in pyspark
    :return:
    """
    cols = ["msisdn"] if cols is None else cols
    table_b = table_b.dropDuplicates(cols)
    res = table_a.join(table_b, cols, how=how)
    return res


def concatenate_lac_ci(cb_table: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    res = cb_table.withColumn("lac_ci", concat(col("lac"), lit("-"), col("ci")))
    return res


def downsampling_master_table(
    master_table_net_segments_: pyspark.sql.DataFrame, fraction: int = 1
) -> pyspark.sql.DataFrame:
    return master_table_net_segments_.sample(False, fraction, 42)
