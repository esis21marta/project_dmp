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

from pyspark.sql import Window
from pyspark.sql import functions as f
from pyspark.sql.functions import broadcast


def create_total_revenue_table(l1_abt_payu, l1_abt_pkg):
    """
    computing total revenue, from both packages and "pay as you go"
    :param l1_abt_payu:
    :param l1_abt_pkg:
    :return:
    """
    payu_df = (
        l1_abt_payu.withColumn(
            "week_start", f.date_sub(f.next_day(f.to_date("trx_date"), "Mon"), 7)
        )
        .select("msisdn", "rev_total", "week_start")
        .groupBy("msisdn", "week_start")
        .agg(f.sum("rev_total").alias("rev_total"))
    )

    pkg_df = (
        l1_abt_pkg.withColumn(
            "week_start", f.date_sub(f.next_day(f.to_date("trx_date"), "Mon"), 7)
        )
        .select("msisdn", "rev_pkg_prchse", "week_start")
        .groupBy("msisdn", "week_start")
        .agg(f.sum("rev_pkg_prchse").alias("rev_total"))
    )

    union_df = payu_df.union(pkg_df)

    l2_mck_int_payu_pkg_rev = union_df.groupBy("msisdn", "week_start").agg(
        f.sum("rev_total").alias("rev_total")
    )

    return l2_mck_int_payu_pkg_rev


def create_dls_payu_revenue_table(l1_abt_payu):
    """
    creating digital revenue from "pay as you go" mode
    :param l1_abt_payu:
    :return:
    """
    dls_df = (
        l1_abt_payu.withColumn(
            "week_start", f.date_sub(f.next_day(f.to_date("trx_date"), "Mon"), 7)
        )
        .select("msisdn", "rev_dls", "week_start")
        .groupBy("msisdn", "week_start")
        .agg(f.sum("rev_dls").alias("rev_dls"))
    )
    # no effect
    l2_mck_int_dls_rev = dls_df.groupBy("msisdn", "week_start").agg(
        f.sum("rev_dls").alias("rev_dls")
    )

    return l2_mck_int_dls_rev


def creating_grid(l2_mck_int_payu_pkg_rev):
    """
    This creates a grid (cartesian product) of the available weeks and the available msisdn
    :param l2_mck_int_payu_pkg_rev:
    :return:
    """
    months = (
        l2_mck_int_payu_pkg_rev.select("week_start")
        .filter(f.col("week_start") > "2019-03-30")
        .repartition(1000)
        .distinct()
    )
    msisdns = l2_mck_int_payu_pkg_rev.select("msisdn").repartition(1000).distinct()

    int_mck_grid = msisdns.crossJoin(broadcast(months))
    return int_mck_grid


def filling_grid_with_revenue_values(
    l2_mck_int_payu_pkg_rev, l2_mck_int_dls_rev, l2_int_mck_grid
):
    """
    This function takes tables of revenue previously created as well as a grid of msisdn's and trx_date
    with no values. It then fills in the revenue for each value of msisdn and trx_date.

    :param l2_mck_int_payu_pkg_rev:
    :param l2_mck_int_dls_rev:
    :param l2_int_mck_grid:
    :return:
    """
    int_mck_payu_pkg_rev_grid = (
        l2_int_mck_grid.join(
            l2_mck_int_payu_pkg_rev, ["msisdn", "week_start"], how="left"
        )
        .join(l2_mck_int_dls_rev, ["msisdn", "week_start"], how="left")
        .select(["week_start", "msisdn", "rev_total", "rev_dls"])
    )
    return int_mck_payu_pkg_rev_grid


def _window_sum_better_name_will_be_found(
    table, days_start, days_end, col_of_transformation, func, new_col_name
):
    """
    auxiliary function.
    Adds a column to the df.
    For each msisdn:
        - rolling window over over an interval of days
            - for each entry, it takes the interval from days_start to days_end (inclusive).
        - sums or counts the number of occurrences in that window.

    :param table:
    :param days_start:
    :param days_end:
    :param col_of_transformation:
    :param func:
    :param new_col_name:
    :return: Similar to the following query

         sum(rev_total) OVER (
                     PARTITION BY msisdn
                     ORDER BY CAST(date(week_start) AS timestamp)
                     RANGE BETWEEN INTERVAL 21 DAYS PRECEDING AND CURRENT ROW)
                     AS rev_window_pre_21,
         FROM table
    """
    window_values = (
        Window()
        .partitionBy("msisdn")
        .orderBy(f.col("week_start").cast("timestamp").cast("long"))
        .rangeBetween(
            days_start * 86400, days_end * 86400
        )  # Range between past n days.
    )

    func = {"sum": f.sum, "count": f.count}[func]
    s = table.withColumn(new_col_name, func(col_of_transformation).over(window_values))
    return s


# TODO
def creating_window_functions(l2_int_mck_payu_pkg_rev_grid):
    """
    For each msisdn and week, we compute the spending for the upcoming n weeks and previous n weeks

    :return:
    """
    transformations = [
        #  days_start,days_end, col_of_transformation, func, new_col_name
        (-21, 0, "rev_total", "sum", "rev_window_pre_21"),
        (-21, 0, "rev_dls", "sum", "dls_window_pre_21"),
        (-21, 0, "rev_total", "count", "rev_window_pre_21_cnt"),
        (7, 28, "rev_total", "sum", "rev_window_post_8_28"),
        (7, 28, "rev_dls", "sum", "dls_window_post_8_28"),
        (7, 28, "rev_total", "count", "rev_window_post_8_28_cnt"),
        (-77, 0, "rev_total", "sum", "rev_window_pre_77"),
        (-77, 0, "rev_dls", "sum", "dls_window_pre_77"),
        (-77, 0, "rev_total", "count", "rev_window_pre_77_cnt"),
        (7, 84, "rev_total", "sum", "rev_window_post_8_84"),
        (7, 84, "rev_dls", "sum", "dls_window_post_8_84"),
        (7, 84, "rev_total", "count", "rev_window_post_8_84_cnt"),
    ]
    res = l2_int_mck_payu_pkg_rev_grid
    for t_row in transformations:
        res = _window_sum_better_name_will_be_found(res, *t_row)
    res = res.select(["msisdn", "week_start"] + [t[-1] for t in transformations])
    int_mck_payu_pkg_rev_window_c1 = res  # only doing this to keep track of the name
    return int_mck_payu_pkg_rev_window_c1
    # df.createOrReplaceTempView("df")
