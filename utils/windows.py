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

import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql.window import Window


def next_week_start_day(date):
    """
    Returns the next monday for the current day

    Args:
        date: Current date

    Returns:
        next_day: Next monday for the current day
    """
    next_day = f.next_day(date, "monday")
    return next_day


def previous_week_start_day(date):
    """
    Returns a rolling window

    Args:
        date: Current date

    Returns:
        previous_day: Previous monday for current day
    """
    previous_day = f.date_sub(f.next_day(date, "monday"), 7)
    return previous_day


def get_rolling_window(num_days, key="msisdn", oby="trx_date", optional_keys=None):
    """Returns a rolling window paritioned by 'key', ordered by 'oby'
    Args:
        num_days: Number of days for the rolling window (if negative, will run for full history)
        key: Partition by Key
        oby: Order by column
        optional_keys: Extra Partition Keys

    Returns:
        w: Rolling window of num_days, partitioned by key, ordered by oby
    """
    if optional_keys is None:
        optional_keys = []
    keys = [f.col(key)] + [f.col(i) for i in optional_keys]
    days = lambda i: (i - 1) * 86400
    if num_days == -1:
        w = (
            Window()
            .partitionBy(*keys)
            .orderBy(f.col(oby).cast("timestamp").cast("long"))
            .rangeBetween(Window.unboundedPreceding, 0)
        )
    else:
        w = (
            Window()
            .partitionBy(*keys)
            .orderBy(f.col(oby).cast("timestamp").cast("long"))
            .rangeBetween(-days(num_days), 0)
        )

    return w


def get_rolling_window_fixed(start_days, end_days, key="msisdn", oby="trx_date"):
    """
    Returns a rolling window paritioned by 'key', ordered by 'oby' between 'start_day' and 'end_day'

    Args:
        start_days: Start number of days for the rolling window
        end_days: End number of days for the rolling window
        key: Partition by Key
        oby: Order by column

    Returns:
        w: Rolling window of num_days, partitioned by key, ordered by oby
    """

    if start_days == 0:
        start_days = 0
    else:
        start_days = (start_days - 1) * 86400

    if end_days == 0:
        end_days = 0
    else:
        end_days = (end_days - 1) * 86400

    w = (
        Window()
        .partitionBy(f.col(key))
        .orderBy(f.col(oby).cast("timestamp").cast("long"))
        .rangeBetween(-start_days, -end_days)
    )

    return w


def get_weekly_rolling_window(num_weeks, key="msisdn", oby="weekstart"):
    """
    Returns a weekly rolling window paritioned by 'key', ordered by 'oby'

    Args:
        num_weeks: Number of weeks for the rolling window
        key: Partition by Key
        oby: Order by column

    Returns:
        w: Rolling window of num_days, partitioned by key, ordered by oby
    """

    w = Window().partitionBy(f.col(key)).orderBy(oby).rowsBetween(num_weeks - 1, 0)
    return w


def get_window_to_pick_last_row(key="msisdn", order_by="trx_date"):
    """Returns a rolling window

    Args:
        key: Partition by Key
        order_by: Column to order partition by

    Returns:
        w: Rolling window (paritioned by key, weekstart, ordered by trx_date desc)
    """

    window_pick_up_last_row = (
        Window()
        .partitionBy([f.col(key), f.col("weekstart")])
        .orderBy(f.col(order_by).desc())
    )
    return window_pick_up_last_row


def get_window_pby_msisdn_oby_trx_date(
    partition_column="msisdn", order_by_column="trx_date"
):
    """
    Returns a rolling window partitioned by partition_column, ordered by order_by_column

    Args:
        partition_column: Partition by Key
        order_by_column: Order by column

    Returns:
        w: Rolling window parititioned by partition_column ordered by order_by_column
    """
    window_sort_by_trx_date = (
        Window().partitionBy(partition_column).orderBy(f.col(order_by_column))
    )
    return window_sort_by_trx_date


def create_date_list_weekly(start_date: str, end_date: str) -> list:
    """
    Function to return a list of all Monday dates between start_date & end_date

    Args:
        start_date: Beginning of the date range for which all Mondays are required
        end_date: Ending of the date range for which all Mondays are required

    Returns:
        List of all Monday's in the given date range
    """
    return [
        d.strftime("%Y-%m-%d")
        for d in pd.date_range(start_date, end_date, freq="W-MON")
    ]


def create_date_list_daily(start_date: str, end_date: str) -> list:
    """
    Function to return a list of all dates between start_date & end_date

    Args:
        start_date: Beginning of the date range for which all dates are required
        end_date: Ending of the date range for which all dates are required

    Returns:
        List of all dates in the given date range
    """
    return [d.strftime("%Y-%m-%d") for d in pd.date_range(start_date, end_date)]


def get_fixed_window_rows_between(
    start, end, key="outlet_id", oby="month", optional_keys=None, asc=True
):
    if optional_keys is None:
        optional_keys = []

    keys = [f.col(key)] + [f.col(i) for i in optional_keys]
    if asc:
        fixed_window = (
            Window.partitionBy(*keys)
            .orderBy(f.col(oby).cast("timestamp").cast("long"))
            .rowsBetween(start, end)
        )
    else:
        fixed_window = (
            Window.partitionBy(*keys)
            .orderBy(f.col(oby).cast("timestamp").cast("long").desc())
            .rowsBetween(start, end)
        )
    return fixed_window


def get_custom_window(partition_by_cols: list, order_by_col: str, asc: bool = True):
    """
    Get custom window partitionBy partition_by_cols and orderBy order_by_col
    :param partition_by_cols: list of partitionBy column
    :param order_by_col: list of orderBy columns
    :param asc: ascending/descending for orderBy column
    :return custom_window: window partitioned by partition_by_cols and order by order_by_col
    """
    if asc:
        custom_window = Window.partitionBy(*partition_by_cols).orderBy(order_by_col)
    else:
        custom_window = Window.partitionBy(*partition_by_cols).orderBy(
            f.desc(order_by_col)
        )
    return custom_window
