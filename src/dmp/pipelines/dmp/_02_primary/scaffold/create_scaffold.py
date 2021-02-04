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

from datetime import datetime

import pyspark
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import SparkSession

from utils import get_end_date, get_start_date, next_week_start_day


def create_lacci_scaffold(dfj_min_max_for_scaffold, columns_of_interest):
    spark = SparkSession.builder.getOrCreate()
    current_date = datetime.today().date().strftime("%Y-%m-%d")
    start_date_epoch = 1543622400  # Dec 01, 2019  12:00:00 AM UTC
    end_date_epoch = 2556143999  # Dec 31, 2050  11:59:59 PM GMT
    df_daily_dates = (
        create_date_list(
            spark=spark, start=start_date_epoch, end=end_date_epoch, range_=24 * 60 * 60
        )
        .filter(f.col("trx_date") <= current_date)
        .drop("weekstart")
        .coalesce(1)
    )

    df_daily_scaffold = (
        dfj_min_max_for_scaffold.join(
            f.broadcast(df_daily_dates),
            [
                df_daily_dates.trx_date >= dfj_min_max_for_scaffold.min_trx_date,
                df_daily_dates.trx_date <= dfj_min_max_for_scaffold.max_trx_date,
            ],
            how="inner",
        )
        .select(columns_of_interest)
        .distinct()
    )
    return df_daily_scaffold


def create_date_list(
    spark, start=1546300800, end=1577836799, range_=24 * 60 * 60
) -> pyspark.sql.DataFrame:
    """
    Creates list of date in week

    Args:
        spark: define spark engine.
        start: start date in number which start to create.
        end: start date in number which end created.
        range_: range in day which create.

    Returns:
        Dataframe contain date & week start
    """
    list_df = (
        spark.range(start, end, range_)
        .withColumn(
            "trx_date",
            f.date_format(f.col("id").cast(dataType=t.TimestampType()), "yyyy-MM-dd"),
        )
        .withColumn("weekstart", next_week_start_day(f.col("trx_date")))
    )

    return list_df


def _get_min_max_weekstart(
    df_data: pyspark.sql.DataFrame, key: str = "msisdn"
) -> pyspark.sql.DataFrame:
    """
    Creates dataframe for the filtered base on selected msisdn partner

    Args:
        df_data: all data aggregation part.

    Returns:
        Dataframe for the filtered base on selected msisdn partner
    """

    df_msidns = df_data.groupBy(key).agg(
        f.to_date(f.min(f.col("weekstart")), "yyyy-MM-dd").alias("min_weekstart"),
        f.to_date(f.max(f.col("weekstart")), "yyyy-MM-dd").alias("max_weekstart"),
    )

    return df_msidns


def create_scaffold(key: str = "msisdn"):
    def _create_scaffold(*df_data: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        This function will return a dataframe which is the scaffold of all domains combined. They do this through the following steps:
            1. Joins all the dataframes from `*df_data` into a single dataframe using `key` as the join key
            2. For each `key`, identify the  from the dataframe in #1
            3. Imputes all the dataframes from `minimum` to `maximum` for each `key` (Note: each MSISDN will be different)
            4. Returns a dataframe of `key` and `weekstart`

        Args:
            *df_data: all data aggregation part.

        Returns:
            Dataframe with scaffolding which filtered base on selected msisdn partner
        """

        # Get All key, Start & End Trx Date
        df_all = _get_min_max_weekstart(df_data=df_data[0], key=key)
        for i in range(1, len(df_data)):
            df = _get_min_max_weekstart(df_data=df_data[i], key=key)
            df_all = df_all.join(df, ["msisdn"], how="full").select(
                f.col("msisdn"),
                f.least(df_all.min_weekstart, df.min_weekstart).alias("min_weekstart"),
                f.greatest(df_all.max_weekstart, df.max_weekstart).alias(
                    "max_weekstart"
                ),
            )

        spark = SparkSession.builder.getOrCreate()

        # start_date_epoch = 1262304000  # Jan 01, 2010  12:00:00 AM GMT
        start_date_epoch = 1546300800  # Jan 01, 2019 12:00:00 AM GMT  As we decided to keep data only from Jan 01, 2019
        end_date_epoch = 2556143999  # Dec 31, 2050  11:59:59 PM GMT
        df_weekly_dates = create_date_list(
            spark=spark,
            start=start_date_epoch,
            end=end_date_epoch,
            range_=7 * 24 * 60 * 60,
        )

        df_weekly_scaffold = df_all.join(
            f.broadcast(df_weekly_dates),
            [
                df_weekly_dates.weekstart >= df_all.min_weekstart,
                df_weekly_dates.weekstart <= df_all.max_weekstart,
            ],
            how="inner",
        ).select(key, "weekstart")

        return df_weekly_scaffold

    return _create_scaffold


def _get_min_max_month(
    df_data: pyspark.sql.DataFrame, key: str = "msisdn"
) -> pyspark.sql.DataFrame:
    """
    Creates dataframe for the filtered base on selected msisdn partner

    Args:
        df_data: all data aggregation part.

    Returns:
        Dataframe for the filtered base on selected msisdn partner
    """

    df_msidns = (
        df_data.withColumn("suffix", f.lit("01").cast(t.StringType()))
        .withColumn("month_id_col", f.col("mo_id"))
        .groupBy(key)
        .agg(
            f.min(
                f.to_date(
                    f.concat_ws("-", f.col("month_id_col"), f.col("suffix")),
                    "yyyy-MM-dd",
                )
            ).alias("min_month"),
            f.max(
                f.to_date(
                    f.concat_ws("-", f.col("month_id_col"), f.col("suffix")),
                    "yyyy-MM-dd",
                )
            ).alias("max_month"),
        )
    )

    return df_msidns


def create_scaffold_month(key: str = "msisdn"):
    def _create_scaffold_month(
        *df_data: pyspark.sql.DataFrame,
    ) -> pyspark.sql.DataFrame:
        """
        This function will return a dataframe which is the scaffold of all domains combined. They do this through the following steps:
            1. Joins all the dataframes from `*df_data` into a single dataframe using `key` as the join key
            2. For each `key`, identify the  from the dataframe in #1
            3. Imputes all the dataframes from `minimum` to `maximum` for each `key` (Note: each MSISDN will be different)
            4. Returns a dataframe of `key` and `weekstart`

        Args:
            *df_data: all data aggregation part.

        Returns:
            Dataframe with scaffolding which filtered base on selected msisdn partner
        """

        # Get All key, Start & End Trx Date
        df_all = _get_min_max_month(df_data=df_data[0], key=key)
        for i in range(1, len(df_data)):
            df = _get_min_max_month(df_data=df_data[i], key=key)
            df_all = df_all.join(df, ["msisdn"], how="full").select(
                f.col("msisdn"),
                f.least(df_all.min_month, df.min_month).alias("min_month"),
                f.greatest(df_all.max_month, df.max_month).alias("max_month"),
            )

        spark = SparkSession.builder.getOrCreate()

        x = 2019
        y = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
        z = 0

        while x < 2051:
            for month in y:
                dt = str(x) + "-" + month + "-01"
                if z == 0:
                    dt_list = [str(dt)]
                    z = 1
                else:
                    dt_list.append(str(dt))
            x = x + 1

        df_month_dates = spark.createDataFrame(
            dt_list, t.StringType()
        ).withColumnRenamed("value", "monthstart")

        df_monthly_scaffold = (
            df_all.join(
                f.broadcast(df_month_dates),
                [
                    df_month_dates.monthstart >= df_all.min_month,
                    df_month_dates.monthstart <= df_all.max_month,
                ],
                how="inner",
            )
            .withColumn("mo_id", f.date_format(f.col("monthstart"), "yyyy-MM"))
            .select(key, "mo_id")
        )

        return df_monthly_scaffold

    return _create_scaffold_month


def create_msisdn_scaffold(
    df_prepaid_data: pyspark.sql.DataFrame,
    df_postpaid_data: pyspark.sql.DataFrame,
    df_msisdn: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    This function will return a dataframe which is the scaffold of all domains combined. They do this through the following steps:
        1. Joins all the dataframes from `*df_data` into a single dataframe using `key` as the join key
        2. For each `key`, identify the  from the dataframe in #1
        3. Imputes all the dataframes from `minimum` to `maximum` for each `key` (Note: each MSISDN will be different)
        4. Returns a dataframe of `key` and `weekstart`

    Args:
        df_prepaid_data: Prepaid Customers Data.
        df_postpaid_data: Postpaid Customers Data.
        df_msisdn: MSISDNs List

    Returns:
        Dataframe with scaffolding which filtered base on selected msisdn partner
    """

    df_msisdn = df_msisdn.select("msisdn").distinct()

    df_prepaid_data = (
        df_prepaid_data.select("msisdn", "event_date")
        .withColumn(
            "event_date",
            f.to_date(f.col("event_date").cast(t.StringType()), "yyyy-MM-dd"),
        )
        .withColumn("weekstart", next_week_start_day(f.col("event_date")))
        .join(df_msisdn, ["msisdn"], how="inner")
    )

    df_postpaid_data = (
        df_postpaid_data.select("msisdn", "event_date")
        .withColumn(
            "event_date",
            f.to_date(f.col("event_date").cast(t.StringType()), "yyyy-MM-dd"),
        )
        .withColumn("weekstart", next_week_start_day(f.col("event_date")))
        .join(df_msisdn, ["msisdn"], how="inner")
    )

    df_weekly_scaffold = create_scaffold(key="msisdn")(
        df_prepaid_data, df_postpaid_data
    )

    return df_weekly_scaffold


def create_scaffold_3m_4m(
    df_scaffold: pyspark.sql.DataFrame,
) -> [pyspark.sql.DataFrame]:
    """
    Creates 3m & 4m Scaffold Data

    Args:
        df_scaffold: Scaffold Data for 6m period

    Returns:
        df_scaffold_3m: Scaffold Data for 3m period
        df_scaffold_4m: Scaffold Data for 4m period
    """

    start_date_3m = get_start_date(period="3m", partition_column="weekstart")
    start_date_4m = get_start_date(period="4m", partition_column="weekstart")
    end_date = get_end_date(partition_column="weekstart")

    df_scaffold_3m = df_scaffold.filter(
        f.col("weekstart").between(start_date_3m, end_date)
    )
    df_scaffold_4m = df_scaffold.filter(
        f.col("weekstart").between(start_date_4m, end_date)
    )

    return dict(df_scaffold_3m=df_scaffold_3m, df_scaffold_4m=df_scaffold_4m)


def create_msisdn_scaffold_fixed_weekstart(
    first_weekstart_params: str, last_weekstart_params: str
):
    def _create_msisdn_scaffold_fixed_weekstart(
        df_prepaid_data: pyspark.sql.DataFrame,
        df_postpaid_data: pyspark.sql.DataFrame,
        df_msisdn: pyspark.sql.DataFrame,
    ) -> pyspark.sql.DataFrame:
        start_date = get_start_date(first_weekstart_params=first_weekstart_params)
        end_date = get_end_date(last_weekstart_params=last_weekstart_params)
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")

        df_prepaid_data = df_prepaid_data.filter(
            f.col("event_date").between(start_date, end_date)
        )
        df_postpaid_data = df_postpaid_data.filter(
            f.col("event_date").between(start_date, end_date)
        )

        return create_msisdn_scaffold(df_prepaid_data, df_postpaid_data, df_msisdn)

    return _create_msisdn_scaffold_fixed_weekstart


def create_msisdn_monthly_scaffold(
    df_prepaid_data: pyspark.sql.DataFrame,
    df_postpaid_data: pyspark.sql.DataFrame,
    df_msisdn: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    This function will return a dataframe which is the scaffold of all domains combined. They do this through the following steps:
        1. Joins all the dataframes from `*df_data` into a single dataframe using `key` as the join key
        2. For each `key`, identify the  from the dataframe in #1
        3. Imputes all the dataframes from `minimum` to `maximum` for each `key` (Note: each MSISDN will be different)
        4. Returns a dataframe of `key` and `weekstart`

    Args:
        df_prepaid_data: Prepaid Customers Data.
        df_postpaid_data: Postpaid Customers Data.
        df_msisdn: MSISDNs List

    Returns:
        Dataframe with scaffolding which filtered base on selected msisdn partner
    """

    df_msisdn = df_msisdn.select("msisdn").distinct()

    df_prepaid_data = (
        df_prepaid_data.withColumn(
            "mo_id", f.date_format(f.col("event_date"), "yyyy-MM")
        )
        .select("msisdn", "mo_id")
        .distinct()
        .join(df_msisdn, ["msisdn"], how="inner")
    )

    df_postpaid_data = (
        df_postpaid_data.withColumn(
            "mo_id", f.date_format(f.col("event_date"), "yyyy-MM")
        )
        .select("msisdn", "mo_id")
        .distinct()
        .join(df_msisdn, ["msisdn"], how="inner")
    )

    df_monthly_scaffold = create_scaffold_month(key="msisdn")(
        df_prepaid_data, df_postpaid_data
    )

    return df_monthly_scaffold
