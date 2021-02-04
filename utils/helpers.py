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

import logging
import re
from datetime import date, datetime, timedelta
from functools import reduce
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pyspark
import pyspark.sql.functions as f
import pyspark.sql.types as t
import pytz
from kedro.framework.context import load_context
from kedro.io import DataSetError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import array, col, explode, lit, struct
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.window import Window

from .geohash import encode, neighbors
from .get_kedro_context import GetKedroContext

logger = logging.getLogger(__name__)

geo_udf = f.udf(lambda x, y, z: encode(float(x), float(y), z))
neighbors_udf = f.udf(lambda geohash: neighbors(geohash), ArrayType(StringType()))


def get_project_context():
    if GetKedroContext.get_context() is None:
        GetKedroContext(context=load_context(Path.cwd()))
    project_context = GetKedroContext.get_context()
    return project_context


def get_config_parameters(project_context=None, config="parameters"):
    """
    :param project_context: Project Context
    :param config: config file name
    :return: return config file
    """
    if project_context is None:
        project_context = get_project_context()

    return project_context._get_config_loader().get(f"{config}*", f"{config}*/**")


def get_column_name_of_type(df, required_types=None):
    """
    Get all the column names of specific type
    """
    schema = [(x.name, str(x.dataType).split("(")[0]) for x in df.schema.fields]

    data_type = {
        "numeric": [
            "DecimalType",
            "DoubleType",
            "FloatType",
            "IntegerType",
            "LongType",
            "ShortType",
        ],
        "string": ["StringType"],
        "boolean": ["BooleanType"],
        "date": ["DateType", "TimestampType"],
        "array": ["ArrayType"],
    }

    if required_types is None:
        return df.columns

    required_type_pyspark = []

    for required_type in required_types:
        required_type_pyspark = required_type_pyspark + data_type[required_type]

    return [
        column_name
        for column_name, data_type in schema
        if data_type in required_type_pyspark
    ]


def get_feature_columns(columns):
    """
    Get all the feature columns that starts with fea_
    """
    return [column for column in columns if column.startswith("fea_")]


def union_all(*dfs):
    """
    Union all the dataframes
    """
    return reduce(DataFrame.union, dfs)


def union_all_with_name(dfs_list):
    """
    Union all the dataframes with name
    """
    return reduce(DataFrame.unionByName, dfs_list)


def union_different_columns_dfs(dataframes_list):
    all_columns = set()
    for df in dataframes_list:
        for x in df.columns:
            all_columns.add(x)

    dataframes_list_with_all_columns = []
    for df in dataframes_list:
        for column in all_columns:
            if column not in df.columns:
                df = df.withColumn(column, f.lit(None))
        dataframes_list_with_all_columns.append(df)
    return union_all_with_name(dataframes_list_with_all_columns)


def join_all(dfs, on, how="inner"):
    """
    Merge all the dataframes
    """
    return reduce(lambda x, y: x.join(y, on=on, how=how), dfs)


def melt(
    df: DataFrame,
    id_vars: Iterable[str],
    value_vars: Iterable[str],
    var_name: str = "variable",
    value_name: str = "value",
) -> DataFrame:
    """
    class:`DataFrame` from wide to long format.

    Args:
        df: Input dataframe
        id_vars: iterable for value
        var_name: name of value

    Returns:
        selected column which need to doing profiling
    """

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = array(
        *(struct(lit(c).alias(var_name), col(c).alias(value_name)) for c in value_vars)
    )

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)


def add_prefix_suffix_to_df_columns(
    df: pyspark.sql.DataFrame, prefix: str = "", suffix: str = "", columns: list = None
):
    """
    Add prefix and suffix to the column names of dataframe
    Args:
        df (pyspark.sql.DataFrame): Dataframe on which we add suffix on columns
        prefix (str): Prefix
        suffix (str): Suffix
        columns (list): Columns on which we want to add prefix and suffix. Default: None

    Returns:
        Dataframe with columns renamed with suffix added
    """
    # If columns list are not provided all suffix on all the columns of dataframe
    if not columns:
        columns = df.columns

    df = df.select(
        *(
            col(x).alias(prefix + x + suffix) if x in columns else col(x)
            for x in df.columns
        )
    )

    return df


def get_window_to_pick_last_row(granularity_col, key="msisdn", order_by="trx_date"):
    """Returns a rolling window
    Args:
        key: Partition by Key
    Returns:
        w: Rolling window (paritioned by key, weekstart, ordered by trx_date desc)
    """
    window_pick_up_last_row = (
        Window()
        .partitionBy([f.col(key), f.col(granularity_col)])
        .orderBy(f.col(order_by).desc())
    )
    return window_pick_up_last_row


def agg_outlet_mode_first_with_count(
    df: pyspark.sql.DataFrame,
    col_count: str,
    rolled_col_names: List[str],
    primary_columns: List[str],
    granularity_col: str,
) -> Dict[str, Any]:
    """
    Given a dataframe and a list of columns (with list datatype), return the dataframe with the mode of each columns.
    :param df:
    :param rolled_col_names: list of columns (with list datatype)
    :return: dataframe with mode of each rolled_col_names (ending with _mode)

    Mode calculation ignores null or empty strings
    """

    mode_columns = []
    output_df = df.select(primary_columns).distinct()

    for rolled_col_name in rolled_col_names:
        output_col_name = rolled_col_name + "_mode"
        output_col_count = rolled_col_name + "_count"
        mode_df = df.select(primary_columns + [rolled_col_name] + [f.col(col_count)])

        mode_df = (
            mode_df.groupBy(*primary_columns, f.col(rolled_col_name))
            .agg(f.sum(col_count).alias("count"))
            .filter(f.col(rolled_col_name).isNotNull())
            .withColumn(
                "order",
                f.row_number().over(
                    get_window_to_pick_last_row(
                        granularity_col, key=primary_columns[0], order_by="count"
                    )
                ),
            )
            .withColumn(output_col_count, f.col("count"))
            .where(f.col("order") == 1)
            .select(
                primary_columns
                + [f.col(rolled_col_name).alias(output_col_name)]
                + [f.col(output_col_count)]
            )
        )

        mode_columns.append(output_col_name)
        mode_columns.append(output_col_count)
        output_df = output_df.join(mode_df, primary_columns, "left")

    return {"dataframe": output_df, "mode_columns": mode_columns}


def calc_distance_bw_2_lat_long(df):
    df = (
        df.withColumn(
            "a",
            (
                f.pow(f.sin(f.radians(f.col("lat2") - f.col("lat1")) / 2), 2)
                + f.cos(f.radians(f.col("lat1")))
                * f.cos(f.radians(f.col("lat2")))
                * f.pow(f.sin(f.radians(f.col("long2") - f.col("long1")) / 2), 2)
            ),
        )
        .withColumn(
            "distance", f.atan2(f.sqrt(f.col("a")), f.sqrt(-f.col("a") + 1)) * 12742000
        )
        .drop("a")
    )
    return df


def get_all_months_bw_sd_ed(start_date="20190101", end_date="20220101"):
    start_date_dt = datetime.strptime(start_date, "%Y%m%d")
    end_date_dt = datetime.strptime(end_date, "%Y%m%d")
    month_list = []

    while start_date_dt <= end_date_dt:
        month = (datetime.strftime(start_date_dt, "%Y-%m-01"),)
        month_list.append(month)
        start_date_dt += timedelta(days=31)

    spark = SparkSession.builder.getOrCreate()

    months_schema = StructType([StructField("month", StringType(), False)])
    df_months = spark.createDataFrame(month_list, schema=months_schema)
    return df_months


def get_month_id_bw_sd_ed(start_date, end_date, sla_date_parameter):
    data = [["2000-01", "2000-01-01"]]

    while start_date < end_date:
        if start_date.day < sla_date_parameter:
            if start_date.month < 3:
                new_month = start_date.month + 10
                new_year = start_date.year - 1
                new_dt = start_date.replace(
                    month=new_month, year=new_year, day=1
                ).strftime("%Y-%m")
                data.append([str(new_dt), str(start_date)])
                start_date += timedelta(days=7)
            else:
                new_month = start_date.month - 2
                new_dt = start_date.replace(month=new_month, day=1).strftime("%Y-%m")
                data.append([str(new_dt), str(start_date)])
                start_date += timedelta(days=7)
        else:
            if start_date.month == 1:
                new_month = 12
                new_year = start_date.year - 1
                new_dt = start_date.replace(
                    month=new_month, year=new_year, day=1
                ).strftime("%Y-%m")
                data.append([str(new_dt), str(start_date)])
                start_date += timedelta(days=7)
            else:
                new_month = start_date.month - 1
                new_dt = start_date.replace(month=new_month, day=1).strftime("%Y-%m")
                data.append([str(new_dt), str(start_date)])
                start_date += timedelta(days=7)

    spark = SparkSession.builder.getOrCreate()

    schema = StructType(
        [
            StructField("mo_id", StringType(), False),
            StructField("weekstart", StringType(), False),
        ]
    )

    df_weekstart = spark.createDataFrame(data, schema=schema)

    return df_weekstart


def convert_cols_to_lc_and_rem_space(df):
    for col in df.columns:
        df = df.withColumnRenamed(col, col.lower().replace(" ", "_"))
    return df


def filter_incorrect_lat_long(df):
    df = df.filter(
        f.col("latitude").cast("float").isNotNull()
        & f.col("longitude").cast("float").isNotNull()
        & (f.col("latitude") >= -90)
        & (f.col("latitude") < 90)
    )
    return df


def get_neighbours(df: pyspark.sql.DataFrame):
    df = filter_incorrect_lat_long(df)
    columns_of_interest = df.columns + ["neighbours_all"]
    df_with_neighbors = (
        df.withColumn(
            "geohash", geo_udf(f.col("latitude"), f.col("longitude"), f.lit(5))
        )
        .withColumn("neighbours", neighbors_udf(f.col("geohash")))
        .withColumn(
            "neighbours_all", f.array_union(f.array("geohash"), f.col("neighbours"))
        )
        .select(columns_of_interest)
    )
    return df_with_neighbors


def add_prefix_to_fea_columns(df_feature, ignore_columns, fea_prefix):
    fea_columns = [col for col in df_feature.columns if col not in ignore_columns]
    for col in fea_columns:
        df_feature = df_feature.withColumnRenamed(col, fea_prefix + col)
    return df_feature


def is_it_sunday(_date, dt_format):
    """
    Checks if the provided date is a Sunday.

    :param _date: date in given format.
    :param dt_format: date time format to cast the date string by
    :return: returns True if Monday. False otherwise
    """
    day = datetime.strptime(str(_date), dt_format)
    if day.weekday() != 6:  # Check if date is not Sunday
        return False
    return True


def is_it_monday(date, dt_format):
    """
    Checks if the provided date is a Monday.

    :param date: date in given format.
    :param dt_format: date time format to cast the date string by
    :return: returns True if Monday. False oterwise
    """
    day = datetime.strptime(str(date), dt_format)
    if day.weekday() != 0:  # Check if weekstart is not Monday
        return False
    return True


def get_period_in_days(period_string, is_month_mapped_to_date=None):
    """
    Returns the number of days (number of rows over which window function is applied) given a period string

    :param period_string: Can be of [1w, 2w, 3w, 1m, 2m, 3m, 4m, 5m, 6m]
    :param is_month_mapped_to_date: flag indicating if month is mapped to date, as used for monthly tables
    :return: Returns number of days as defined given a valid period string
    """
    if is_month_mapped_to_date:
        periods = {"1m": 1, "2m": 2, "3m": 3}
    else:
        periods = {
            "1w": 1 * 7,
            "2w": 2 * 7,
            "3w": 3 * 7,
            "1m": 4 * 7,
            "2m": 8 * 7,
            "3m": 13 * 7,
            "4m": 17 * 7,
            "5m": 22 * 7,
            "6m": 26 * 7,
            "1cm": 1 * 30,
            "2cm": 2 * 30,
            "3cm": 3 * 30,
            "4cm": 4 * 30,
            "5cm": 5 * 30,
            "6cm": 6 * 30,
        }

    period = periods.get(period_string)
    if not period:
        raise DataSetError(
            f"Invalid partition_filter_period provided: {period_string}.\n"
            f"Valid values for partition_filter_period are {list(periods.keys())}"
        )

    return period


def generate_timestamp(timezone="Asia/Jakarta") -> str:
    """Generate the timestamp

    Returns:
        String representation of the current timestamp.
    """
    return datetime.now(tz=pytz.timezone(timezone)).strftime("%Y-%m-%dT%H.%M.%S.%f")


def get_weekstart(context=None, last_weekstart_params="last_weekstart"):
    """
        If it's before or Wednesday, return last Monday.
        After Wednesday return this weeks Monday.
    :param context: kedro context
    :param last_weekstart_params: last weekstart parameter name
    :return: weekstart
    """
    if context is None:
        context = get_project_context()

    weekstart = context.params.get(last_weekstart_params, None)

    if str == type(weekstart):
        weekstart = datetime.strptime(weekstart, "%Y-%m-%d").date()

    if weekstart:
        if not is_it_monday(weekstart, "%Y-%m-%d"):
            raise IOError(
                "Pipeline {last_weekstart_params} in parameters.yml should be Monday.\n"
                "{last_weekstart_params}: {weekstart}".format(
                    last_weekstart_params=last_weekstart_params, weekstart=weekstart,
                )
            )
    else:
        weekstart = date.today() - timedelta(days=2)
        weekstart -= timedelta(days=weekstart.weekday())

    return weekstart


def get_last_month(context=None):
    """
        If it's on or before 5th of month, return second last month.
        After 5th of month return this last month.
    :param context: kedro context
    :return: month
    """
    if context is None:
        context = GetKedroContext.get_context()

    last_month: date = context.params.get("last_month", None)

    if str == type(last_month):
        last_month = datetime.strptime(last_month, "%Y-%m-%d").date()

    if not last_month:
        last_month = (date.today() - timedelta(days=5)).replace(day=1) - timedelta(
            days=1
        )

    month = last_month.month
    year = last_month.year
    if month == 12:
        month = 1
        year += 1
    else:
        month = month + 1

    last_month = last_month.replace(year=year, month=month, day=1) - timedelta(days=1)

    return last_month


def get_start_date(
    period="1w",
    context=None,
    partition_column="trx_date",
    first_weekstart_params="first_weekstart",
):
    """
        Returns Start Date of the Range
    :param period: Filter Period
    :param context: kedro context
    :param partition_column: Partition Column
    :param first_weekstart_params: first weekstart parameter name
    :return: Start Date
    """
    if context is None:
        context = get_project_context()

    if re.search("^[1-9][0-9]*cm$", period):
        first_month: date = context.params.get("first_month", None)

        if str == type(first_month):
            first_month = datetime.strptime(first_month, "%Y-%m-%d").date()

        if not first_month:
            first_month = get_last_month(context=context) - timedelta(
                days=(get_period_in_days(period) - 3)
            )

        start_date = first_month.replace(day=1)

    else:
        first_weekstart = context.params.get(first_weekstart_params, None)

        if str == type(first_weekstart):
            first_weekstart = datetime.strptime(first_weekstart, "%Y-%m-%d").date()

        if first_weekstart:
            if not is_it_monday(first_weekstart, "%Y-%m-%d"):
                raise IOError(
                    "Pipeline {first_weekstart_params} in parameters.yml should be Monday.\n"
                    "{first_weekstart_params}: {weekstart}".format(
                        first_weekstart_params=first_weekstart_params,
                        weekstart=first_weekstart,
                    )
                )
        else:
            first_weekstart = get_weekstart(context=context)

        if "weekstart" == partition_column:
            start_date = first_weekstart - timedelta(
                days=(get_period_in_days(period) - 6)
            )
        elif "yearweek_partition" == partition_column:
            start_date = first_weekstart
        else:
            start_date = first_weekstart - timedelta(days=get_period_in_days(period))

    return start_date


def get_end_date(
    context=None,
    partition_column="trx_date",
    last_weekstart_params="last_weekstart",
    period="1w",
):
    """
        Returns End Date of the Range
    :param context: kedro context
    :param partition_column: Partition Column
    :param last_weekstart_params: last weekstart parameter name
    :return: End Date
    """
    if context is None:
        context = get_project_context()

    if re.search("^[1-9][0-9]*cm$", period):
        end_date = get_last_month(context=context)
    else:
        last_weekstart = get_weekstart(
            context=context, last_weekstart_params=last_weekstart_params
        )

        if ("weekstart" == partition_column) | (
            "yearweek_partition" == partition_column
        ):
            end_date = last_weekstart
        else:
            end_date = last_weekstart - timedelta(days=1)

    return end_date


def full_join_all_dfs(dfs, key):
    counter = 1
    for df in dfs:
        if counter == 1:
            df_fea_merged_all = df
        else:
            df_fea_merged_all = df_fea_merged_all.join(df, [key], how="full")
        counter += 1
    return df_fea_merged_all


def rename_columns(df: pyspark.sql.DataFrame, old_new_column_names_dict: Dict):
    for old_col_name, new_col_name in old_new_column_names_dict.items():
        df = df.withColumnRenamed(old_col_name, new_col_name)
    return df


def union_all(*dfs):
    return reduce(DataFrame.unionAll, dfs)


def clean_lacci_from_mkios(df_mkios: pyspark.sql.DataFrame):
    df_mkios_cleaned = (
        df_mkios.withColumn(
            "splitted1", f.split("subs_lacci_id", "\\|").cast("array<string>")
        )
        .withColumn(
            "subs_lacci_id",
            f.concat(f.col("splitted1")[0], f.lit("|"), f.col("splitted1")[1]),
        )
        .withColumn("splitted2", f.split("rs_lacci_id", "\\|").cast("array<string>"))
        .withColumn(
            "rs_lacci_id",
            f.concat(f.col("splitted2")[0], f.lit("|"), f.col("splitted2")[1]),
        )
        .drop("splitted1", "splitted2")
    )
    return df_mkios_cleaned


def clean_lacci_from_urp(df_urp: pyspark.sql.DataFrame):
    df_urp_cleaned = (
        df_urp.withColumn(
            "splitted", f.split("subs_lacci_id", "\\|").cast("array<string>")
        )
        .withColumn(
            "subs_lacci_id",
            f.concat(f.col("splitted")[0], f.lit("|"), f.col("splitted")[1]),
        )
        .drop("splitted")
    )
    return df_urp_cleaned


def fillna_with_null(df: pyspark.sql.DataFrame, subset=None):
    if subset is None:
        subset = df.columns
    nan_supported_data_types = ["FloatType", "DoubleType", "DecimalType"]
    nan_supported_columns = [
        x.name
        for x in df.schema.fields
        if str(x.dataType).split("(")[0] in nan_supported_data_types
        and x.name in subset
    ]
    df = df.replace(float("nan"), None, subset=nan_supported_columns)
    return df


def calculate_weeks_since_last_activity(
    df, activity_col, lookback_weeks, primary_cols, feature_col
):
    """
    Updating file path to added created time stamp to version the generated data set
    :param df: input dataframe
    :param activity_col: activity column name
    :param lookback_weeks: lookback period in weeks
    :param primary_cols: primary keys
    :param feature_col: desired feature name
    :return: dataframe with features denoting weeks since last activity
    """
    lookback_window = (
        Window.partitionBy("msisdn")
        .orderBy("weekstart")
        .rowsBetween(-lookback_weeks, 0)
    )
    lag_window = Window.partitionBy("msisdn").orderBy("weekstart")
    df = (
        df.withColumn(
            "activity_date",
            f.when(
                (f.col(activity_col) != 0) & (f.col(activity_col).isNotNull()),
                f.col("weekstart"),
            ),
        )
        .withColumn(
            "last_activity_date",
            f.last(f.col("activity_date"), ignorenulls=True).over(lookback_window),
        )
        .withColumn(
            "prev_activity_date", f.lag(f.col("last_activity_date")).over(lag_window)
        )
        .withColumn(
            feature_col,
            f.when(f.col("weekstart") == f.col("activity_date"), 0)
            .when(
                (f.col("prev_activity_date").isNull())
                & (f.col("last_activity_date").isNull()),
                99999,
            )
            .otherwise(f.datediff(f.col("weekstart"), f.col("prev_activity_date")) / 7)
            .cast(t.LongType())
            .alias(feature_col),
        )
    )
    required_cols = primary_cols + [feature_col]
    return df.select(*required_cols)


def count_unique_list_month(
    df: pyspark.sql.DataFrame, array_col: str, alias: str
) -> pyspark.sql.DataFrame:

    df = df.withColumn("monthstart", f.col("weekstart").substr(1, 7))

    df_unique_list = df.groupBy("msisdn", "monthstart").agg(
        f.size(f.array_distinct(f.flatten(f.collect_set(f.col(array_col))))).alias(
            alias
        )
    )

    return df_unique_list


def remove_prefix_from_period_string(period_string: str) -> str:
    try:
        period_sting_formatted = re.findall("([1-9][0-9]*[A-Za-z]+)", period_string)[0]
    except:
        raise Exception(f"Invalid period string: {period_string}")
    return period_sting_formatted
