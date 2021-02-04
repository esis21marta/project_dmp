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

# in-built libraries
import datetime
from functools import reduce
from operator import add
from typing import Any, Dict, Tuple

# third-party libraries
import pandas
import pyspark
import pyspark.sql.functions as f
from dateutil.relativedelta import relativedelta
from pyspark.sql.window import Window

# internal code
from ..utils import get_filter_month


def filtered_outlets_helper(
    prefilter_df: pyspark.sql.DataFrame,
    postfilter_df: pyspark.sql.DataFrame,
    master_table_params: Dict[str, Any],
    filter_month: datetime.date,
) -> pyspark.sql.DataFrame:
    filtered_outlets = ""
    if master_table_params["save_filtered_outlets"]:
        pre_df = prefilter_df.select("outlet_id", "month")
        post_df = postfilter_df.select("outlet_id", "month")
        filtered_outlets = (
            pre_df.filter(f.col("month") == filter_month)
            .join(
                post_df.filter(f.col("month") == filter_month),
                on="outlet_id",
                how="left_anti",
            )
            .select("outlet_id")
            .distinct()
        )

    return filtered_outlets


def prepare_non_tsf_outlet_ids(
    ra_mck_int_master_dynamic_non_tsf: pyspark.sql.DataFrame,
    master_table_params: Dict[str, Any],
) -> pyspark.sql.DataFrame:
    """Obtain distinct outlet_ids in filter month and cache DataFrame

    Args:
        ra_mck_int_master_dynamic_non_tsf: Reseller master table (revenue) without time series filling
        master_table_params: Master table parameters from parameters.yml which contains month to filter by

    Returns:
        Outlet ids from filter month in non-time series filled revenue master table
    """

    filter_month = get_filter_month(master_table_params["filter_month"])

    dynamic_non_tsf_outlet_ids = (
        ra_mck_int_master_dynamic_non_tsf.withColumn(
            "month",
            f.trunc(
                f.to_date(
                    master_table_params["agg_date"],
                    format=master_table_params["to_date_format"],
                ),
                "month",
            ),
        )
        .filter(f.col(master_table_params["agg_primary_columns"][1]) == filter_month)
        .select(master_table_params["agg_primary_columns"][0])
        .distinct()
    )

    dynamic_non_tsf_outlet_ids.cache()

    return dynamic_non_tsf_outlet_ids


def prepare_outlets_static_features(
    ra_master_static: pyspark.sql.DataFrame, master_table_params: Dict[str, Any]
) -> pyspark.sql.DataFrame:
    """Select specified features from the static (geospatial) master table

    Args:
        ra_master_static: Reseller master table with static features (geospatial)
        master_table_params: Master table parameters from parameters.yml which contains list of static feature columns

    Returns:
        Reseller master table with selected static features (geospatial)
    """

    filter_month = get_filter_month(master_table_params["filter_month"])

    return ra_master_static.filter(f.col("month") == filter_month).select(
        master_table_params["agg_primary_columns"][0],
        *master_table_params["static_columns"],
    )


def filter_outlet_created_at_date(
    ra_master_dynamic: pyspark.sql.DataFrame,
    master_table_params: Dict[str, Any],
    dynamic_non_tsf_outlet_ids: pyspark.sql.DataFrame,
) -> Tuple[pyspark.sql.DataFrame, pyspark.sql.DataFrame]:
    """Retain only outlets which are created before the given month in the reseller dynamic master table
    - Also create month truncated column in dataframe

    Args:
        ra_master_dynamic: Reseller master table with dynamic features (revenue)
        master_table_params: Master table parameters from parameters.yml which contains month to filter by
        dynamic_non_tsf_outlet_ids: Outlet ids of the filtered month from the revenue master table
            without time series filling
    Returns:
        Reseller master table with outlets created before given month (revenue)
    """

    filter_month = get_filter_month(master_table_params["filter_month"])

    # create month column
    ra_master_dynamic = ra_master_dynamic.withColumn(
        "month",
        f.trunc(
            f.to_date(
                master_table_params["agg_date"],
                format=master_table_params["to_date_format"],
            ),
            "month",
        ),
    )

    output_df = ra_master_dynamic.filter(
        (
            f.to_date(
                "fea_outlet_created_at", format=master_table_params["to_date_format"]
            )
            < filter_month
        )
    ).drop_duplicates()

    output_df.cache()

    # get filtered outlets
    dynamic_tsf_outlet_ids = filtered_outlets_helper(
        ra_master_dynamic, output_df, master_table_params, filter_month
    )
    filtered_outlets = dynamic_tsf_outlet_ids.join(
        dynamic_non_tsf_outlet_ids, on=master_table_params["agg_primary_columns"][0]
    )

    return output_df, filtered_outlets


def build_target_variable(
    ra_master_dynamic: pyspark.sql.DataFrame, master_table_params: Dict[str, Any]
) -> pyspark.sql.DataFrame:
    """Create target variable by combining MKIOS and physical voucher redemption cashflows

    Args:
        ra_master_dynamic: Reseller master table with dynamic features (revenue)
        master_table_params: Master table parameters from parameters.yml which contains component names and target name

    Returns:
        Reseller master table with target variable added as a column (revenue)
    """

    component_funcs = [
        f.coalesce(f.col(component), f.lit(0.0))
        for component in master_table_params["target_variable_components"]
    ]

    ra_master_dynamic_with_target = ra_master_dynamic.withColumn(
        master_table_params["target_variable"], reduce(add, component_funcs)
    )

    return ra_master_dynamic_with_target


def filter_consecutive_zero_sales(
    ra_master_dynamic: pyspark.sql.DataFrame,
    master_table_params: Dict[str, Any],
    dynamic_non_tsf_outlet_ids: pyspark.sql.DataFrame,
) -> Tuple[pyspark.sql.DataFrame, pyspark.sql.DataFrame]:
    """Retain only outlets which do not exceed the specified number of consecutive days with zero cashflow

    Args:
        ra_master_dynamic: Reseller master table with dynamic features (revenue)
        master_table_params: Master table parameters from parameters.yml which contains month to filter by
        dynamic_non_tsf_outlet_ids: Outlet ids of the filtered month from the revenue master table
            without time series filling

    Returns:
        Reseller master table with outlets less than specified number of consecutive days with zero cashflow (revenue)
    """

    output_df = ra_master_dynamic
    filter_month = get_filter_month(master_table_params["filter_month"])

    if master_table_params["consecutive_days_zero_sale"] > 0:
        # Windows definition
        w1 = Window.partitionBy(
            ra_master_dynamic[master_table_params["agg_primary_columns"][0]]
        ).orderBy(ra_master_dynamic[master_table_params["agg_date"]])
        w2 = Window.partitionBy(
            ra_master_dynamic[master_table_params["agg_primary_columns"][0]],
            ra_master_dynamic[master_table_params["target_variable"]],
        ).orderBy(ra_master_dynamic[master_table_params["agg_date"]])

        # create difference between row numbers
        res = ra_master_dynamic.withColumn(
            "group", f.row_number().over(w1) - f.row_number().over(w2)
        )

        # Window definition for 0 streak
        w3 = Window.partitionBy(
            res[master_table_params["agg_primary_columns"][0]],
            res[master_table_params["target_variable"]],
            res["group"],
        ).orderBy(res[master_table_params["agg_date"]])

        # create column for streak of 0 value target variable
        streak_res = res.withColumn(
            "zero_sale_streak",
            f.when(res[master_table_params["target_variable"]] != 0, 0).otherwise(
                f.row_number().over(w3)
            ),
        )

        # group by max by primary columns and filter to only outlet_ids less than specified days of 0 sales
        filtered_consecutive_zero_sales = (
            streak_res.select(
                *master_table_params["agg_primary_columns"], "zero_sale_streak"
            )
            .filter(
                f.col(master_table_params["agg_primary_columns"][1]) == filter_month
            )
            .groupBy(master_table_params["agg_primary_columns"])
            .agg(f.max("zero_sale_streak").alias("zero_sale_streak_max"))
            .filter(
                f.col("zero_sale_streak_max")
                < master_table_params["consecutive_days_zero_sale"]
            )
            .select(master_table_params["agg_primary_columns"][0])
            .distinct()
        )

        output_df = ra_master_dynamic.join(
            filtered_consecutive_zero_sales,
            on=master_table_params["agg_primary_columns"][0],
        )

        # rename columns to avoid AnalysisException error
        output_df = output_df.withColumnRenamed(
            master_table_params["agg_primary_columns"][1],
            master_table_params["agg_primary_columns"][1],
        )

    output_df.cache()

    # get filtered outlets
    dynamic_tsf_outlet_ids = filtered_outlets_helper(
        ra_master_dynamic, output_df, master_table_params, filter_month
    )
    filtered_outlets = dynamic_tsf_outlet_ids.join(
        dynamic_non_tsf_outlet_ids, on=master_table_params["agg_primary_columns"][0]
    )

    return output_df, filtered_outlets


def filter_dynamic_master_month(
    ra_master_dynamic: pyspark.sql.DataFrame, master_table_params: Dict[str, Any]
) -> pyspark.sql.DataFrame:
    """Filter dynamic reseller master table to specified month (revenue)

    Args:
        ra_master_dynamic: Reseller master table with dynamic features (revenue)
        master_table_params: Master table parameters from parameters.yml which contains month to filter by

    Returns:
        Dynamic reseller master table filtered to specified month
    """

    filter_month = get_filter_month(master_table_params["filter_month"])

    # filter pre-aggregated dataframe to specified time period to speed up feature creation
    month_filtered_df = ra_master_dynamic.filter(
        f.col(master_table_params["agg_primary_columns"][1]) == filter_month
    )

    # cache this dataframe as it will be used repeatedly
    month_filtered_df.cache()

    return month_filtered_df


def agg_dynamic_numerical_features_sum_mean(
    ra_master_dynamic: pyspark.sql.DataFrame, master_table_params: Dict[str, Any]
) -> pyspark.sql.DataFrame:
    """Create sum and mean aggregated features for numerical features in dynamic reseller master table (revenue)

    Args:
        ra_master_dynamic: Reseller master table with dynamic features (revenue)
        master_table_params: Master table parameters from parameters.yml which contains group by sum/mean parameters

    Returns:
        Aggregated sum and mean numerical features on <outlet_id, month> level
    """

    filter_month = get_filter_month(master_table_params["filter_month"])

    # filter to relevant months for dynamic reseller master table
    months_filtered_df = (
        ra_master_dynamic.filter(
            f.col("month").between(
                (
                    filter_month
                    - relativedelta(
                        months=master_table_params["lagged_numerical_features_months"]
                    )
                ),
                filter_month,
            )
        )
        .withColumn("col_count", f.lit(1))
        .fillna(0, subset=master_table_params["agg_numerical_columns"])
    )

    # filter to only get rows where cashflow is non-zero
    filtered_dynamic = months_filtered_df.filter(
        f.col(master_table_params["target_variable"]) != 0
    )

    # get list of PySpark sum aggregation function calls for numerical columns
    numerical_col_sum_agg_funcs = [
        f.sum(col).alias(f"{col}_sum")
        for col in master_table_params["agg_numerical_columns"]
    ]

    # get list of PySpark mean aggregation function calls for numerical columns
    numerical_col_mean_agg_funcs = [
        f.avg(col).alias(f"{col}_mean")
        for col in master_table_params["agg_numerical_columns"]
    ]

    # perform group by sum aggregation
    agg_numerical_df = filtered_dynamic.groupBy(
        master_table_params["agg_primary_columns"]
    ).agg(*numerical_col_sum_agg_funcs, *numerical_col_mean_agg_funcs)

    return agg_numerical_df


def create_network_metric_features(
    aggregated_features_with_primary_columns_df: pyspark.sql.DataFrame,
    master_table_params: Dict[str, Any],
) -> pyspark.sql.DataFrame:
    """Create network-related metric features such as retainability and accessibility

    Args:
        aggregated_features_with_primary_columns_df: Aggregated features with primary columns DataFrame
        master_table_params:
            Master table parameters from parameters.yml which contains
                - Numerator columns for network metric feature
                - Denominator columns for network metric features
    Returns:
        Computed network metric features on <outlet_id, month> level
    """

    network_metrics_df = aggregated_features_with_primary_columns_df
    numerator_cols = master_table_params["agg_calc_percentages"]["numerators"]
    select_cols = master_table_params["agg_primary_columns"]

    if numerator_cols[0]:
        denominator_cols = master_table_params["agg_calc_percentages"]["denominators"]
        metric_col_names = [
            "_".join(numerator_col.split("_")[:-3]) for numerator_col in numerator_cols
        ]

        for idx, numerator_col in enumerate(numerator_cols):
            metric_col_name = metric_col_names[idx]
            denominator_col = denominator_cols[idx]
            network_metrics_df = network_metrics_df.withColumn(
                metric_col_name, (f.col(numerator_col) / f.col(denominator_col))
            )

        select_cols += metric_col_names

    network_metrics_df = network_metrics_df.select(select_cols)

    return network_metrics_df


def create_lagged_features(
    aggregated_features_with_primary_columns_df: pyspark.sql.DataFrame,
    master_table_params: Dict[str, Any],
) -> pyspark.sql.DataFrame:
    """Create time lagged features

    Args:
        aggregated_features_with_primary_columns_df: Aggregated features with primary columns DataFrame
        master_table_params:
            Master table parameters from parameters.yml which contains
                - Number of months to create lag features for
                - Primary columns to partition by and order for lagged features

    Returns:
        Lagged features and original unlagged features on <outlet_id, month> level
    """

    lagged_df = aggregated_features_with_primary_columns_df
    filter_month = get_filter_month(master_table_params["filter_month"])

    if master_table_params["lagged_numerical_features_months"] > 0:

        # create lagged features
        lagged_columns = [
            col
            for col in lagged_df.columns
            if col not in master_table_params["agg_primary_columns"]
        ]

        for lag_column in lagged_columns:
            for num_months in range(
                1, master_table_params["lagged_numerical_features_months"] + 1
            ):
                lagged_df = lagged_df.withColumn(
                    f"{lag_column}_past_{num_months}m",
                    f.lag(f.col(f"{lag_column}"), count=num_months).over(
                        Window.partitionBy(
                            master_table_params["agg_primary_columns"][0]
                        ).orderBy(f.col(master_table_params["agg_primary_columns"][1]))
                    ),
                )

        # filter to specified month
        lagged_df = lagged_df.filter(
            f.col(master_table_params["agg_primary_columns"][1]) == filter_month
        )

    return lagged_df


def agg_dynamic_categorical_features_mode(
    ra_master_dynamic: pyspark.sql.DataFrame, master_table_params: Dict[str, Any]
) -> pyspark.sql.DataFrame:
    """Create group by mode features for categorical features in dynamic reseller master table (revenue)

    Args:
        ra_master_dynamic: Reseller master table with dynamic features (revenue), filtered to specified month
        master_table_params: Master table parameters from parameters.yml which contains group by mode parameters

    Returns:
        Aggregated mode categorical features on <outlet_id, month> level
    """

    mode_columns = []

    for rolled_col_name in master_table_params["agg_mode_columns"]:
        output_col_name = rolled_col_name + "_mode"
        output_col_count = rolled_col_name + "_count"

        group_counts = (
            ra_master_dynamic.where(ra_master_dynamic[rolled_col_name].isNotNull())
            .groupBy(master_table_params["agg_primary_columns"] + [rolled_col_name])
            .count()
        )
        window = Window.partitionBy(master_table_params["agg_primary_columns"]).orderBy(
            f.desc("count"), f.asc(rolled_col_name)
        )
        mode_df = (
            group_counts.withColumn("order", f.row_number().over(window))
            .where(f.col("order") == 1)
            .select(
                master_table_params["agg_primary_columns"]
                + [rolled_col_name, f.col("count")]
            )
            .withColumnRenamed(rolled_col_name, output_col_name)
            .withColumnRenamed("count", output_col_count)
        )

        mode_columns.extend([output_col_name, output_col_count])

        # rename columns to avoid AnalysisException error
        mode_df = mode_df.withColumnRenamed(
            master_table_params["agg_primary_columns"][1],
            master_table_params["agg_primary_columns"][1],
        )

        ra_master_dynamic = ra_master_dynamic.join(
            mode_df, master_table_params["agg_primary_columns"], "left"
        )

    agg_mode_select_columns = master_table_params["agg_primary_columns"] + mode_columns
    agg_mode_df = ra_master_dynamic.select(agg_mode_select_columns).distinct()
    agg_mode_df = agg_mode_df.toDF(
        *(col.replace("_mode", "") for col in agg_mode_df.columns)
    )

    return agg_mode_df


def agg_dynamic_features_last(
    ra_master_dynamic: pyspark.sql.DataFrame, master_table_params: Dict[str, Any]
) -> pyspark.sql.DataFrame:
    """Create group by last features for features in dynamic reseller master table (revenue)

    Args:
        ra_master_dynamic: Reseller master table with dynamic features (revenue), filtered to specified month
        master_table_params: Master table parameters from parameters.yml which contains group by last parameters

    Returns:
        Aggregated last features on <outlet_id, month> level
    """

    # group by last value for numerical/categorical variables
    window = (
        Window()
        .partitionBy(master_table_params["agg_primary_columns"])
        .orderBy(f.col(master_table_params["agg_date"]).desc())
    )

    agg_last_df = (
        ra_master_dynamic.select(
            master_table_params["agg_primary_columns"]
            + master_table_params["agg_last_columns"]
            + [master_table_params["agg_date"]]
        )
        .withColumn("order", f.row_number().over(window))
        .where(f.col("order") == 1)
        .select(
            master_table_params["agg_primary_columns"]
            + master_table_params["agg_last_columns"]
        )
        .distinct()
    )

    return agg_last_df


def agg_dynamic_numerical_features_max(
    ra_master_dynamic: pyspark.sql.DataFrame, master_table_params: Dict[str, Any]
) -> pyspark.sql.DataFrame:
    """Create max aggregated features for numerical features in dynamic reseller master table (revenue)

    Args:
        ra_master_dynamic: Reseller master table with dynamic features (revenue), filtered to specified month
        master_table_params: Master table parameters from parameters.yml which contains group by max parameters

    Returns:
        Aggregated max numerical features on <outlet_id, month> level
    """

    # group by max value
    max_agg_funcs = [
        f.max(col).alias(f"{col}_max") for col in master_table_params["agg_max_columns"]
    ]

    agg_max_df = ra_master_dynamic.groupBy(
        master_table_params["agg_primary_columns"]
    ).agg(*max_agg_funcs)

    return agg_max_df


def join_agg_dynamic_features(
    master_table_params: Dict[str, Any], *aggregated_dataframes: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    """Join all aggregated features from dynamic reseller master table

    Args:
        master_table_params: Master table parameters from parameters.yml which contains primary columns parameters
        aggregated_dataframes: Dataframes of aggregated features on <outlet_id, month> level

    Returns:
        Joined aggregated features on <outlet_id, month> level
    """

    output_df = None

    for idx, dataframe in enumerate(aggregated_dataframes):

        # rename column to avoid pyspark AnalysisException
        dataframe = dataframe.withColumnRenamed(
            master_table_params["agg_primary_columns"][1],
            master_table_params["agg_primary_columns"][1],
        )

        if not idx:
            output_df = dataframe
        else:
            output_df = output_df.join(
                dataframe, master_table_params["agg_primary_columns"], "left"
            )

    return output_df


def join_ds_outlets_master_table(
    master_table_params: Dict[str, Any],
    ra_mck_int_features_dmp_dynamic_prepared: pyspark.sql.DataFrame,
    ra_mck_int_features_dmp_static_prepared: pyspark.sql.DataFrame,
    dynamic_non_tsf_outlet_ids: pyspark.sql.DataFrame,
) -> Tuple[pandas.DataFrame, pyspark.sql.DataFrame]:
    """Join both dynamic and static outlet features (geospatial and revenue)

    Args:
        master_table_params: Master table parameters from parameters.yml which contains primary columns parameters
        ra_mck_int_features_dmp_dynamic_prepared: Dynamic features of outlets
        ra_mck_int_features_dmp_static_prepared: Static features of outlets
        dynamic_non_tsf_outlet_ids: Outlet ids of the filtered month from the revenue master table
            without time series filling
    Returns:
        Joined reseller master table on <outlet_id, month> level as a pandas DataFrame
    """

    filter_month = get_filter_month(master_table_params["filter_month"])

    master = (
        ra_mck_int_features_dmp_dynamic_prepared.drop_duplicates()
        .join(
            ra_mck_int_features_dmp_static_prepared,
            master_table_params["agg_primary_columns"][0],
            "left",
        )
        .replace({"": None})
    )

    # filter outlets whose latitude and longitude does not reside in Indonesia
    output_df = master.filter(
        (f.col("fea_outlet_string_location_longitude") >= 94.7717124)
        & (f.col("fea_outlet_string_location_longitude") <= 141.0194444)
        & (f.col("fea_outlet_string_location_latitude") >= -11.2085669)
        & (f.col("fea_outlet_string_location_latitude") <= 6.2744496)
    )

    output_df.cache()

    # get filtered outlets
    dynamic_tsf_outlet_ids = filtered_outlets_helper(
        master, output_df, master_table_params, filter_month
    )
    filtered_outlets = dynamic_tsf_outlet_ids.join(
        dynamic_non_tsf_outlet_ids, on=master_table_params["agg_primary_columns"][0]
    )

    # prepare for output to pandas DataFrame
    master_pandas = output_df.orderBy(
        master_table_params["agg_primary_columns"], ascending=True
    ).toPandas()

    return master_pandas, filtered_outlets
