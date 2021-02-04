# in-built libraries
from typing import Any, Dict

# third-party libraries
import pandas
import pyspark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

# internal code
from ..utils import get_filter_month


def save_filtered_outlets_to_hive(
    created_at_date_filtered_outlets: pyspark.sql.DataFrame,
    seven_consecutive_zero_sales_days_filtered_outlets: pyspark.sql.DataFrame,
    indo_coordinates_filtered_outlets: pyspark.sql.DataFrame,
    bottom_threshold_filtered_outlets: pandas.DataFrame,
    top_threshold_filtered_outlets: pandas.DataFrame,
    master_table_params: Dict[str, Any],
) -> pyspark.sql.DataFrame:

    # convert pandas dataframes to spark dataframes
    spark = SparkSession.builder.getOrCreate()
    bottom_threshold_filtered_outlets_spark = spark.createDataFrame(
        bottom_threshold_filtered_outlets
    )
    top_threshold_filtered_outlets_spark = spark.createDataFrame(
        top_threshold_filtered_outlets
    )

    # add filter reasons
    filter_reason_dict = {
        "DS-filter_outlet_created_at_date-filtered_outlet_on_created_at_date": created_at_date_filtered_outlets,
        "DS-filter_consecutive_zero_sales-filtered_outlet_seven_or_greater_consecutive_zero_sales": seven_consecutive_zero_sales_days_filtered_outlets,
        "DS-join_ds_outlets_master_table-filtered_outlet_indo_coordinates": indo_coordinates_filtered_outlets,
        "DS-master_sample-filtered_outlet_below_bottom_cashflow_threshold": bottom_threshold_filtered_outlets_spark,
        "DS-master_sample-filtered_outlet_above_top_cashflow_threshold": top_threshold_filtered_outlets_spark,
    }
    filter_with_reasons_list = []
    for reason, spark_df in filter_reason_dict.items():
        filter_with_reasons_list.append(
            spark_df.withColumn("filter_type", f.lit(reason))
        )

    # union spark dataframes
    unioned_df = filter_with_reasons_list[0]
    for spark_df in filter_with_reasons_list[1:]:
        unioned_df = unioned_df.union(spark_df)

    # add run date
    unioned_df = unioned_df.withColumn("run_date_time", f.current_timestamp())

    # add start and end date
    filter_month = get_filter_month(master_table_params["filter_month"])
    unioned_df = unioned_df.withColumn("start_date", f.lit(filter_month)).withColumn(
        "end_date", f.date_sub(f.add_months(f.lit(filter_month), 1), 1)
    )

    return unioned_df
