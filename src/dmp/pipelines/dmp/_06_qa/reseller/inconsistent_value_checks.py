import logging

import pyspark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType

from utils import get_end_date, get_start_date

log = logging.getLogger(__name__)


def get_additional_starting_outlet_numbers(
    df_digipos: pyspark.sql.DataFrame, df_mkios: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:

    start_date = get_start_date(period="1cm")
    end_date = get_end_date(period="1cm")

    df_left = (
        df_mkios.select("outlet_id")
        .distinct()
        .join(df_digipos.select("outlet_id").distinct(), ["outlet_id"], how="left_anti")
    )

    valid_records_extra = int(df_left.select("outlet_id").distinct().count())
    spark = SparkSession.builder.getOrCreate()
    data = [(valid_records_extra,)]
    schema = StructType([StructField("count", IntegerType(), True),])
    df = spark.createDataFrame(data, schema=schema)
    df = (
        df.withColumn("start_date", f.lit(start_date.strftime("%Y-%m-%d")))
        .withColumn("end_date", f.lit(end_date.strftime("%Y-%m-%d")))
        .withColumn("run_date_time", f.current_timestamp())
    )
    return df


def track_outlet_filtering(df_all_filters: pyspark.sql.DataFrame,):
    # Returning parquet file to store as SparkHiveDataSet
    return df_all_filters
