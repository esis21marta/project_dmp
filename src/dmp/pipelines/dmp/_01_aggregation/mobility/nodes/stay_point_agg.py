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

import pyspark
import pyspark.sql.functions as f
import pyspark.sql.types as t

from utils import encode, get_config_parameters, get_end_date, get_start_date
from utils.spark_data_set_helper import get_file_path

log = logging.getLogger(__name__)


def mobility_stay_point_agg(
    stay_point: pyspark.sql.DataFrame, month_dt_dict
) -> pyspark.sql.DataFrame:
    # defining the udf for geohash encode and neighbour geohash
    geo_encode_udf = f.udf(lambda x, y: encode(x, y, precision=5), t.StringType())

    # neighbor_udf = f.udf(lambda x: neighbors(x), t.ArrayType(t.StringType()))

    dt_func_udf = f.udf(lambda x: month_dt_dict.get(x), t.StringType())

    # stay point processing

    df_stay_point = (
        stay_point.where(f.to_date(f.col("calling_date"), "yyyy-MM") >= "2019-07")
        .withColumn(
            "geohash", (geo_encode_udf(stay_point.grid_lat, stay_point.grid_lon))
        )
        .withColumn(
            "stay_duration",
            f.unix_timestamp(f.col("end_date")) - f.unix_timestamp(f.col("start_date")),
        )
        .withColumn("mo_id", f.date_format(f.col("calling_date"), "yyyy-MM"))
    )

    df_stay_point_agg = (
        df_stay_point.groupBy("msisdn", "geohash", "mo_id",)
        .agg(
            f.sum(f.col("stay_duration")).alias("agg_duration"),
            f.first(f.col("kelurahan_name")).alias("kelurahan_name"),
            f.first(f.col("kecamatan_name")).alias("kecamatan_name"),
            f.first(f.col("kabupaten_name")).alias("kabupaten_name"),
            f.first(f.col("province_name")).alias("province_name"),
            f.first(f.col("grid_lat")).alias("lat1"),
            f.first(f.col("grid_lon")).alias("long1"),
            f.countDistinct(f.col("kelurahan_name")).alias("village_cnt"),
        )
        .withColumn(
            "month_mapped_dt", f.date_format(dt_func_udf(f.col("mo_id")), "yyyy-MM-dd")
        )
    )

    df_stay_point_agg_max = df_stay_point_agg.groupBy("msisdn", "mo_id").agg(
        f.max(f.col("agg_duration")).alias("agg_duration_max")
    )

    df_stay_point_final = df_stay_point_agg.join(
        df_stay_point_agg_max, on=["msisdn", "mo_id"]
    ).withColumn(
        "duration_max_flag",
        f.when(f.col("agg_duration") == f.col("agg_duration_max"), 1).otherwise(0),
    )

    return df_stay_point_final


def create_staypoint_agg_monthly(
    df_staypoint: pyspark.sql.DataFrame, month_dt_dict
) -> None:
    """
    Weekly aggregation for PayU Usage tables.

    Args:
        df_work: work Data.
        month_dt_dict
    """

    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date(period="1cm", context=None, partition_column="mo_id")
    end_date = get_end_date(context=None, partition_column="mo_id", period="1cm")

    monthly_agg_catalog = conf_catalog["l2_staypoint"]

    # load_args = conf_catalog["l1_staypoint"]["load_args"]
    save_args = monthly_agg_catalog["save_args"]
    save_args.pop("partitionBy", None)
    file_path = get_file_path(filepath=monthly_agg_catalog["filepath"])
    file_format = monthly_agg_catalog["file_format"]
    partitions = int(monthly_agg_catalog["partitions"])
    log.info(
        "Starting Monthly Aggregation for Month {start_date}".format(
            start_date=start_date.strftime("%Y-%m")
        )
    )
    log.info(f"File Path: {file_path}")
    log.info(f"File Format: {file_format}")
    # log.info(f"Load Args: {load_args}")
    log.info(f"Save Args: {save_args}")
    log.info(f"Partitions: {partitions}")

    while start_date < end_date:
        month_start = start_date.strftime("%Y-%m")
        sdate = start_date.strftime("%Y-%m")

        log.info("Starting Monthly Aggregation for month: {}".format(month_start))

        df_data = (
            df_staypoint.withColumn(
                "mo_id", f.date_format(f.col("calling_date"), "yyyy-MM")
            )
            .filter(f.col("mo_id") == sdate)
            .drop("mo_id")
        )

        df = mobility_stay_point_agg(df_data, month_dt_dict).drop(f.col("mo_id"))

        partition_file_path = "{file_path}/mo_id={mo_id}".format(
            file_path=file_path, mo_id=month_start
        )

        df.repartition(numPartitions=partitions).write.save(
            partition_file_path, file_format, **save_args
        )
        log.info("Completed Monthly Aggregation for month: {}".format(sdate))

        sdmonth = start_date.month
        if sdmonth < 12:
            start_date = start_date.replace(month=sdmonth + 1, day=1)
        else:
            sdyear = start_date.year
            start_date = start_date.replace(year=sdyear + 1, month=1, day=1)
