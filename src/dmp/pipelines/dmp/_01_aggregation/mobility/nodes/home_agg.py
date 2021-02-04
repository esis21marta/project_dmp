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


def mobility_home_stay_agg(
    home_stay_point: pyspark.sql.DataFrame, month_dt_dict
) -> pyspark.sql.DataFrame:
    # defining the udf for geohash encode and neighbour geohash
    geo_encode_udf = f.udf(lambda x, y: encode(x, y, precision=5), t.StringType())

    dt_func_udf = f.udf(lambda x: month_dt_dict.get(x), t.StringType())

    # home stay point processing

    df = (
        home_stay_point.where(f.col("mo_id") >= "2019-07")
        .selectExpr(
            "imsi as msisdn",
            "mo_id",
            "home1_lat",
            "home1_lon",
            "home1_days",
            "home1_duration",
            "home1_kelurahan_name",
            "home1_neighboring_kelurahan_name",
            "home1_kecamatan_name",
            "home1_kabupaten_name",
            "home1_province_name",
        )
        .withColumn(
            "geohash",
            geo_encode_udf(home_stay_point.home1_lat, home_stay_point.home1_lon),
        )
    )

    df_home_geohash_add = df.groupBy(
        "msisdn",
        "mo_id",
        "geohash",
        "home1_kelurahan_name",
        "home1_neighboring_kelurahan_name",
        "home1_kecamatan_name",
        "home1_kabupaten_name",
        "home1_province_name",
    ).agg(
        f.sum(f.col("home1_days")).alias("home1_days"),
        f.sum(f.col("home1_duration")).alias("home1_duration"),
        f.countDistinct(f.col("home1_kelurahan_name")).alias("village_cnt"),
        f.first(f.col("home1_lat")).alias("home1_lat"),
        f.first(f.col("home1_lon")).alias("home1_lon"),
    )

    # getting the maximum duration for a given msisdn, geohash combination in a month
    df_home_geohash = (
        df_home_geohash_add.groupBy("msisdn", "mo_id")
        .agg(f.max(f.col("home1_duration")).alias("home1_duration_max"))
        .join(df_home_geohash_add, on=["msisdn", "mo_id"])
        .withColumn(
            "duration_max_flag",
            f.when(f.col("home1_duration") == f.col("home1_duration_max"), 1).otherwise(
                0
            ),
        )
        .withColumn(
            "month_mapped_dt", f.date_format(dt_func_udf(f.col("mo_id")), "yyyy-MM-dd")
        )
    )

    return df_home_geohash


def create_home_agg_monthly(df_home: pyspark.sql.DataFrame, month_dt_dict) -> None:
    """
    Weekly aggregation for PayU Usage tables.

    Args:
        df_home: home Data.
        month_dt_dict
    """

    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date(period="1cm", context=None, partition_column="mo_id")
    end_date = get_end_date(context=None, partition_column="mo_id", period="1cm")

    monthly_agg_catalog = conf_catalog["l2_home_stay"]

    load_args = conf_catalog["l1_home_stay"]["load_args"]
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
    log.info(f"Load Args: {load_args}")
    log.info(f"Save Args: {save_args}")
    log.info(f"Partitions: {partitions}")

    while start_date < end_date:
        month_start = start_date.strftime("%Y-%m")
        sdate = start_date.strftime(load_args["partition_date_format"])

        log.info("Starting Monthly Aggregation for month: {}".format(month_start))

        df_data = df_home.filter(f.col(load_args["partition_column"]) == sdate)

        df = mobility_home_stay_agg(df_data, month_dt_dict).drop(f.col("mo_id"))

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
