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
from datetime import timedelta

import pyspark
import pyspark.sql.functions as f
from pyspark.sql.types import StringType

from utils import (
    get_config_parameters,
    get_end_date,
    get_start_date,
    next_week_start_day,
)
from utils.spark_data_set_helper import get_file_path

log = logging.getLogger(__name__)


def convert_hexa_to_text(df):
    """
    Function for convert column from hexadecimal to Text ASCII

    Args:
        df: column which need to convert

    Returns:
        df: columns with convertion
    """

    df = df.withColumn(
        "bnumber_decoded",
        f.when(
            f.length("bnumber") % 2 != 0,
            f.expr("substring(bnumber, 0, length(bnumber)-1)"),
        ).otherwise(df["bnumber"]),
    )
    df = df.withColumn(
        "bnumber_decoded",
        f.when(
            ~df.bnumber.startswith("628"),
            f.decode(
                f.unhex(f.translate(f.col("bnumber_decoded"), ":;<=>?", "ABCDEF")),
                "US-ASCII",
            ),
        ).otherwise(df["bnumber"]),
    )
    df = df.withColumn(
        "bnumber_decoded",
        f.when(f.lower(df["bnumber_decoded"]).contains("krediv"), "kredivo").otherwise(
            df["bnumber_decoded"]
        ),
    )

    return df


def get_kredivo_msisdns(df_weekly: pyspark.sql.DataFrame,):
    """
    Get MSISDNs list for kredivo SMS recepients

    Args:
        df_weekly: Weekly Aggregation Commercial Text Messaging

    Returns:
        df_kredivo: MSISDNs list for kredivo SMS recepients
    """
    df_kredivo = (
        df_weekly.filter(f.array_contains(f.col("senders_07d"), "kredivo"))
        .select("msisdn")
        .distinct()
    )

    return df_kredivo


def _weekly_aggregation(
    df_abt_usage_mss_dd: pyspark.sql.DataFrame,
    df_comm_text_mapping: pyspark.sql.DataFrame,
    df_broken_bnumber: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:

    """
    Creates Weekly Commercial Text Messaging for the partner MSISDNs

    Args:
        df_abt_usage_mss_dd: Daily Commercial Text Messaging Usage DataFrame
        df_comm_text_mapping: Mapping between Sender and Category
        df_broken_bnumber: Mapping broken bnumber with sender & category

    Returns:
        df_weekly: Text Messaging Weekly DataFrame
    """

    df_abt_usage_mss_dd = (
        df_abt_usage_mss_dd.withColumn(
            "event_date",
            f.to_date(f.col("event_date").cast(StringType()), "yyyy-MM-dd"),
        )
        .withColumn("weekstart", next_week_start_day(f.col("event_date")))
        .withColumnRenamed("anumber", "msisdn")
    )

    df_abt_usage_mss_dd = convert_hexa_to_text(df_abt_usage_mss_dd)

    # remove first string to handle broken data
    df_abt_usage_mss_dd = df_abt_usage_mss_dd.withColumn(
        "bnumber_decoded_sub",
        f.expr("substring(bnumber_decoded, 2, length(bnumber_decoded))"),
    )
    df_comm = (
        df_abt_usage_mss_dd.join(
            f.broadcast(df_comm_text_mapping).alias("A"),
            (
                (
                    f.lower(df_abt_usage_mss_dd.bnumber_decoded)
                    == f.lower(f.col("A.sender"))
                )
            ),
            how="left",
        )
        .join(
            f.broadcast(df_comm_text_mapping).alias("B"),
            (
                (
                    f.lower(df_abt_usage_mss_dd.bnumber_decoded_sub)
                    == f.lower(f.col("B.sender"))
                )
            ),
            how="left",
        )
        .join(
            f.broadcast(df_broken_bnumber).alias("C"),
            ((df_abt_usage_mss_dd.bnumber == f.col("C.bnumber"))),
            how="left",
        )
        .withColumn(
            "sender_real",
            f.coalesce(f.col("A.sender"), f.col("B.sender"), f.col("C.sender")),
        )
        .withColumn(
            "category_real",
            f.coalesce(f.col("A.category"), f.col("B.category"), f.col("C.category")),
        )
        .withColumn(
            "evalueserve_comments_real",
            f.coalesce(
                f.col("A.evalueserve_comments"),
                f.col("B.evalueserve_comments"),
                f.col("C.evalueserve_comments"),
            ),
        )
    )
    df_comm = (
        df_comm.drop("sender", "category", "evalueserve_comments")
        .filter((~f.isnull("sender_real")) & (~f.isnull("category_real")))
        .withColumnRenamed("sender_real", "sender")
        .withColumnRenamed("category_real", "category")
        .withColumnRenamed("evalueserve_comments_real", "evalueserve_comments")
    )

    df_weekly = df_comm.groupBy("msisdn", "weekstart", "category").agg(
        f.collect_set("sender").alias("senders_07d"),
        f.sum(
            f.when((f.col("calltype") == "05_sms_in"), f.col("total_trx")).otherwise(0)
        ).alias("incoming_count_07d"),
        f.sum(
            f.when(
                (f.col("calltype") == "05_sms_in")
                & f.col("evalueserve_comments").isin(
                    {
                        "bureau of taxation and taxation as a sub-ordinate of the financial administration - public sector",
                        "tax office",
                    }
                ),
                f.col("total_trx"),
            ).otherwise(0)
        ).alias("count_txt_msg_incoming_government_tax"),
    )

    return df_weekly


def create_commercial_text_messaging_weekly(
    df_abt_usage_mss_dd: pyspark.sql.DataFrame,
    df_comm_text_mapping: pyspark.sql.DataFrame,
    df_broken_bnumber: pyspark.sql.DataFrame,
) -> None:
    """
    Creates Weekly Commercial Text Messaging for the partner MSISDNs

    Args:
        df_abt_usage_mss_dd: Daily Commercial Text Messaging Usage DataFrame
        df_comm_text_mapping: Mapping between Sender and Category
        df_broken_bnumber: Mapping broken bnumber with sender & category

    """

    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date()
    end_date = get_end_date()

    weekly_agg_catalog = conf_catalog["l2_commercial_text_messaging_weekly"]

    load_args = conf_catalog["l1_abt_usage_mss_abt_dd"]["load_args"]
    save_args = weekly_agg_catalog["save_args"]
    save_args.pop("partitionBy", None)
    file_path = get_file_path(filepath=weekly_agg_catalog["filepath"])
    file_format = weekly_agg_catalog["file_format"]
    partitions = int(weekly_agg_catalog["partitions"])

    kredivo_sms_catalog = conf_catalog["l2_kredivo_sms_recipients"]
    kredivo_save_args = kredivo_sms_catalog["save_args"]
    kredivo_save_args.pop("partitionBy", None)
    kredivo_file_path = get_file_path(filepath=kredivo_sms_catalog["filepath"])
    kredivo_file_format = kredivo_sms_catalog["file_format"]
    kredivo_partitions = int(kredivo_sms_catalog["partitions"])

    log.info(
        "Starting Weekly Aggregation for WeekStart {start_date} to {end_date}".format(
            start_date=(start_date + timedelta(days=7)).strftime("%Y-%m-%d"),
            end_date=(end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
        )
    )
    log.info(f"File Path: {file_path}")
    log.info(f"File Format: {file_format}")
    log.info(f"Load Args: {load_args}")
    log.info(f"Save Args: {save_args}")
    log.info(f"Partitions: {partitions}")

    # Caching df_comm_text_mapping, So that Spark won't end up reading this DataFrame from HDFS in each iteration
    df_comm_text_mapping.cache()
    df_broken_bnumber.cache()

    while start_date < end_date:
        week_start = (start_date + timedelta(days=7)).strftime("%Y-%m-%d")
        sdate = start_date.strftime(load_args["partition_date_format"])
        edate = (start_date + timedelta(days=6)).strftime(
            load_args["partition_date_format"]
        )

        log.info("Starting Weekly Aggregation for WeekStart: {}".format(week_start))

        df_data = df_abt_usage_mss_dd.filter(
            f.col(load_args["partition_column"]).between(sdate, edate)
        )

        df = _weekly_aggregation(
            df_abt_usage_mss_dd=df_data,
            df_comm_text_mapping=df_comm_text_mapping,
            df_broken_bnumber=df_broken_bnumber,
        ).drop(f.col("weekstart"))

        df.cache()

        partition_file_path = "{file_path}/weekstart={weekstart}".format(
            file_path=file_path, weekstart=week_start
        )

        df.repartition(numPartitions=partitions).write.save(
            partition_file_path, file_format, **save_args
        )

        kredivo_partition_file_path = "{kredivo_file_path}/weekstart={weekstart}".format(
            kredivo_file_path=kredivo_file_path, weekstart=week_start
        )
        df_kredivo = get_kredivo_msisdns(df)
        df_kredivo.repartition(numPartitions=kredivo_partitions).write.save(
            kredivo_partition_file_path, kredivo_file_format, **kredivo_save_args
        )
        log.info(
            "Completed Weekly Aggregation for WeekStart: {}".format(
                (start_date + timedelta(days=7)).strftime("%Y-%m-%d")
            )
        )

        start_date += timedelta(days=7)
    df_comm_text_mapping.unpersist()
