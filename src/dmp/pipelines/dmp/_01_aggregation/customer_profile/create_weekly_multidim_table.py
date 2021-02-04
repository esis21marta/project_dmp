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


def _weekly_aggregation(
    df_cb_multi_dim: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Returns customer loyalty tier and points usage features.

    Args:
        df_cb_multi_dim: Customer profile dataframe (multidim snapshot table)

    Returns:
        df_cb_multi_dim with msisdn, weekstart and customer profile features (in select statement below)

    """

    df_cb_multi_dim = df_cb_multi_dim.withColumn(
        "trx_date", f.to_date(f.col("trx_date").cast(StringType()), "yyyy-MM-dd")
    )

    df_cb_multi_dim = df_cb_multi_dim.withColumn(
        "weekstart", next_week_start_day(f.col("trx_date"))
    )

    df_agg = df_cb_multi_dim.groupBy(f.col("msisdn"), f.col("weekstart")).agg(
        f.max(f.col("lte_usim_user_flag")).alias("fea_custprof_lte_usim_user_flag"),
        f.max(f.col("area_sales")).alias("fea_custprof_area_sales"),
        f.max(f.col("region_sales")).alias("fea_custprof_region_sales"),
        f.max(f.col("bill_responsibility_type")).alias(
            "fea_custprof_bill_responsibility_type"
        ),
        f.max(f.col("segment_data_user")).alias("fea_custprof_segment_data_user"),
        f.max(f.col("los")).alias("fea_custprof_los"),
        f.max(f.col("status")).alias("fea_custprof_status"),
        f.max(f.col("brand")).alias("fea_custprof_brand"),
        f.max(f.col("price_plan")).alias("fea_custprof_price_plan"),
        f.max(f.col("cust_type_desc")).alias("fea_custprof_cust_type_desc"),
        f.max(f.col("cust_subtype_desc")).alias("fea_custprof_cust_subtype_desc"),
        f.max(f.col("segment_hvc_mtd")).alias("fea_custprof_segment_hvc_mtd"),
        f.max(f.col("segment_hvc_m1")).alias("fea_custprof_segment_hvc_m1"),
        f.max(f.col("loyalty_tier")).alias("fea_custprof_loyalty_tier"),
        f.max(f.col("nik_gender")).alias("fea_custprof_nik_gender"),
        f.max(f.col("nik_age")).alias("fea_custprof_nik_age"),
        f.max(f.col("bill_cycle")).alias("fea_custprof_bill_cycle"),
        f.max(f.col("persona_los")).alias("fea_custprof_persona_los"),
        f.max(f.col("prsna_quadrant")).alias("fea_custprof_prsna_quadrant"),
        f.max(f.col("arpu_segment_name")).alias("fea_custprof_arpu_segment_name"),
        f.max(f.col("mytsel_user_flag")).alias("fea_custprof_mytsel_user_flag"),
        f.max(f.col("kecamatan")).alias("fea_custprof_kecamatan"),
        f.max(f.col("kabupaten")).alias("fea_custprof_kabupaten"),
        f.max(f.col("persona_id")).alias("persona_id"),
        f.avg(f.col("tsel_poin")).alias("custprof_tsel_poin_avg"),
        f.min(f.col("tsel_poin")).alias("custprof_tsel_poin_min"),
        f.max(f.col("tsel_poin")).alias("custprof_tsel_poin_max"),
    )

    return df_agg


def agg_customer_profile_to_weekly(df_cb_multi_dim: pyspark.sql.DataFrame,) -> None:
    """
    Fetches customer loyalty tier and points usage data.

    Args:
        df_cb_multi_dim: Customer profile dataframe (multidim snapshot table)

    """

    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date()
    end_date = get_end_date()

    # Data before  June 2019  is not correct
    df_cb_multi_dim = df_cb_multi_dim.filter(
        f.col("event_date") >= "2019-06-01"
    ).filter(
        f.col("event_date").between(
            (start_date - timedelta(days=7)).strftime("%Y-%m-%d"),
            (end_date + timedelta(days=7)).strftime("%Y-%m-%d"),
        )
    )

    weekly_agg_catalog = conf_catalog["l2_customer_profile_weekly"]

    save_args = weekly_agg_catalog["save_args"]
    save_args.pop("partitionBy", None)
    file_path = get_file_path(filepath=weekly_agg_catalog["filepath"])
    file_format = weekly_agg_catalog["file_format"]
    partitions = int(weekly_agg_catalog["partitions"])
    log.info(
        "Starting Weekly Aggregation for WeekStart {start_date} to {end_date}".format(
            start_date=(start_date + timedelta(days=7)).strftime("%Y-%m-%d"),
            end_date=(end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
        )
    )
    log.info(f"File Path: {file_path}")
    log.info(f"File Format: {file_format}")
    log.info(f"Save Args: {save_args}")
    log.info(f"Partitions: {partitions}")

    while start_date < end_date:
        week_start = (start_date + timedelta(days=7)).strftime("%Y-%m-%d")
        edate = (start_date + timedelta(days=6)).strftime("%Y-%m-%d")

        log.info("Starting Weekly Aggregation for WeekStart: {}".format(week_start))

        # Only sunday data is required
        df_data = df_cb_multi_dim.filter(f.col("trx_date") == edate)

        df = _weekly_aggregation(df_cb_multi_dim=df_data).drop(f.col("weekstart"))

        partition_file_path = "{file_path}/weekstart={weekstart}".format(
            file_path=file_path, weekstart=week_start
        )

        df.repartition(numPartitions=partitions).write.save(
            partition_file_path, file_format, **save_args
        )
        log.info(
            "Completed Weekly Aggregation for WeekStart: {}".format(
                (start_date + timedelta(days=7)).strftime("%Y-%m-%d")
            )
        )

        start_date += timedelta(days=7)
