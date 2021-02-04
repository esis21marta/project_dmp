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
from typing import *

import pyspark
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.window import *

from src.dmp.pipelines.dmp._02_primary.scaffold.create_scaffold import (
    create_lacci_scaffold,
)
from utils import get_config_parameters, get_end_date, get_start_date
from utils.spark_data_set_helper import get_file_path

log = logging.getLogger(__name__)


def subselect_mkios_by_digipos_rs_msisdns(
    df_mkios: pyspark.sql.DataFrame,
    df_mapping_table_outlet_id_msisdn: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Subselects rs_msisdns from smy.rech_mkios_dd for whom there is
    an rs_msisdn and outlet_id mapping present in the mapping table.

    :param df_mkios: Table containing all recharges and revenue data
    :param df_mapping_table_outlet_id_msisdn: Mapping table between rs_msisdn
    and outlet_id.
    :return: Subselected data from the mkios table for which a mapping exists
    in the mapping table (outlet_digipos_reference_dd).
    """
    dfj_digipos_mkios = df_mkios.join(
        f.broadcast(df_mapping_table_outlet_id_msisdn),
        (f.col("rs_msisdn") == f.col("outlet_msisdn")),
        how="inner",
    )
    return dfj_digipos_mkios


def _agg_recharges_by_modern_channel_to_daily(
    df_mkios: pyspark.sql.DataFrame, df_urp: pyspark.sql
) -> pyspark.sql.DataFrame:

    # Getting min, max date for a subs_lacci_id from mkios
    df_mkios_min_max = df_mkios.groupBy("subs_lacci_id").agg(
        f.min("trx_date").alias("min_trx_date"), f.max("trx_date").alias("max_trx_date")
    )

    # Getting min, max date for a subs_lacci_id from urp
    df_urp_min_max = df_urp.groupBy("subs_lacci_id").agg(
        f.min("trx_date").alias("min_trx_date"), f.max("trx_date").alias("max_trx_date")
    )

    # Getting overall min, max date for a subs_lacci_id
    df_subs_lacci_id_min_max = (
        df_mkios_min_max.union(df_urp_min_max)
        .groupBy("subs_lacci_id")
        .agg(
            f.min("min_trx_date").alias("min_trx_date"),
            f.max("max_trx_date").alias("max_trx_date"),
        )
    )

    # Creating a scaffold with columns - subs_lacci_id, trx_date
    # The trx_date ranges between min, max date computed above
    df_daily_scaffold = create_lacci_scaffold(
        df_subs_lacci_id_min_max, ["subs_lacci_id", "trx_date"]
    )

    # Filling time series for mkios table on subs_lacci_id, trx_date
    df_mkios_tsf = (
        df_mkios.join(df_daily_scaffold, ["subs_lacci_id", "trx_date"], how="full")
        .withColumnRenamed("subs_lacci_id", "subs_lacci_id_mkios")
        .withColumnRenamed("rs_lacci_id", "rs_lacci_id_mkios")
        .withColumnRenamed("trx_date", "trx_date_mkios")
        .select("subs_lacci_id_mkios", "rs_lacci_id_mkios", "trx_date_mkios")
        .distinct()
    )

    # Calculating monthly mode for subs_lacci_id, trx_date, for cases where
    # rs_lacci_id is missing after the time series filling of mkios table
    df_mkios_monthly_mode_for_lacci_id = (
        df_mkios.withColumn("month", f.trunc("trx_date", "month"))
        .groupBy("subs_lacci_id", "month", "rs_lacci_id")
        .agg(f.count("*").alias("count"), f.max("trx_date").alias("max_trx_date"))
        .withColumn(
            "row_number",
            f.row_number().over(
                Window.partitionBy("subs_lacci_id", "month").orderBy(
                    f.desc("count"), f.desc("max_trx_date")
                )
            ),
        )
        .filter(f.col("row_number") == 1)
        .withColumnRenamed("rs_lacci_id", "rs_lacci_id_mode")
        .withColumnRenamed("subs_lacci_id", "subs_lacci_id_mkios")
        .select("subs_lacci_id_mkios", "month", "rs_lacci_id_mode")
    )

    # Calculating total_recharge_modern_channel on subs_lacci_id, trx_date level
    df_urp_rech = df_urp.groupBy("subs_lacci_id", "trx_date").agg(
        f.sum(f.coalesce("rech", f.lit(0))).alias("total_recharge_modern_channel")
    )

    df_urp_rech = (
        df_urp_rech.withColumnRenamed("subs_lacci_id", "subs_lacci_id_urp")
        .withColumnRenamed("rs_lacci_id", "rs_lacci_id_urp")
        .withColumnRenamed("trx_date", "trx_date_urp")
    )

    # Left joining time series filled mkios table with urp table on subs_lacci_id,
    # trx_date to match modern channel recharges
    dfj_mkios_tsf_urp = df_mkios_tsf.join(
        df_urp_rech,
        [
            df_mkios_tsf.subs_lacci_id_mkios == df_urp_rech.subs_lacci_id_urp,
            df_mkios_tsf.trx_date_mkios == df_urp_rech.trx_date_urp,
        ],
        how="left",
    )

    dfj_mkios_tsf_urp = dfj_mkios_tsf_urp.withColumn(
        "month", f.trunc("trx_date_mkios", "month")
    )

    # Filling up all the missing lacci_id which are null after the left join of
    # mkios and urp table. The missing values are replaced with monthly mode and
    # all rows having subs_lacci_id_urp as null as these are rows which need not
    # be matched
    dfj_tsf_rs_lacci_filled = (
        dfj_mkios_tsf_urp.join(
            df_mkios_monthly_mode_for_lacci_id,
            ["subs_lacci_id_mkios", "month"],
            how="left",
        )
        .withColumn(
            "rs_lacci_id_mkios",
            (
                f.when(
                    f.col("rs_lacci_id_mkios").isNull(), f.col("rs_lacci_id_mode")
                ).otherwise(f.col("rs_lacci_id_mkios"))
            ),
        )
        .filter(f.col("subs_lacci_id_urp").isNotNull())
    )

    # Caculating individual channel based recharges from urp table
    df_urp_rech_by_channel = (
        df_urp.withColumn(
            "channel_group",
            f.when(f.col("channel_group").isNull(), f.lit("null")).otherwise(
                f.col("channel_group")
            ),
        )
        .withColumn(
            "channel_group", f.regexp_replace(f.lower(f.col("channel_group")), " ", "_")
        )
        .withColumn(
            "channel_group",
            f.concat(f.lit("fea_outlets_urp_channel_rech_"), f.col("channel_group")),
        )
        .groupBy("subs_lacci_id", "trx_date")
        .pivot("channel_group")
        .agg(f.coalesce(f.sum("rech"), f.lit(0)).alias("recharge_modern_channel"))
    )

    # Joining lacci_filled table with urp channel based recharges
    df_modern_channel_by_subs_lacci_id = dfj_tsf_rs_lacci_filled.join(
        df_urp_rech_by_channel,
        [
            dfj_tsf_rs_lacci_filled.subs_lacci_id_mkios
            == df_urp_rech_by_channel.subs_lacci_id,
            dfj_tsf_rs_lacci_filled.trx_date_mkios == df_urp_rech_by_channel.trx_date,
        ],
        how="left",
    )

    fea_columns = [
        "total_recharge_modern_channel",
        "fea_outlets_urp_channel_rech_bank",
        "fea_outlets_urp_channel_rech_bonus",
        "fea_outlets_urp_channel_rech_dealer",
        "fea_outlets_urp_channel_rech_device",
        "fea_outlets_urp_channel_rech_direct",
        "fea_outlets_urp_channel_rech_e-kiosk",
        "fea_outlets_urp_channel_rech_fisik",
        "fea_outlets_urp_channel_rech_international_partner",
        "fea_outlets_urp_channel_rech_modern_channel",
        "fea_outlets_urp_channel_rech_null",
        "fea_outlets_urp_channel_rech_online",
        "fea_outlets_urp_channel_rech_project",
        "fea_outlets_urp_channel_rech_retail_nasional",
    ]

    for column in fea_columns:
        if column not in df_modern_channel_by_subs_lacci_id.columns:
            df_modern_channel_by_subs_lacci_id = df_modern_channel_by_subs_lacci_id.withColumn(
                column, f.lit(0).cast(t.DecimalType(38, 2))
            )
        else:
            df_modern_channel_by_subs_lacci_id = df_modern_channel_by_subs_lacci_id.withColumn(
                column, f.col(column).cast(t.DecimalType(38, 2))
            )

    # Grouping by rs_lacci_id (taken from mkios) and aggregating all channel based recharges
    # and summing total recharges
    df_modern_channel_recharge = df_modern_channel_by_subs_lacci_id.groupBy(
        f.col("rs_lacci_id_mkios").alias("location_bts"),
        f.col("trx_date_mkios").alias("trx_date"),
    ).agg(
        f.coalesce(f.sum("total_recharge_modern_channel"), f.lit(0)).alias(
            "total_recharge_modern_channel"
        ),
        f.coalesce(f.sum("fea_outlets_urp_channel_rech_bank"), f.lit(0)).alias(
            "fea_outlets_urp_channel_rech_bank"
        ),
        f.coalesce(f.sum("fea_outlets_urp_channel_rech_bonus"), f.lit(0)).alias(
            "fea_outlets_urp_channel_rech_bonus"
        ),
        f.coalesce(f.sum("fea_outlets_urp_channel_rech_dealer"), f.lit(0)).alias(
            "fea_outlets_urp_channel_rech_dealer"
        ),
        f.coalesce(f.sum("fea_outlets_urp_channel_rech_device"), f.lit(0)).alias(
            "fea_outlets_urp_channel_rech_device"
        ),
        f.coalesce(f.sum("fea_outlets_urp_channel_rech_direct"), f.lit(0)).alias(
            "fea_outlets_urp_channel_rech_direct"
        ),
        f.coalesce(f.sum("fea_outlets_urp_channel_rech_e-kiosk"), f.lit(0)).alias(
            "fea_outlets_urp_channel_rech_e_kiosk"
        ),
        f.coalesce(f.sum("fea_outlets_urp_channel_rech_fisik"), f.lit(0)).alias(
            "fea_outlets_urp_channel_rech_fisik"
        ),
        f.coalesce(
            f.sum("fea_outlets_urp_channel_rech_international_partner"), f.lit(0)
        ).alias("fea_outlets_urp_channel_rech_international_partner"),
        f.coalesce(
            f.sum("fea_outlets_urp_channel_rech_modern_channel"), f.lit(0)
        ).alias("fea_outlets_urp_channel_rech_modern_channel_as_channel"),
        f.coalesce(f.sum("fea_outlets_urp_channel_rech_null"), f.lit(0)).alias(
            "fea_outlets_urp_channel_rech_null"
        ),
        f.coalesce(f.sum("fea_outlets_urp_channel_rech_online"), f.lit(0)).alias(
            "fea_outlets_urp_channel_rech_online"
        ),
        f.coalesce(f.sum("fea_outlets_urp_channel_rech_project"), f.lit(0)).alias(
            "fea_outlets_urp_channel_rech_project"
        ),
        f.coalesce(
            f.sum("fea_outlets_urp_channel_rech_retail_nasional"), f.lit(0)
        ).alias("fea_outlets_urp_channel_rech_retail_nasional"),
    )
    return df_modern_channel_recharge


def calc_recharges_by_modern_channel(
    df_mkios: pyspark.sql.DataFrame, df_urp: pyspark.sql.DataFrame
) -> None:

    conf_catalog = get_config_parameters(config="catalog")
    start_date = get_start_date(period="1cm")
    end_date = get_end_date(period="1cm")

    # ------------------------------- Output Catalog Entries ------------------------------------ #
    daily_agg_catalog = conf_catalog["l1_modern_daily_aggregated"]

    save_args = daily_agg_catalog["save_args"]
    save_args.pop("partitionBy", None)

    file_path = get_file_path(filepath=daily_agg_catalog["filepath"])
    file_format = daily_agg_catalog["file_format"]
    partitions = int(daily_agg_catalog["partitions"])

    log.info(
        "Starting Daily Aggregation for Dates {start_date} to {end_date}".format(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
        )
    )

    log.info(f"File Path: {file_path}")
    log.info(f"File Format: {file_format}")
    log.info(f"Save Args: {save_args}")
    log.info(f"Partitions: {partitions}")

    df = _agg_recharges_by_modern_channel_to_daily(df_mkios, df_urp)
    df.cache()

    while start_date <= end_date:
        event_date = start_date.strftime("%Y-%m-%d")
        log.info("Starting Daily Aggregation for Day: {}".format(event_date))

        df_filtered = df.filter(f.col("trx_date") == event_date).drop("trx_date")

        partition_file_path = "{file_path}/trx_date={event_date}".format(
            file_path=file_path, event_date=event_date
        )
        df_filtered.repartition(numPartitions=partitions).write.save(
            partition_file_path, file_format, **save_args
        )

        log.info("Completed Daily Aggregation for Day: {}".format(event_date))
        start_date += timedelta(days=1)

    df.unpersist()


def _agg_modern_channel_recharges_for_all_lacci_to_daily(
    dfj_mkios_digipos: pyspark.sql.DataFrame, df_modern: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:

    df_modern = df_modern.withColumnRenamed("location_bts", "rs_lacci_id")
    df_modern_min_max_dates = df_modern.groupBy("rs_lacci_id", "trx_date").agg(
        f.min("trx_date").alias("min_trx_date")
    )

    dfj_mkios_digipos_min_max_dates = (
        dfj_mkios_digipos.withColumn(
            "trx_date", f.date_format("trx_date", "yyyy-MM-dd")
        )
        .groupBy("rs_lacci_id", "trx_date")
        .agg(
            f.min("trx_date").alias("min_trx_date"),
            f.min("created_at").alias("min_created_at"),
        )
        .withColumn("min_trx_date", f.least("min_trx_date", "min_created_at"))
        .drop("min_created_at")
    )

    current_date = get_end_date()
    current_date = current_date.strftime("%Y-%m-%d")
    df_final_min_max_dates = (
        df_modern_min_max_dates.union(dfj_mkios_digipos_min_max_dates)
        .groupBy("rs_lacci_id")
        .agg(
            f.min("min_trx_date").alias("min_trx_date"),
            f.lit(current_date).alias("max_trx_date"),
        )
    )

    df_all_lacci_for_outlets = (
        dfj_mkios_digipos.select("outlet_id", "rs_lacci_id")
        .distinct()
        .filter(f.col("rs_lacci_id").isNotNull())
    )

    df_min_max_for_scaffold = df_all_lacci_for_outlets.join(
        df_final_min_max_dates, ["rs_lacci_id"]
    )
    dfj_min_max_for_scaffold = (
        df_min_max_for_scaffold.withColumn("digipos_start_date", f.lit("2019-06-01"))
        .withColumn("min_trx_date", f.greatest("digipos_start_date", "min_trx_date"))
        .select("outlet_id", "rs_lacci_id", "max_trx_date", "min_trx_date")
    )

    df_daily_scaffold = create_lacci_scaffold(
        dfj_min_max_for_scaffold, ["outlet_id", "rs_lacci_id", "trx_date"]
    )
    dfj_modern_tsf = df_daily_scaffold.join(
        df_modern, ["rs_lacci_id", "trx_date"], how="inner"
    )
    df_modern_channel_recharges_all = dfj_modern_tsf.groupBy(
        "outlet_id", "trx_date"
    ).agg(
        f.coalesce(f.sum("total_recharge_modern_channel"), f.lit(0)).alias(
            "fea_outlets_all_recharges_modern_channel"
        )
    )
    return df_modern_channel_recharges_all


def calc_modern_channel_recharges_for_all_lacci(
    dfj_mkios_digipos: pyspark.sql.DataFrame, df_modern: pyspark.sql.DataFrame
) -> None:

    conf_catalog = get_config_parameters(config="catalog")
    start_date = get_start_date(period="1cm")
    end_date = get_end_date(period="1cm")

    # ------------------------------- Input Catalog Entries ------------------------------------ #
    load_args_mkios = conf_catalog["l1_dfj_digipos_mkios"]["load_args"]
    load_args_modern = conf_catalog["l1_modern_daily_aggregated"]["load_args"]

    # ------------------------------- Output Catalog Entries ------------------------------------ #
    daily_agg_catalog = conf_catalog["l1_total_modern_channel_recharges"]

    save_args = daily_agg_catalog["save_args"]
    save_args.pop("partitionBy", None)

    file_path = get_file_path(filepath=daily_agg_catalog["filepath"])
    file_format = daily_agg_catalog["file_format"]
    partitions = int(daily_agg_catalog["partitions"])

    log.info(
        "Starting Daily Aggregation for Dates {start_date} to {end_date}".format(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
        )
    )

    log.info(f"File Path: {file_path}")
    log.info(f"File Format: {file_format}")
    log.info(f"Load Args: {load_args_mkios}")
    log.info(f"Save Args: {save_args}")
    log.info(f"Partitions: {partitions}")

    while start_date <= end_date:
        event_date = start_date.strftime("%Y-%m-%d")
        current_date_mkios = start_date.strftime(
            load_args_mkios["partition_date_format"]
        )
        current_date_modern = start_date.strftime(
            load_args_modern["partition_date_format"]
        )

        log.info("Starting Daily Aggregation for Day: {}".format(event_date))

        DATE_COLUMN = "trx_date"
        df_mkios_digipos_filtered = dfj_mkios_digipos.filter(
            f.col(DATE_COLUMN) == current_date_mkios
        )
        df_modern_filtered = df_modern.filter(f.col(DATE_COLUMN) == current_date_modern)

        df = _agg_modern_channel_recharges_for_all_lacci_to_daily(
            df_mkios_digipos_filtered, df_modern_filtered
        )
        df = df.drop("trx_date")

        partition_file_path = "{file_path}/trx_date={event_date}".format(
            file_path=file_path, event_date=event_date
        )

        df.repartition(numPartitions=partitions).write.save(
            partition_file_path, file_format, **save_args
        )

        log.info("Completed Daily Aggregation for Day: {}".format(event_date))
        start_date += timedelta(days=1)


def _get_pv_stats_by_lacci_city_rs_msisdn(
    df_mkios, df_digipos_cleaned, df_pv_inj, fea_prefix
) -> List[pyspark.sql.DataFrame]:
    dfj_pv_inj_mkios = df_pv_inj.join(
        df_mkios, ["outlet_msisdn", "trx_date", "reseller_lacci"], how="inner"
    )
    df_pv_stats_by_lacci = dfj_pv_inj_mkios.groupBy("rs_lacci_id", "trx_date").agg(
        f.sum("nominal").alias("nominal_by_lacci"),
        f.collect_set("serial_number").alias("trx_by_lacci"),
    )

    df_mkios_dist = df_mkios.select(
        "outlet_msisdn", "rs_lacci_id", "trx_date"
    ).distinct()
    dfj_mkios_dist_pv_lacci = df_mkios_dist.join(
        df_pv_stats_by_lacci, ["rs_lacci_id", "trx_date"], how="full"
    )
    df_stats_by_outlet = dfj_mkios_dist_pv_lacci.join(
        df_digipos_cleaned, ["outlet_msisdn"]
    )
    df_pv_lacci_stats_by_outlet = df_stats_by_outlet.groupBy(
        "outlet_id", "trx_date"
    ).agg(
        f.sum("nominal_by_lacci").alias(fea_prefix + "sum_nominal_by_lacci"),
        f.size(f.array_distinct(f.flatten(f.collect_set("trx_by_lacci")))).alias(
            fea_prefix + "tot_trx_by_lacci"
        ),
    )

    df_pv_inj_grpby_city = df_pv_inj.groupBy("reseller_city", "trx_date").agg(
        f.sum("nominal").alias(fea_prefix + "sum_nominal_by_reseller_city"),
        f.countDistinct("serial_number").alias(fea_prefix + "trx_by_reseller_city"),
    )

    df_pv_inj_grpby_rs_msisdn = df_pv_inj.groupBy("outlet_msisdn", "trx_date").agg(
        f.sum("nominal").alias("sum_nominal_by_rs_msisdn"),
        f.collect_set("serial_number").alias("pv_inj_trx_by_rs_msisdn"),
    )

    df_pv_rs_msisdn_stats = df_pv_inj_grpby_rs_msisdn.join(
        df_digipos_cleaned, ["outlet_msisdn"]
    )
    df_pv_rs_msisdn_stats = df_pv_rs_msisdn_stats.groupby("outlet_id", "trx_date").agg(
        f.sum("sum_nominal_by_rs_msisdn").alias(
            fea_prefix + "sum_nominal_by_rs_msisdn"
        ),
        f.size(
            f.array_distinct(f.flatten(f.collect_set("pv_inj_trx_by_rs_msisdn")))
        ).alias(fea_prefix + "trx_by_rs_msisdn"),
    )

    return [df_pv_lacci_stats_by_outlet, df_pv_inj_grpby_city, df_pv_rs_msisdn_stats]


def calc_physical_voucher_inj(
    df_claudia_enable: pyspark.sql.DataFrame,
    df_mkios: pyspark.sql.DataFrame,
    df_outlet_id_msisdn_mapping: pyspark.sql.DataFrame,
) -> List[pyspark.sql.DataFrame]:

    df_claudia_enable.cache()
    df_outlet_id_msisdn_mapping.cache()
    df_mkios.cache()

    cols_of_interest = [
        "outlet_msisdn",
        "trx_date",
        "reseller_city",
        "reseller_lacci",
        "nominal",
        "serial_number",
    ]
    df_claudia_enable = df_claudia_enable.withColumn(
        "trx_date", f.date_format("trx_date", "yyyy-MM-dd")
    )
    df_claudia_enable = df_claudia_enable.withColumn(
        "reseller_city", f.regexp_replace("reseller_city", "\\+", " ")
    )

    df_pv_inj = (
        df_claudia_enable.filter(f.col("voucher_status") == 1)
        .withColumnRenamed("reseller_msisdn", "outlet_msisdn")
        .select(cols_of_interest)
    )

    df_mkios = (
        df_mkios.withColumn(
            "reseller_lacci", f.regexp_replace("rs_lacci_id", "\\|", "")
        )
        .select(
            f.col("rs_msisdn").alias("outlet_msisdn"),
            "trx_date",
            "reseller_lacci",
            "rs_lacci_id",
        )
        .distinct()
    )

    df_pv_inj = _get_pv_stats_by_lacci_city_rs_msisdn(
        df_mkios,
        df_outlet_id_msisdn_mapping,
        df_pv_inj,
        fea_prefix="fea_outlets_pv_inj_",
    )

    pv_inj_lacci_outlet_stats = df_pv_inj[0]
    pv_inj_city_stats = df_pv_inj[1]
    pv_inj_rs_msisdn_outlet_stats = df_pv_inj[2]

    return [pv_inj_lacci_outlet_stats, pv_inj_city_stats, pv_inj_rs_msisdn_outlet_stats]


def calc_physical_voucher_red(
    df_claudia_daily_used: pyspark.sql.DataFrame,
    df_mkios: pyspark.sql.DataFrame,
    df_outlet_id_msisdn_mapping: pyspark.sql.DataFrame,
) -> List[pyspark.sql.DataFrame]:

    df_claudia_daily_used.cache()
    df_outlet_id_msisdn_mapping.cache()
    df_mkios.cache()

    cols_of_interest = [
        "outlet_msisdn",
        "trx_date",
        "reseller_city",
        "reseller_lacci",
        "nominal",
        "serial_number",
    ]
    df_pv_red = (
        df_claudia_daily_used.filter(f.col("voucher_status") == 3)
        .withColumnRenamed("reseller_msisdn", "outlet_msisdn")
        .select(cols_of_interest)
    )

    df_mkios = (
        df_mkios.withColumn(
            "reseller_lacci", f.regexp_replace("rs_lacci_id", "\\|", "")
        )
        .select(
            f.col("rs_msisdn").alias("outlet_msisdn"),
            "trx_date",
            "reseller_lacci",
            "rs_lacci_id",
        )
        .distinct()
    )

    df_pv_red = _get_pv_stats_by_lacci_city_rs_msisdn(
        df_mkios,
        df_outlet_id_msisdn_mapping,
        df_pv_red,
        fea_prefix="fea_outlets_pv_red_",
    )

    pv_red_lacci_outlet_stats = df_pv_red[0]
    pv_red_city_stats = df_pv_red[1]
    pv_red_rs_msisdn_outlet_stats = df_pv_red[2]

    return [pv_red_lacci_outlet_stats, pv_red_city_stats, pv_red_rs_msisdn_outlet_stats]


def _aggregate_rech_to_daily(dfj_mkios_digipos) -> List[pyspark.sql.DataFrame]:
    DATE_COLUMN = "trx_date"
    df_mkios_get_counts_for_mode_calc = dfj_mkios_digipos.groupBy(
        "outlet_id",
        DATE_COLUMN,
        f.col("denom").alias("denomination"),
        f.col("rech").alias("recharge_mkios"),
        f.col("rs_lacci_id").alias("location_bts"),
    ).agg(
        f.sum("trx_rech").alias("total_trx_rech"),
        f.count("rech").alias("count_rech_mkios"),
    )

    df_mkios_daily_agg = dfj_mkios_digipos.groupBy("outlet_id", DATE_COLUMN).agg(
        f.collect_set("outlet_msisdn").alias("distinct_rs_msisdns"),
        f.sum("rech").alias("total_cashflow_mkios"),
        f.coalesce(
            f.sum(f.when(f.col("split_code") == "001", f.col("rech"))), f.lit(0)
        ).alias("total_recharge_mkios"),
        f.coalesce(
            f.sum(f.when(f.col("split_code") != "001", f.col("rech"))), f.lit(0)
        ).alias("total_revenue_mkios"),
        f.avg("rech").alias("mean_cashflow_mkios"),
        f.expr("percentile_approx(rech, 0.5)").alias("median_cashflow_mkios"),
        f.sum("trx_rech").alias("total_trx_cashflow_mkios"),
        f.coalesce(
            f.sum(f.when(f.col("split_code") == "001", f.col("trx_rech"))), f.lit(0)
        ).alias("total_trx_recharge_mkios"),
        f.coalesce(
            f.sum(f.when(f.col("split_code") != "001", f.col("trx_rech"))), f.lit(0)
        ).alias("total_trx_revenue_mkios"),
        f.expr("percentile_approx(denom, 0.5)").alias("median_denomination_recharge"),
    )
    return [df_mkios_get_counts_for_mode_calc, df_mkios_daily_agg]


def agg_recharge_mkios_for_features(dfj_mkios_digipos: pyspark.sql.DataFrame) -> None:
    """
    Calculates Recharge aggregated columns on daily level.
    :param dfj_mkios_digipos: Joined table mkios & digipos mapping table
    :return df_rech_mkios_modern
    """
    conf_catalog = get_config_parameters(config="catalog")
    start_date = get_start_date(period="1cm")
    end_date = get_end_date(period="1cm")

    # ------------------------------- Input Catalog Entries ------------------------------------ #
    load_args = conf_catalog["l1_dfj_digipos_mkios"]["load_args"]

    # ------------------------------- Output Catalog Entries ------------------------------------ #
    l1_mkios_recharge_daily_aggregated = conf_catalog[
        "l1_mkios_recharge_daily_aggregated"
    ]
    l1_mkios_recharge_daily_mode_aggregated = conf_catalog[
        "l1_mkios_recharge_daily_mode_aggregated"
    ]

    save_args_rech_daily_agg = l1_mkios_recharge_daily_aggregated["save_args"]
    save_args_rech_daily_mode_agg = l1_mkios_recharge_daily_mode_aggregated["save_args"]
    save_args_rech_daily_agg.pop("partitionBy", None)
    save_args_rech_daily_mode_agg.pop("partitionBy", None)

    file_path_rech_daily_agg = get_file_path(
        filepath=l1_mkios_recharge_daily_aggregated["filepath"]
    )
    file_format_rech_daily_agg = l1_mkios_recharge_daily_aggregated["file_format"]
    partitions_rech_daily_agg = int(l1_mkios_recharge_daily_aggregated["partitions"])

    file_path_rech_daily_mode_agg = get_file_path(
        filepath=l1_mkios_recharge_daily_mode_aggregated["filepath"]
    )
    file_format_rech_daily_mode_agg = l1_mkios_recharge_daily_mode_aggregated[
        "file_format"
    ]
    partitions_rech_daily_mode_agg = int(
        l1_mkios_recharge_daily_mode_aggregated["partitions"]
    )

    log.info(f"File Path: {file_path_rech_daily_agg}")
    log.info(f"File Format: {file_format_rech_daily_agg}")
    log.info(f"Load Args: {load_args}")
    log.info(f"Save Args: {save_args_rech_daily_agg}")
    log.info(f"Partitions: {partitions_rech_daily_agg}")

    log.info(f"File Path: {file_path_rech_daily_mode_agg}")
    log.info(f"File Format: {file_format_rech_daily_mode_agg}")
    log.info(f"Load Args: {load_args}")
    log.info(f"Save Args: {save_args_rech_daily_mode_agg}")
    log.info(f"Partitions: {partitions_rech_daily_mode_agg}")

    while start_date <= end_date:
        current_date = start_date.strftime(load_args["partition_date_format"])
        log.info("Starting Daily Aggregation for Day: {}".format(current_date))

        DATE_COLUMN = "trx_date"
        dfj_mkios_digipos_filtered = dfj_mkios_digipos.filter(
            f.col(DATE_COLUMN) == current_date
        )

        (
            df_mkios_get_counts_for_mode_calc,
            df_mkios_daily_agg,
        ) = _aggregate_rech_to_daily(dfj_mkios_digipos_filtered)

        rech_daily_agg_partition_file_path = "{file_path}/trx_date={current_date}".format(
            file_path=file_path_rech_daily_agg, current_date=current_date
        )

        rech_mode_daily_agg_partition_file_path = "{file_path}/trx_date={current_date}".format(
            file_path=file_path_rech_daily_mode_agg, current_date=current_date
        )

        df_mkios_get_counts_for_mode_calc.repartition(
            numPartitions=partitions_rech_daily_mode_agg
        ).write.save(
            rech_mode_daily_agg_partition_file_path,
            file_format_rech_daily_mode_agg,
            **save_args_rech_daily_mode_agg,
        )
        df_mkios_daily_agg.repartition(
            numPartitions=partitions_rech_daily_agg
        ).write.save(
            rech_daily_agg_partition_file_path,
            file_format_rech_daily_agg,
            **save_args_rech_daily_agg,
        )

        log.info("Completed Daily Aggregation for Day: {}".format(start_date))
        start_date += timedelta(days=1)
