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
from typing import Any, Dict, List

import pyspark
import pyspark.sql.functions as f

from utils import (
    get_config_parameters,
    get_end_date,
    get_outlet_ids_to_exclude,
    get_start_date,
)
from utils.spark_data_set_helper import get_file_path

log = logging.getLogger(__name__)


def get_filter_names() -> Dict[Any, str]:
    filters = dict()
    multiple_latlong = "DE_multiple_lat_long"
    multiple_types = "DE_multiple_type"
    personal_resellers = "DE_personal_resellers"
    physical_voucher_inj = "DE_physical_voucher_injections"
    rs_msisdn_to_multiple_outlet_id = "DE_rs_msisdns_attached_to_multiple_outlet_id"
    filters["multiple_latlong"] = multiple_latlong
    filters["personal_resellers"] = personal_resellers
    filters["physical_voucher_inj"] = physical_voucher_inj
    filters["rs_msisdn_to_multiple_outlet_id"] = rs_msisdn_to_multiple_outlet_id
    filters["multiple_type"] = multiple_types
    return filters


def _remove_outlets_with_quality_issues(
    df_outlet_dim_dd: pyspark.sql.DataFrame, df_digipos_ref: pyspark.sql.DataFrame
) -> Dict[Any, pyspark.sql.DataFrame]:

    df_digipos_ref = df_digipos_ref.filter(
        (f.col("outlet_id").isNotNull())
        & (f.col("no_rs").isNotNull())
        & (f.col("outlet_id") != "")
        & (f.col("no_rs") != "")
        & (f.lower(f.col("outlet_id")) != "jabar")
    )

    df_outlet_dim_dd = df_outlet_dim_dd.withColumnRenamed(
        "lattitude", "latitude"
    ).withColumn("latlong", f.concat(f.col("latitude"), f.lit("|"), f.col("longitude")))

    df_outlets_with_multiple_latlong = (
        df_outlet_dim_dd.groupBy("outlet_id")
        .agg(f.countDistinct("latlong").alias("latlong_count"),)
        .filter(f.col("latlong_count") > 1)
    )

    df_outlets_with_multiple_type = (
        df_outlet_dim_dd.groupBy("outlet_id")
        .agg(f.countDistinct("tipe_outlet").alias("type_count"),)
        .filter(f.col("type_count") > 1)
    )

    df_personal_resellers = (
        df_digipos_ref.filter(f.col("outlet_id").startswith("6"))
        .select("outlet_id")
        .distinct()
    )

    df_physical_vouchers = (
        df_digipos_ref.filter(f.col("outlet_id").isin(*get_outlet_ids_to_exclude()))
        .select("outlet_id")
        .distinct()
    )

    df_outlet_ids_for_no_rs_with_multiple_outlet = (
        df_digipos_ref.groupBy("no_rs")
        .agg(f.collect_set("outlet_id").alias("outlet_id_set"))
        .withColumn("outlet_count", f.size("outlet_id_set"))
        .filter(f.col("outlet_count") > 1)
        .withColumn("outlet_id", f.explode("outlet_id_set"))
        .select("outlet_id")
        .distinct()
    )

    df_digipos_cleaned = (
        df_digipos_ref.join(
            df_outlets_with_multiple_latlong, ["outlet_id"], how="left_anti"
        )
        .join(df_outlets_with_multiple_type, ["outlet_id"], how="left_anti")
        .join(df_personal_resellers, ["outlet_id"], how="left_anti")
        .join(df_physical_vouchers, ["outlet_id"], how="left_anti")
        .join(
            df_outlet_ids_for_no_rs_with_multiple_outlet, ["outlet_id"], how="left_anti"
        )
    )

    df_digipos_cleaned = (
        df_digipos_cleaned.withColumn(
            "outlet_msisdn", f.concat(f.lit("62"), f.col("no_rs"))
        )
        .withColumn(
            "created_at",
            f.date_format(f.to_date("created_at", "dd-MMM-yy"), "yyyy-MM-dd"),
        )
        .groupBy("outlet_id", "outlet_msisdn")
        .agg(f.min("created_at").alias("created_at"))
    )

    return {
        "digipos_cleaned": df_digipos_cleaned,
        "multiple_latlong": df_outlets_with_multiple_latlong,
        "multiple_type": df_outlets_with_multiple_type,
        "personal_resellers": df_personal_resellers,
        "physical_vouchers": df_physical_vouchers,
        "no_rs_to_multiple_outlet": df_outlet_ids_for_no_rs_with_multiple_outlet,
    }


def _daily_aggregation(
    df_outlet_dim_dd: pyspark.sql.DataFrame,
    df_outlets_with_multiple_latlong: pyspark.sql.DataFrame,
    df_outlets_with_multiple_type: pyspark.sql.DataFrame,
    df_personal_resellers: pyspark.sql.DataFrame,
    df_physical_vouchers: pyspark.sql.DataFrame,
    df_outlet_ids_for_no_rs_with_multiple_outlet: pyspark.sql.DataFrame,
) -> List[pyspark.sql.DataFrame]:
    """
    Gets mapping table and daily aggregated outlet level table
    :param df_outlet_dim_dd: Outlet raw table
    :returns outlet_df_month, outlet_df_cleaned
    """
    df_outlet_dim_dd = df_outlet_dim_dd.filter(
        (f.col("outlet_id").isNotNull())
        & (f.col("outlet_id") != "")
        & (f.lower(f.col("outlet_id")) != "jabar")
    )
    log.info(
        "0.Initial Count: "
        + str(df_outlet_dim_dd.select("outlet_id").distinct().count())
    )

    log.info(
        "1.Outlets with Multiple Latlong: "
        + str(df_outlets_with_multiple_latlong.count())
    )

    df_outlet_dim_dd = df_outlet_dim_dd.join(
        df_outlets_with_multiple_latlong, ["outlet_id"], how="left_anti"
    )

    df_matched_multiple_type = df_outlet_dim_dd.join(
        df_outlets_with_multiple_type, ["outlet_id"], how="inner"
    )
    df_matched_multiple_type = df_matched_multiple_type.select("outlet_id").distinct()

    log.info("1.1.Outlets with Multiple Type: " + str(df_matched_multiple_type.count()))

    df_outlet_dim_dd = df_outlet_dim_dd.join(
        df_matched_multiple_type, ["outlet_id"], how="left_anti"
    )

    df_match_personal_resellers = (
        df_outlet_dim_dd.join(df_personal_resellers, ["outlet_id"])
        .select("outlet_id")
        .distinct()
    )

    log.info("2.Personal Resellers: " + str(df_match_personal_resellers.count()))

    df_outlet_dim_dd = df_outlet_dim_dd.join(
        df_match_personal_resellers, ["outlet_id"], how="left_anti"
    )

    df_matched_physical_vouchers = (
        df_outlet_dim_dd.join(df_physical_vouchers, ["outlet_id"])
        .select("outlet_id")
        .distinct()
    )

    log.info("3.Physical Vouchers: " + str(df_matched_physical_vouchers.count()))

    df_outlet_dim_dd = df_outlet_dim_dd.join(
        df_matched_physical_vouchers, ["outlet_id"], how="left_anti"
    )

    df_matched_no_rs_to_multiple = (
        df_outlet_dim_dd.join(
            df_outlet_ids_for_no_rs_with_multiple_outlet, ["outlet_id"]
        )
        .select("outlet_id")
        .distinct()
    )

    log.info("4.no_rs to Multiple: " + str(df_matched_no_rs_to_multiple.count()))

    df_outlet_dim_dd = df_outlet_dim_dd.join(
        df_matched_no_rs_to_multiple, ["outlet_id"], how="left_anti"
    )

    log.info(
        "5.Final Count: " + str(df_outlet_dim_dd.select("outlet_id").distinct().count())
    )

    all_filter_names = get_filter_names()
    df_all_filters = (
        df_outlets_with_multiple_latlong.select("outlet_id")
        .withColumn("filter_type", f.lit(all_filter_names["multiple_latlong"]))
        .union(
            df_match_personal_resellers.withColumn(
                "filter_type", f.lit(all_filter_names["personal_resellers"])
            )
        )
        .union(
            df_outlets_with_multiple_type.select("outlet_id").withColumn(
                "filter_type", f.lit(all_filter_names["multiple_type"])
            )
        )
        .union(
            df_matched_physical_vouchers.withColumn(
                "filter_type", f.lit(all_filter_names["physical_voucher_inj"])
            )
        )
        .union(
            df_matched_no_rs_to_multiple.withColumn(
                "filter_type",
                f.lit(all_filter_names["rs_msisdn_to_multiple_outlet_id"]),
            )
        )
        .withColumn("run_date_time", f.current_timestamp())
    )

    df_outlet_dim_dd_cleaned = (
        df_outlet_dim_dd.withColumn("trx_date", f.to_date("trx_date", "yyyyMMdd"))
        .withColumn(
            "classification",
            f.when(
                (f.col("klasifikasi") == "") | f.col("klasifikasi").isNull(), None
            ).otherwise(f.col("klasifikasi")),
        )
        .withColumn(
            "frequency_visit",
            f.when(
                (f.col("jadwal_kunjungan") == "") | f.col("jadwal_kunjungan").isNull(),
                None,
            ).otherwise(f.col("jadwal_kunjungan")),
        )
        .withColumn(
            "total_rech",
            f.when(
                ((f.col("total_rech") == 0) | (f.col("total_rech").isNull())), None
            ).otherwise(f.col("total_rech")),
        )
        .withColumn(
            "location_longlat",
            f.concat(f.col("longitude"), f.lit("|"), f.col("lattitude")),
        )
        .withColumn(
            "location_longlat",
            f.when(
                (f.col("location_longlat") == "") | f.col("location_longlat").isNull(),
                None,
            ).otherwise(f.col("location_longlat")),
        )
        .withColumn(
            "created_at",
            f.to_date(f.unix_timestamp("created_at", "dd-MMM-yy").cast("timestamp")),
        )
    )

    df_outlet_digipos_stats = df_outlet_dim_dd_cleaned.select(
        "outlet_id",
        "trx_date",
        f.col("kabupaten").alias("city"),
        f.col("kategori").alias("category"),
        f.col("tipe_outlet").alias("type"),
        f.col("tipe_lokasi").alias("location_type"),
        f.col("lattitude").alias("latitude"),
        "longitude",
        "created_at",
        "area",
        "frequency_visit",
        "location_longlat",
        "classification",
        f.col("regional").alias("region"),
        f.col("total_rech").alias("total_rech_digipos"),
        f.col("total_trx_rech").alias("total_trx_rech_digipos"),
        f.col("total_rev_package").alias("total_rev_package_digipos"),
        f.col("total_trx_package").alias("total_trx_package_digipos"),
        f.col("rev_package_akuisisi").alias("total_rev_package_akuisisi_digipos"),
        f.col("rev_package_voice").alias("total_rev_package_voice_digipos"),
        f.col("rev_package_sms").alias("total_rev_package_sms_digipos"),
        f.col("rev_package_bulk").alias("total_rev_package_bulk_digipos"),
        f.col("rev_package_fix").alias("total_rev_package_fix_digipos"),
        f.col("rev_package_hvc").alias("total_rev_package_hvc_digipos"),
        f.col("rev_package_haji").alias("total_rev_package_haji_digipos"),
    )

    return [df_outlet_digipos_stats, df_all_filters]


def agg_outlet_digipos_daily(
    df_outlet_dim_dd: pyspark.sql.DataFrame, df_digipos_ref: pyspark.sql.DataFrame
) -> None:

    conf_catalog = get_config_parameters(config="catalog")
    start_date = get_start_date(period="1cm")
    end_date = get_end_date(period="1cm")

    df_digipos_ref = df_digipos_ref.filter(
        f.col("event_date") <= end_date.strftime("%Y-%m-%d")
    )
    df_outlet_dim_dd.cache()
    df_digipos_ref.cache()

    node_output_catalog_entry_digipos = conf_catalog[
        "l1_outlet_digipos_daily_aggregated"
    ]
    node_output_catalog_entry_outlet_id_msisdn_mapping = conf_catalog[
        "l1_outlet_id_msisdn_mapping"
    ]

    node_output_catalog_entry_all_filters = conf_catalog["l1_all_filters"]

    save_args_digipos = node_output_catalog_entry_digipos["save_args"]
    save_args_outlet_id_msisdn_mapping = node_output_catalog_entry_outlet_id_msisdn_mapping[
        "save_args"
    ]
    save_args_filters = node_output_catalog_entry_all_filters["save_args"]

    partitions_digipos = int(node_output_catalog_entry_digipos["partitions"])
    partitions_outlet_id_msisdn_mapping = int(
        node_output_catalog_entry_outlet_id_msisdn_mapping["partitions"]
    )
    partitions_filters = int(node_output_catalog_entry_all_filters["partitions"])

    save_args_digipos.pop("partitionBy", None)
    save_args_outlet_id_msisdn_mapping.pop("partitionBy", None)
    save_args_filters.pop("partitionBy", None)

    file_path_digipos_daily_agg = get_file_path(
        filepath=node_output_catalog_entry_digipos["filepath"]
    )
    file_format_digipos_daily_agg = node_output_catalog_entry_digipos["file_format"]

    file_path_outlet_id_msisdn_mapping = get_file_path(
        filepath=node_output_catalog_entry_outlet_id_msisdn_mapping["filepath"]
    )
    file_format_outlet_id_msisdn_mapping = node_output_catalog_entry_outlet_id_msisdn_mapping[
        "file_format"
    ]

    file_path_filters = get_file_path(
        filepath=node_output_catalog_entry_all_filters["filepath"]
    )
    file_format_filters = node_output_catalog_entry_all_filters["file_format"]

    load_args_outlet_dim_dd = conf_catalog["l1_outlet_dim_dd"]["load_args"]

    log.info(
        "Starting Daily Aggregation for Dates {start_date} to {end_date}".format(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
        )
    )

    log.info(f"File Path: #1")
    log.info(f"File Path: {file_path_digipos_daily_agg}")
    log.info(f"File Format: {file_format_digipos_daily_agg}")
    log.info(f"Save Args: {save_args_digipos}")

    log.info(f"File Path: #2")
    log.info(f"File Path: {file_path_outlet_id_msisdn_mapping}")
    log.info(f"File Format: {file_format_outlet_id_msisdn_mapping}")
    log.info(f"Load Args: {load_args_outlet_dim_dd}")
    log.info(f"Save Args: {save_args_outlet_id_msisdn_mapping}")

    results = _remove_outlets_with_quality_issues(df_outlet_dim_dd, df_digipos_ref)
    df_digipos_cleaned = results["digipos_cleaned"]
    df_outlets_with_multiple_latlong = results["multiple_latlong"]
    df_outlets_with_multiple_type = results["multiple_type"]
    df_personal_resellers = results["personal_resellers"]
    df_physical_vouchers = results["physical_vouchers"]
    df_outlet_ids_for_no_rs_with_multiple_outlet = results["no_rs_to_multiple_outlet"]

    df_outlet_digipos_stats, df_all_filters = _daily_aggregation(
        df_outlet_dim_dd,
        df_outlets_with_multiple_latlong,
        df_outlets_with_multiple_type,
        df_personal_resellers,
        df_physical_vouchers,
        df_outlet_ids_for_no_rs_with_multiple_outlet,
    )

    df_digipos_cleaned.repartition(
        numPartitions=partitions_outlet_id_msisdn_mapping
    ).write.save(
        file_path_outlet_id_msisdn_mapping,
        file_format_outlet_id_msisdn_mapping,
        **save_args_outlet_id_msisdn_mapping,
    )

    df_all_filters = df_all_filters.withColumn(
        "start_date", f.lit(start_date.strftime("%Y-%m-%d"))
    ).withColumn("end_date", f.lit(end_date.strftime("%Y-%m-%d")))

    df_all_filters.repartition(numPartitions=partitions_filters).write.save(
        file_path_filters, file_format_filters, **save_args_filters
    )

    while start_date <= end_date:
        trx_date_str = start_date.strftime("%Y-%m-%d")
        log.info("Writinng Daily Aggregation for Day: {}".format(trx_date_str))

        df_filtered = df_outlet_digipos_stats.filter(
            f.col("trx_date") == trx_date_str
        ).drop("trx_date")

        partition_file_path_digipos_stats = "{file_path}/trx_date={date}".format(
            file_path=file_path_digipos_daily_agg, date=trx_date_str
        )

        df_filtered.repartition(numPartitions=partitions_digipos).write.save(
            partition_file_path_digipos_stats,
            file_format_digipos_daily_agg,
            **save_args_digipos,
        )

        log.info(
            "Completed Writing Daily Aggregation for Day: {}".format(
                start_date.strftime("%Y-%m-%d")
            )
        )
        start_date += timedelta(days=1)
