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
from datetime import datetime
from typing import List

import pyspark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from src.dmp.pipelines.dmp._02_primary.scaffold.create_scaffold import create_date_list
from utils import (
    agg_outlet_mode_first_with_count,
    get_config_parameters,
    get_end_date,
    get_required_output_columns,
    get_start_date,
)
from utils.spark_data_set_helper import get_file_path, get_versioned_save_file_path

log = logging.getLogger(__name__)


def calc_fea_outlet_revenue_daily_training(
    df_outlet: pyspark.sql.DataFrame,
    df_mkios: pyspark.sql.DataFrame,
    df_mkios_mode: pyspark.sql.DataFrame,
    df_modern: pyspark.sql.DataFrame,
    df_pv_inj_lacci_outlet_stats: pyspark.sql.DataFrame,
    df_pv_inj_city_stats: pyspark.sql.DataFrame,
    df_pv_inj_rs_msisdn_outlet_stats: pyspark.sql.DataFrame,
    df_pv_red_lacci_outlet_stats: pyspark.sql.DataFrame,
    df_pv_red_city_stats: pyspark.sql.DataFrame,
    df_pv_red_rs_msisdn_outlet_stats: pyspark.sql.DataFrame,
    df_zone_pricing: pyspark.sql.DataFrame,
    df_total_modern_channel_recharges: pyspark.sql.DataFrame,
    df_hsupa: pyspark.sql.DataFrame,
    df_3g_and_payload: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: List[str],
):
    """
    Gets weekly Aggregation data from outlet and digipos
    :param df_outlet: data weekly aggregation for data outlet with msisdn outlet from digipos
    :param df_mkios: data summary weekly aggregation for data recharge mkios base on msisdn outlet
    :param df_mkios_mode: data weekly modulus for recharge transaction of mkios
    :param df_modern: data weekly transaction of modern channel compare with mkios transaction in bts lacci_id area
    :param df_pv_red_rs_msisdn_outlet_stats: Physical voucher redemption rs_msisdn features (nominal, serial num)
    :param df_pv_red_city_stats: Physical voucher redemption city features (nominal, serial num)
    :param df_pv_red_lacci_outlet_stats: Physical voucher redemption lacci based features (nominal, serial num)
    :param df_pv_inj_rs_msisdn_outlet_stats: Physical voucher injection rs_msisdn based features (nominal, serial num)
    :param df_pv_inj_city_stats: Physical voucher injection city based features (nominal, serial num)
    :param df_pv_inj_lacci_outlet_stats: Physical voucher injection lacci based features (nominal, serial num)
    :param df_zone_pricing: Zone pricing city, month wise
    :param df_total_modern_channel_recharges: Total modern channel recharges taking all lacci at outlet_id, trx_date level
    :param df_hsupa: Network hsupa features
    :param df_3g_and_payload: Network 3g & payload features
    :param required_output_features: Important features list
    :param feature_mode: Toggle feature mode - all/feature_list
    :return: Join all dataframes to form the final master table on outlet_id, trx_date
    """

    # Join data from mode of mkios with data modern channel
    df_calc_outlet_mode_for_loc_bts_denomination = (
        agg_outlet_mode_first_with_count(
            df_mkios_mode,
            col_count="total_trx_rech",
            rolled_col_names=["denomination", "location_bts"],
            primary_columns=["outlet_id", "trx_date"],
            granularity_col="trx_date",
        )
    )["dataframe"]

    df_mode_location_bts_denomination = df_calc_outlet_mode_for_loc_bts_denomination.select(
        "outlet_id",
        "trx_date",
        f.col("denomination_mode").alias(
            "fea_outlet_decimal_most_popular_denomination"
        ),
        f.col("location_bts_mode").alias("location_bts"),
    )

    df_calc_outlet_mode_rech_mkios = (
        agg_outlet_mode_first_with_count(
            df_mkios_mode,
            col_count="count_rech_mkios",
            rolled_col_names=["recharge_mkios"],
            primary_columns=["outlet_id", "trx_date"],
            granularity_col="trx_date",
        )
    )["dataframe"]
    df_mode_rech_mkios = df_calc_outlet_mode_rech_mkios.select(
        "outlet_id",
        "trx_date",
        f.col("recharge_mkios_mode").alias("fea_outlet_decimal_mode_cashflow_mkios"),
    )

    df_mkios_mode_all = df_mode_location_bts_denomination.join(
        df_mode_rech_mkios, ["outlet_id", "trx_date"], how="left_outer"
    ).select(
        "outlet_id",
        "trx_date",
        "fea_outlet_decimal_most_popular_denomination",
        "location_bts",
        "fea_outlet_decimal_mode_cashflow_mkios",
    )

    dfj_mkios_mode_all_modern = (
        df_mkios_mode_all.join(df_modern, ["location_bts", "trx_date"], "left_outer")
        .withColumnRenamed("location_bts", "fea_outlet_string_location_bts")
        .withColumnRenamed(
            "total_recharge_modern_channel",
            "fea_outlet_decimal_total_recharge_modern_channel",
        )
    )

    # Join data from recharge transaction mkios with (mode of mkios and data modern channel)
    dfj_mkios_modern = (
        (
            df_mkios.join(
                dfj_mkios_mode_all_modern, ["outlet_id", "trx_date"], "left_outer"
            )
        )
        .select(
            f.col("outlet_id"),
            "trx_date",
            f.col("distinct_rs_msisdns").alias("fea_distinct_rs_msisdns"),
            f.col("total_cashflow_mkios").alias(
                "fea_outlet_decimal_total_cashflow_mkios"
            ),
            f.col("total_recharge_mkios").alias(
                "fea_outlet_decimal_total_recharge_mkios"
            ),
            f.col("total_revenue_mkios").alias(
                "fea_outlet_decimal_total_revenue_mkios"
            ),
            f.col("mean_cashflow_mkios").alias(
                "fea_outlet_decimal_mean_cashflow_mkios"
            ),
            f.col("median_cashflow_mkios").alias(
                "fea_outlet_decimal_median_cashflow_mkios"
            ),
            f.col("total_trx_cashflow_mkios").alias(
                "fea_outlet_long_total_trx_cashflow_mkios"
            ),
            f.col("total_trx_recharge_mkios").alias(
                "fea_outlet_long_total_trx_recharge_mkios"
            ),
            f.col("total_trx_revenue_mkios").alias(
                "fea_outlet_long_total_trx_revenue_mkios"
            ),
            f.col("median_denomination_recharge").alias(
                "fea_outlet_decimal_median_denomination_recharge"
            ),
            "fea_outlet_decimal_most_popular_denomination",
            "fea_outlet_decimal_mode_cashflow_mkios",
            "fea_outlet_string_location_bts",
            "fea_outlet_decimal_total_recharge_modern_channel",
            "fea_outlets_urp_channel_rech_bank",
            "fea_outlets_urp_channel_rech_bonus",
            "fea_outlets_urp_channel_rech_dealer",
            "fea_outlets_urp_channel_rech_device",
            "fea_outlets_urp_channel_rech_direct",
            "fea_outlets_urp_channel_rech_e_kiosk",
            "fea_outlets_urp_channel_rech_fisik",
            "fea_outlets_urp_channel_rech_international_partner",
            "fea_outlets_urp_channel_rech_modern_channel_as_channel",
            "fea_outlets_urp_channel_rech_null",
            "fea_outlets_urp_channel_rech_online",
            "fea_outlets_urp_channel_rech_project",
            "fea_outlets_urp_channel_rech_retail_nasional",
        )
        .withColumn(
            "fea_outlet_decimal_proportion_recharge_amount_by_cashflow",
            (
                f.col("fea_outlet_decimal_total_recharge_mkios")
                / f.col("fea_outlet_decimal_total_cashflow_mkios")
            ),
        )
        .withColumn(
            "fea_outlet_decimal_proportion_recharge_trx_by_cashflow",
            (
                f.col("fea_outlet_long_total_trx_recharge_mkios")
                / f.col("fea_outlet_long_total_trx_cashflow_mkios")
            ),
        )
    )

    df_zone_pricing = df_zone_pricing.select(
        "city", "month", "ppmb_voucher_old", "ppmb_voucher_new", "ppmb_core", "ppmb_ac"
    ).withColumn("month", f.to_date("month", "yyyyMM"))

    df_pv_inj_city_stats = df_pv_inj_city_stats.withColumnRenamed(
        "trx_date", "trx_date_pv_inj"
    ).withColumnRenamed("reseller_city", "reseller_city_pv_inj")

    df_pv_red_city_stats = df_pv_red_city_stats.withColumnRenamed(
        "trx_date", "trx_date_pv_red"
    ).withColumnRenamed("reseller_city", "reseller_city_pv_red")

    df_outlet = df_outlet.withColumn("month", f.trunc("trx_date", "month"))
    df_outlet_with_zone_pricing_pv_inj_red = (
        df_outlet.join(df_zone_pricing, ["month", "city"], how="left")
        .join(
            df_pv_inj_city_stats,
            (
                (df_outlet.city == df_pv_inj_city_stats.reseller_city_pv_inj)
                & (df_pv_inj_city_stats.trx_date_pv_inj == df_outlet.trx_date)
            ),
            how="left",
        )
        .join(
            df_pv_red_city_stats,
            (
                (df_outlet.city == df_pv_red_city_stats.reseller_city_pv_red)
                & (df_pv_red_city_stats.trx_date_pv_red == df_outlet.trx_date)
            ),
            how="left",
        )
        .drop(
            "trx_date_pv_inj",
            "trx_date_pv_red",
            "reseller_city_pv_red",
            "reseller_city_pv_inj",
        )
    )

    df_outlet_all = (
        (
            df_outlet_with_zone_pricing_pv_inj_red.join(
                dfj_mkios_modern, ["outlet_id", "trx_date"], how="full"
            )
            .join(
                df_total_modern_channel_recharges, ["outlet_id", "trx_date"], how="full"
            )
            .join(df_pv_inj_lacci_outlet_stats, ["outlet_id", "trx_date"], how="left")
            .join(
                df_pv_inj_rs_msisdn_outlet_stats, ["outlet_id", "trx_date"], how="left"
            )
            .join(df_pv_red_lacci_outlet_stats, ["outlet_id", "trx_date"], how="left")
            .join(
                df_pv_red_rs_msisdn_outlet_stats, ["outlet_id", "trx_date"], how="left"
            )
            .join(df_hsupa, ["outlet_id", "trx_date"], how="left")
            .join(df_3g_and_payload, ["outlet_id", "trx_date"], how="left")
        )
        .withColumn(
            "fea_outlet_double_share_digipos_by_recharge_mkios",
            (
                f.when(
                    (f.col("fea_outlet_decimal_total_cashflow_mkios").isNull())
                    | (f.col("fea_outlet_decimal_total_cashflow_mkios") == 0),
                    -1.0,
                )
                .when(f.col("total_rech_digipos") == 0, 0.0)
                .when(
                    f.col("fea_outlet_decimal_total_cashflow_mkios")
                    < f.col("total_rech_digipos"),
                    -1.0,
                )
                .otherwise(
                    f.col("total_rech_digipos")
                    / f.col("fea_outlet_decimal_total_cashflow_mkios")
                )
            ),
        )
        .withColumn(
            "fea_outlet_integer_los", f.datediff(f.col("trx_date"), f.col("created_at"))
        )
        .withColumnRenamed("area", "fea_outlet_area")
        .withColumnRenamed("region", "fea_outlet_region")
        .withColumnRenamed("category", "fea_outlet_string_category")
        .withColumnRenamed("type", "fea_outlet_string_type")
        .withColumnRenamed("location_type", "fea_outlet_string_location_type")
        .withColumnRenamed("created_at", "fea_outlet_created_at")
        .withColumnRenamed("total_rech_digipos", "fea_outlet_long_total_rech_digipos")
        .withColumnRenamed(
            "total_trx_rech_digipos", "fea_outlet_long_total_trx_rech_digipos"
        )
        .withColumnRenamed(
            "total_rev_package_digipos", "fea_outlet_long_total_rev_package_digipos"
        )
        .withColumnRenamed(
            "total_trx_package_digipos", "fea_outlet_long_total_trx_package_digipos"
        )
        .withColumnRenamed(
            "total_rev_package_akuisisi_digipos",
            "fea_outlet_long_tot_rev_package_akuisisi_digipos",
        )
        .withColumnRenamed(
            "total_rev_package_voice_digipos",
            "fea_outlet_long_tot_rev_package_voice_digipos",
        )
        .withColumnRenamed(
            "total_rev_package_sms_digipos",
            "fea_outlet_long_tot_rev_package_sms_digipos",
        )
        .withColumnRenamed(
            "total_rev_package_bulk_digipos",
            "fea_outlet_long_tot_rev_package_bulk_digipos",
        )
        .withColumnRenamed(
            "total_rev_package_fix_digipos",
            "fea_outlet_long_tot_rev_package_fix_digipos",
        )
        .withColumnRenamed(
            "total_rev_package_hvc_digipos",
            "fea_outlet_long_tot_rev_package_hvc_digipos",
        )
        .withColumnRenamed(
            "total_rev_package_haji_digipos",
            "fea_outlet_long_tot_rev_package_haji_digipos",
        )
        .withColumnRenamed("ppmb_voucher_old", "fea_outlet_ppmb_voucher_old")
        .withColumnRenamed("ppmb_voucher_new", "fea_outlet_ppmb_voucher_new")
        .withColumnRenamed("ppmb_core", "fea_outlet_ppmb_core")
        .withColumnRenamed("ppmb_ac", "fea_outlet_ppmb_ac")
        .withColumnRenamed("latitude", "fea_outlet_string_location_latitude")
        .withColumnRenamed("longitude", "fea_outlet_string_location_longitude")
        .withColumnRenamed("classification", "fea_outlet_string_classification")
        .withColumnRenamed("frequency_visit", "fea_outlet_string_frequency_visit")
    )

    required_output_columns = get_required_output_columns(
        output_features=df_outlet_all.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id", "trx_date"],
    )
    df_outlet_all = df_outlet_all.select(required_output_columns)

    conf_catalog = get_config_parameters(config="catalog")
    outlet_master_catalog_entry = conf_catalog["l3_outlet_master_revenue_daily"]

    save_args = outlet_master_catalog_entry["save_args"]
    partitions = int(outlet_master_catalog_entry["partitions"])
    file_format = outlet_master_catalog_entry["file_format"]
    file_path = get_file_path(
        filepath=outlet_master_catalog_entry["filepath"]
    )  # It will add hdfs_base_path in the file path
    create_versions = outlet_master_catalog_entry["create_versions"]

    if create_versions:
        file_path = get_versioned_save_file_path(
            file_path
        )  # It will add version in the file path
    log.info(f"saving to: {file_path}")

    df_outlet_all = df_outlet_all.withColumn(
        "month", f.date_format(f.col("trx_date"), "YYYY-MM-01")
    )

    df_outlet_all.repartition(numPartitions=partitions).write.save(
        file_path, file_format, **save_args
    )


def calc_fea_outlet_revenue_daily_scoring(
    df_outlet: pyspark.sql.DataFrame,
    df_mkios: pyspark.sql.DataFrame,
    df_mkios_mode: pyspark.sql.DataFrame,
    df_modern: pyspark.sql.DataFrame,
    df_pv_red_lacci_outlet_stats: pyspark.sql.DataFrame,
    df_pv_red_city_stats: pyspark.sql.DataFrame,
    df_pv_red_rs_msisdn_outlet_stats: pyspark.sql.DataFrame,
    df_hsupa: pyspark.sql.DataFrame,
    df_3g_and_payload: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: List[str],
):
    """
    Gets weekly Aggregation data from outlet and digipos
    :param df_outlet: data weekly aggregation for data outlet with msisdn outlet from digipos
    :param df_mkios: data summary weekly aggregation for data recharge mkios base on msisdn outlet
    :param df_mkios_mode: data weekly modulus for recharge transaction of mkios
    :param df_modern: data weekly transaction of modern channel compare with mkios transaction in bts lacci_id area
    :param df_pv_red_rs_msisdn_outlet_stats: Physical voucher redemption rs_msisdn features (nominal, serial num)
    :param df_pv_red_city_stats: Physical voucher redemption city features (nominal, serial num)
    :param df_pv_red_lacci_outlet_stats: Physical voucher redemption lacci based features (nominal, serial num)
    :param df_hsupa: Network hsupa features
    :param df_3g_and_payload: Network 3g & payload features
    :param required_output_features: Important features list
    :param feature_mode: Toggle feature mode - all/feature_list
    :return: Join all dataframes to form the final master table on outlet_id, trx_date
    """

    # Join data from mode of mkios with data modern channel
    df_calc_outlet_mode_for_loc_bts_denomination = (
        agg_outlet_mode_first_with_count(
            df_mkios_mode,
            col_count="total_trx_rech",
            rolled_col_names=["denomination", "location_bts"],
            primary_columns=["outlet_id", "trx_date"],
            granularity_col="trx_date",
        )
    )["dataframe"]

    df_mode_location_bts_denomination = df_calc_outlet_mode_for_loc_bts_denomination.select(
        "outlet_id",
        "trx_date",
        f.col("denomination_mode").alias(
            "fea_outlet_decimal_most_popular_denomination"
        ),
        f.col("location_bts_mode").alias("location_bts"),
    )

    df_calc_outlet_mode_rech_mkios = (
        agg_outlet_mode_first_with_count(
            df_mkios_mode,
            col_count="count_rech_mkios",
            rolled_col_names=["recharge_mkios"],
            primary_columns=["outlet_id", "trx_date"],
            granularity_col="trx_date",
        )
    )["dataframe"]
    df_mode_rech_mkios = df_calc_outlet_mode_rech_mkios.select(
        "outlet_id",
        "trx_date",
        f.col("recharge_mkios_mode").alias("fea_outlet_decimal_mode_cashflow_mkios"),
    )

    df_mkios_mode_all = df_mode_location_bts_denomination.join(
        df_mode_rech_mkios, ["outlet_id", "trx_date"], how="left_outer"
    ).select(
        "outlet_id",
        "trx_date",
        "fea_outlet_decimal_most_popular_denomination",
        "location_bts",
        "fea_outlet_decimal_mode_cashflow_mkios",
    )

    dfj_mkios_mode_all_modern = (
        df_mkios_mode_all.join(df_modern, ["location_bts", "trx_date"], "left_outer")
        .withColumnRenamed("location_bts", "fea_outlet_string_location_bts")
        .withColumnRenamed(
            "total_recharge_modern_channel",
            "fea_outlet_decimal_total_recharge_modern_channel",
        )
    )

    # Join data from recharge transaction mkios with (mode of mkios and data modern channel)
    dfj_mkios_modern = (
        (
            df_mkios.join(
                dfj_mkios_mode_all_modern, ["outlet_id", "trx_date"], "left_outer"
            )
        )
        .select(
            f.col("outlet_id"),
            "trx_date",
            f.col("distinct_rs_msisdns").alias("fea_distinct_rs_msisdns"),
            f.col("total_cashflow_mkios").alias(
                "fea_outlet_decimal_total_cashflow_mkios"
            ),
            f.col("total_recharge_mkios").alias(
                "fea_outlet_decimal_total_recharge_mkios"
            ),
            f.col("total_revenue_mkios").alias(
                "fea_outlet_decimal_total_revenue_mkios"
            ),
            f.col("mean_cashflow_mkios").alias(
                "fea_outlet_decimal_mean_cashflow_mkios"
            ),
            f.col("median_cashflow_mkios").alias(
                "fea_outlet_decimal_median_cashflow_mkios"
            ),
            f.col("total_trx_cashflow_mkios").alias(
                "fea_outlet_long_total_trx_cashflow_mkios"
            ),
            f.col("total_trx_recharge_mkios").alias(
                "fea_outlet_long_total_trx_recharge_mkios"
            ),
            f.col("total_trx_revenue_mkios").alias(
                "fea_outlet_long_total_trx_revenue_mkios"
            ),
            f.col("median_denomination_recharge").alias(
                "fea_outlet_decimal_median_denomination_recharge"
            ),
            "fea_outlet_decimal_most_popular_denomination",
            "fea_outlet_decimal_mode_cashflow_mkios",
            "fea_outlet_string_location_bts",
            "fea_outlet_decimal_total_recharge_modern_channel",
            "fea_outlets_urp_channel_rech_bank",
            "fea_outlets_urp_channel_rech_bonus",
            "fea_outlets_urp_channel_rech_dealer",
            "fea_outlets_urp_channel_rech_device",
            "fea_outlets_urp_channel_rech_direct",
            "fea_outlets_urp_channel_rech_e_kiosk",
            "fea_outlets_urp_channel_rech_fisik",
            "fea_outlets_urp_channel_rech_international_partner",
            "fea_outlets_urp_channel_rech_modern_channel_as_channel",
            "fea_outlets_urp_channel_rech_null",
            "fea_outlets_urp_channel_rech_online",
            "fea_outlets_urp_channel_rech_project",
            "fea_outlets_urp_channel_rech_retail_nasional",
        )
        .withColumn(
            "fea_outlet_decimal_proportion_recharge_amount_by_cashflow",
            (
                f.col("fea_outlet_decimal_total_recharge_mkios")
                / f.col("fea_outlet_decimal_total_cashflow_mkios")
            ),
        )
        .withColumn(
            "fea_outlet_decimal_proportion_recharge_trx_by_cashflow",
            (
                f.col("fea_outlet_long_total_trx_recharge_mkios")
                / f.col("fea_outlet_long_total_trx_cashflow_mkios")
            ),
        )
    )

    df_pv_red_city_stats = df_pv_red_city_stats.withColumnRenamed(
        "trx_date", "trx_date_pv_red"
    ).withColumnRenamed("reseller_city", "reseller_city_pv_red")

    df_outlet = df_outlet.withColumn("month", f.trunc("trx_date", "month"))
    df_outlet_with_zone_pricing_pv_inj_red = df_outlet.join(
        df_pv_red_city_stats,
        (
            (df_outlet.city == df_pv_red_city_stats.reseller_city_pv_red)
            & (df_pv_red_city_stats.trx_date_pv_red == df_outlet.trx_date)
        ),
        how="left",
    ).drop(
        "trx_date_pv_inj",
        "trx_date_pv_red",
        "reseller_city_pv_red",
        "reseller_city_pv_inj",
    )

    df_outlet_all = (
        (
            df_outlet_with_zone_pricing_pv_inj_red.join(
                dfj_mkios_modern, ["outlet_id", "trx_date"], how="full"
            )
            .join(df_pv_red_lacci_outlet_stats, ["outlet_id", "trx_date"], how="left")
            .join(
                df_pv_red_rs_msisdn_outlet_stats, ["outlet_id", "trx_date"], how="left"
            )
            .join(df_hsupa, ["outlet_id", "trx_date"], how="left")
            .join(df_3g_and_payload, ["outlet_id", "trx_date"], how="left")
        )
        .withColumn(
            "fea_outlet_double_share_digipos_by_recharge_mkios",
            (
                f.when(
                    (f.col("fea_outlet_decimal_total_cashflow_mkios").isNull())
                    | (f.col("fea_outlet_decimal_total_cashflow_mkios") == 0),
                    -1.0,
                )
                .when(f.col("total_rech_digipos") == 0, 0.0)
                .when(
                    f.col("fea_outlet_decimal_total_cashflow_mkios")
                    < f.col("total_rech_digipos"),
                    -1.0,
                )
                .otherwise(
                    f.col("total_rech_digipos")
                    / f.col("fea_outlet_decimal_total_cashflow_mkios")
                )
            ),
        )
        .withColumn(
            "fea_outlet_integer_los", f.datediff(f.col("trx_date"), f.col("created_at"))
        )
        .withColumnRenamed("area", "fea_outlet_area")
        .withColumnRenamed("region", "fea_outlet_region")
        .withColumnRenamed("category", "fea_outlet_string_category")
        .withColumnRenamed("type", "fea_outlet_string_type")
        .withColumnRenamed("location_type", "fea_outlet_string_location_type")
        .withColumnRenamed("created_at", "fea_outlet_created_at")
        .withColumnRenamed("total_rech_digipos", "fea_outlet_long_total_rech_digipos")
        .withColumnRenamed(
            "total_trx_rech_digipos", "fea_outlet_long_total_trx_rech_digipos"
        )
        .withColumnRenamed(
            "total_rev_package_digipos", "fea_outlet_long_total_rev_package_digipos"
        )
        .withColumnRenamed(
            "total_trx_package_digipos", "fea_outlet_long_total_trx_package_digipos"
        )
        .withColumnRenamed(
            "total_rev_package_akuisisi_digipos",
            "fea_outlet_long_tot_rev_package_akuisisi_digipos",
        )
        .withColumnRenamed(
            "total_rev_package_voice_digipos",
            "fea_outlet_long_tot_rev_package_voice_digipos",
        )
        .withColumnRenamed(
            "total_rev_package_sms_digipos",
            "fea_outlet_long_tot_rev_package_sms_digipos",
        )
        .withColumnRenamed(
            "total_rev_package_bulk_digipos",
            "fea_outlet_long_tot_rev_package_bulk_digipos",
        )
        .withColumnRenamed(
            "total_rev_package_fix_digipos",
            "fea_outlet_long_tot_rev_package_fix_digipos",
        )
        .withColumnRenamed(
            "total_rev_package_hvc_digipos",
            "fea_outlet_long_tot_rev_package_hvc_digipos",
        )
        .withColumnRenamed(
            "total_rev_package_haji_digipos",
            "fea_outlet_long_tot_rev_package_haji_digipos",
        )
        .withColumnRenamed("latitude", "fea_outlet_string_location_latitude")
        .withColumnRenamed("longitude", "fea_outlet_string_location_longitude")
        .withColumnRenamed("classification", "fea_outlet_string_classification")
        .withColumnRenamed("frequency_visit", "fea_outlet_string_frequency_visit")
    )

    required_output_columns = get_required_output_columns(
        output_features=df_outlet_all.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id", "trx_date"],
    )
    df_outlet_all = df_outlet_all.select(required_output_columns)

    df_outlet_all = df_outlet_all.withColumn(
        "month", f.date_format(f.col("trx_date"), "YYYY-MM-01")
    )

    conf_catalog = get_config_parameters(config="catalog")
    outlet_master_catalog_entry = conf_catalog["l3_outlet_master_revenue_daily"]

    save_args = outlet_master_catalog_entry["save_args"]
    partitions = int(outlet_master_catalog_entry["partitions"])
    file_format = outlet_master_catalog_entry["file_format"]
    file_path = get_file_path(
        filepath=outlet_master_catalog_entry["filepath"]
    )  # It will add hdfs_base_path in the file path
    create_versions = outlet_master_catalog_entry["create_versions"]

    if create_versions:
        file_path = get_versioned_save_file_path(
            file_path
        )  # It will add version in the file path
    log.info(f"saving to: {file_path}")

    df_outlet_all.repartition(numPartitions=partitions).write.save(
        file_path, file_format, **save_args
    )


def fill_time_series_outlet_revenue_master(
    df_outlet_revenue_master: pyspark.sql.DataFrame,
):
    spark = SparkSession.builder.getOrCreate()
    start_date_epoch = 1546300800  # Jan 01, 2019  12:00:00 AM UTC
    end_date_epoch = 2556143999  # Dec 31, 2050  11:59:59 PM GMT
    df_daily_dates = create_date_list(
        spark=spark, start=start_date_epoch, end=end_date_epoch, range_=24 * 60 * 60
    ).drop("weekstart")
    start_date = get_start_date(period="1cm")
    end_date = get_end_date(period="1cm")

    datetime.today().date().strftime("%Y-%m-%d")
    df_daily_dates = (
        df_daily_dates.filter(
            (f.col("trx_date") >= start_date) & (f.col("trx_date") <= end_date)
        )
        .select("trx_date")
        .coalesce(1)
    )

    df_outlet_date_scaffold = (
        df_outlet_revenue_master.select("outlet_id")
        .distinct()
        .crossJoin(df_daily_dates)
    )
    df_master_tsf = df_outlet_date_scaffold.join(
        df_outlet_revenue_master, ["outlet_id", "trx_date"], how="left"
    )
    df_master_tsf = df_master_tsf.withColumn(
        "month", f.date_format(f.col("trx_date"), "YYYY-MM-01")
    )
    return df_master_tsf.fillna(0.0)
