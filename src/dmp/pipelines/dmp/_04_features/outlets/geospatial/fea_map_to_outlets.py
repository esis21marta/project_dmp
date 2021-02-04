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

from typing import List

import pyspark
import pyspark.sql.functions as f

from utils import get_custom_window, get_required_output_columns


def map_features_to_outlets(
    df_outlets: pyspark.sql.DataFrame,
    # df_ext_fb_share: pyspark.sql.DataFrame,
    # df_ext_4g_bts_share: pyspark.sql.DataFrame,
    df_arch: pyspark.sql.DataFrame,
    df_dealer: pyspark.sql.DataFrame,
    # df_ookla: pyspark.sql.DataFrame,
    # df_opensignal_busyhour: pyspark.sql.DataFrame,
    df_digistar_sellthru_det: pyspark.sql.DataFrame,
    df_salesforce: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: List[str],
):
    window = get_custom_window(["outlet_id"], "dates", asc=False,)
    df_outlets = df_outlets.withColumn("kabupaten", f.lower(f.col("kabupaten")))
    df_outlets = df_outlets.select("outlet_id", "kabupaten", "cluster")
    # ------------- FB Share ------------- #
    # df_fb_share = df_outlets.join(
    #     f.broadcast(df_ext_fb_share),
    #     f.lower(df_outlets.kabupaten) == f.lower(df_ext_fb_share.city),
    #     how="inner",
    # )
    # fb_share_columns_of_interest = [
    #     "outlet_id",
    #     f.col("dates").alias("fea_outlets_fb_share_date"),
    #     f.col("city").alias("fea_outlets_fb_share_city"),
    #     f.col("share_tsel").alias("fea_outlets_fb_share_tsel_share"),
    #     f.col("share_xl").alias("fea_outlets_fb_share_xl_share"),
    #     f.col("share_isat").alias("fea_outlets_fb_share_isat_share"),
    #     f.col("share_three").alias("fea_outlets_fb_share_three_share"),
    #     f.col("share_smartfren").alias("fea_outlets_fb_share_smartfren_share"),
    # ]
    # df_fb_share = (
    #     df_fb_share.withColumn("month", f.trunc("dates", "month",),)
    #     .withColumn("row_number", f.row_number().over(window),)
    #     .filter(f.col("row_number") == 1)
    # )

    # ------------- 4G BTS Share ------------- #
    # df_4g_bts_share = df_outlets.join(
    #     f.broadcast(df_ext_4g_bts_share),
    #     f.lower(df_outlets.kabupaten) == f.lower(df_ext_4g_bts_share.district),
    #     how="inner",
    # )
    # df_4g_bts_share = (
    #     df_4g_bts_share.withColumn("year", f.col("year").cast("int").cast("string"),)
    #     .withColumn("month", f.col("month").cast("int").cast("string"),)
    #     .withColumn("month", f.lpad(f.col("month"), 2, "0",),)
    #     .withColumn(
    #         "dates",
    #         f.concat(
    #             f.col("year"), f.lit("-"), f.col("month"), f.lit("-"), f.lit("01"),
    #         ),
    #     )
    # )
    # bts_4g_columns_of_interest = [
    #     "outlet_id",
    #     f.col("district").alias("fea_outlets_4g_share_district"),
    #     f.col("year").alias("fea_outlets_4g_share_year"),
    #     f.col("month").alias("fea_outlets_4g_share_month"),
    #     f.col("tsel_value").alias("fea_outlets_4g_share_tsel_value"),
    #     f.col("xl_value").alias("fea_outlets_4g_share_xl_value"),
    #     f.col("indosat_value").alias("fea_outlets_4g_share_indosat_value"),
    #     f.col("three_value").alias("fea_outlets_4g_share_three_value"),
    #     f.col("smartfren_value").alias("fea_outlets_4g_share_smartfren_value"),
    #     f.col("tsel_share").alias("fea_outlets_4g_share_tsel_share"),
    #     f.col("xl_share").alias("fea_outlets_4g_share_xl_share"),
    #     f.col("indosat_share").alias("fea_outlets_4g_share_indosat_share"),
    #     f.col("three_share").alias("fea_outlets_4g_share_three_share"),
    #     f.col("smartfren_share").alias("fea_outlets_4g_share_smartfren_share"),
    # ]
    # df_4g_bts_share = df_4g_bts_share.withColumn(
    #     "row_number", f.row_number().over(window),
    # ).filter(f.col("row_number") == 1)

    # ------------- Outlet Archetype ------------- #
    df_arch = df_arch.select("city", "archetype",)
    df_outlet_archetype = df_outlets.join(
        f.broadcast(df_arch),
        f.lower(df_outlets.kabupaten) == f.lower(df_arch.city),
        how="inner",
    )
    archetype_columns_of_interest = [
        "outlet_id",
        f.col("archetype").alias("fea_archetype"),
    ]

    # ------------- Outlet Dealer ------------- #
    # One outlet may have many rs_msisdns, take the first one
    df_dealer_uniq = (
        df_dealer.select("cluster_name", "company_name",)
        .groupBy("cluster_name")
        .agg(f.first("company_name").alias("company_name"))
        .withColumnRenamed("cluster_name", "cluster")
    )
    outlet_dealer_columns_of_interest = [
        "outlet_id",
        f.col("cluster").alias("fea_outlets_dealer_cluster_name"),
        f.col("company_name").alias("fea_outlets_dealer_company_name"),
    ]
    df_outlet_dealer = df_outlets.join(
        f.broadcast(df_dealer_uniq), ["cluster"], how="left",
    )

    # ------------- OOKLA Network ------------- #
    # df_ookla = df_ookla.withColumn("kabupaten", f.lower(f.col("kabupaten")))
    # df_ookla_pivot_by_city = (
    #     df_ookla.filter(
    #         (f.col("sim_network_operator").isNotNull())
    #         & (f.col("sim_network_operator") != f.lit("null"))
    #     )
    #     .withColumn("sim_network_operator", f.lower(f.col("sim_network_operator")))
    #     .groupBy("kabupaten")
    #     .pivot("sim_network_operator")
    #     .agg(
    #         f.min("download_kbps").alias("min_download_kbps"),
    #         f.max("download_kbps").alias("max_download_kbps"),
    #         f.avg("download_kbps").alias("avg_download_kbps"),
    #     )
    # )

    # df_ookla_pivot_by_city = add_prefix_to_fea_columns(
    #     df_ookla_pivot_by_city, ["kabupaten"], "fea_outlets_ookla_"
    # )
    # df_outlet_ookla_speeds_by_city_mapped_to_outlets = df_outlets.join(
    #     f.broadcast(df_ookla_pivot_by_city), ["kabupaten"], how="inner"
    # )
    # oolka_fea_columns = ["outlet_id"] + [
    #     col
    #     for col in df_outlet_ookla_speeds_by_city_mapped_to_outlets.columns
    #     if "fea_" in col
    # ]

    # opensignal_busyhour_columns_of_interest = [
    #     "city",
    #     "telkomsel_throughput_download_in_busy_hours_mbps",
    #     "telkomsel_latency_ms",
    #     "isat_throughput_download_in_busy_hours_mbps",
    #     "isat_latency_ms",
    #     "xl_throughput_download_in_busy_hours_mbps",
    #     "xl_latency_ms",
    #     "three_throughput_download_in_busy_hours_mbps",
    #     "three_latency_ms",
    #     "smartfren_throughput_download_in_busy_hours_mbps",
    #     "smartfren_latency_ms",
    # ]
    # df_opensignal_busyhour = df_opensignal_busyhour.select(
    #     opensignal_busyhour_columns_of_interest
    # )
    # df_opensignal_busyhour = add_prefix_to_fea_columns(
    #     df_opensignal_busyhour, ["city"], "fea_outlets_ookla_"
    # )
    # df_outlet_opensignal_busyhour_metrics_by_city_mapped_to_outlets = df_outlets.join(
    #     f.broadcast(df_opensignal_busyhour),
    #     f.lower(df_outlets.kabupaten) == f.lower(df_opensignal_busyhour.city),
    #     how="inner",
    # )
    # opensignal_fea_busyhour_columns_of_interest = ["outlet_id"] + [
    #     col
    #     for col in df_outlet_opensignal_busyhour_metrics_by_city_mapped_to_outlets.columns
    #     if "fea_" in col
    # ]

    # ------------- Sell Through Counts ------------- #
    sell_through_columns_of_interest = ["outlet_id", "fea_outlets_sell_through_counts"]
    df_sell_through_counts = df_digistar_sellthru_det.groupBy("outlet_id").agg(
        f.count("*").alias("fea_outlets_sell_through_counts")
    )
    df_sell_through_counts_per_outlet = df_outlets.join(
        df_sell_through_counts, ["outlet_id"], how="inner"
    )

    # ------------- Salesforce Features ------------- #
    sf_columns_of_interest = [
        "outlet_id",
        f.col("sf_code").alias("fea_outlets_salesforce_sf_code"),
    ]
    df_sf_code_counts = df_salesforce.groupBy("outlet_id", "sf_code").count()
    window_get_most_freq_sf_code = get_custom_window(["outlet_id"], "count", asc=False)
    df_sf_code_ranks = df_sf_code_counts.withColumn(
        "row_number", f.row_number().over(window_get_most_freq_sf_code)
    )
    dft_most_freq_sf_code = df_sf_code_ranks.filter(f.col("row_number") == 1).drop(
        "row_number"
    )

    # df_4g_bts_share = df_4g_bts_share.select(bts_4g_columns_of_interest)
    # df_fb_share = df_fb_share.select(fb_share_columns_of_interest)
    df_outlet_archetype = df_outlet_archetype.select(archetype_columns_of_interest)
    df_outlet_dealer = df_outlet_dealer.select(outlet_dealer_columns_of_interest)
    # df_outlet_ookla_speeds_by_city_mapped_to_outlets = df_outlet_ookla_speeds_by_city_mapped_to_outlets.select(
    #     oolka_fea_columns
    # )
    # df_outlet_opensignal_busyhour_metrics_by_city_mapped_to_outlets = df_outlet_opensignal_busyhour_metrics_by_city_mapped_to_outlets.select(
    #     opensignal_fea_busyhour_columns_of_interest
    # )
    df_sell_through_counts_per_outlet = df_sell_through_counts_per_outlet.select(
        sell_through_columns_of_interest
    )
    dft_most_freq_sf_code = dft_most_freq_sf_code.select(sf_columns_of_interest)

    # required_output_columns_4g_bts_share = get_required_output_columns(
    #     output_features=df_4g_bts_share.columns,
    #     feature_mode=feature_mode,
    #     feature_list=required_output_features,
    #     extra_columns_to_keep=["outlet_id"],
    # )
    # required_output_columns_fb_share = get_required_output_columns(
    #     output_features=df_fb_share.columns,
    #     feature_mode=feature_mode,
    #     feature_list=required_output_features,
    #     extra_columns_to_keep=["outlet_id"],
    # )
    required_output_columns_outlet_archetype = get_required_output_columns(
        output_features=df_outlet_archetype.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id"],
    )
    required_output_columns_outlet_dealer = get_required_output_columns(
        output_features=df_outlet_dealer.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id"],
    )
    # required_output_columns_ookla_speeds_by_city = get_required_output_columns(
    #     output_features=df_outlet_ookla_speeds_by_city_mapped_to_outlets.columns,
    #     feature_mode=feature_mode,
    #     feature_list=required_output_features,
    #     extra_columns_to_keep=["outlet_id"],
    # )
    # required_output_columns_opensignal_busyhour = get_required_output_columns(
    #     output_features=df_outlet_opensignal_busyhour_metrics_by_city_mapped_to_outlets.columns,
    #     feature_mode=feature_mode,
    #     feature_list=required_output_features,
    #     extra_columns_to_keep=["outlet_id"],
    # )
    required_output_columns_sell_through_counts_per_outlet = get_required_output_columns(
        output_features=df_sell_through_counts_per_outlet.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id"],
    )
    required_output_columns_most_freq_sf_code = get_required_output_columns(
        output_features=dft_most_freq_sf_code.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id"],
    )

    # df_4g_bts_share = df_4g_bts_share.select(required_output_columns_4g_bts_share)
    # df_fb_share = df_fb_share.select(required_output_columns_fb_share)
    df_outlet_archetype = df_outlet_archetype.select(
        required_output_columns_outlet_archetype
    )
    df_outlet_dealer = df_outlet_dealer.select(required_output_columns_outlet_dealer)
    # df_outlet_ookla_speeds_by_city_mapped_to_outlets = df_outlet_ookla_speeds_by_city_mapped_to_outlets.select(
    #     required_output_columns_ookla_speeds_by_city
    # )
    # df_outlet_opensignal_busyhour_metrics_by_city_mapped_to_outlets = df_outlet_opensignal_busyhour_metrics_by_city_mapped_to_outlets.select(
    #     required_output_columns_opensignal_busyhour
    # )
    df_sell_through_counts_per_outlet = df_sell_through_counts_per_outlet.select(
        required_output_columns_sell_through_counts_per_outlet
    )
    dft_most_freq_sf_code = dft_most_freq_sf_code.select(
        required_output_columns_most_freq_sf_code
    )

    # ------------- Return Feature Outputs ------------- #
    return [
        # df_4g_bts_share,
        # df_fb_share,
        df_outlet_archetype,
        df_outlet_dealer,
        # df_outlet_ookla_speeds_by_city_mapped_to_outlets,
        # df_outlet_opensignal_busyhour_metrics_by_city_mapped_to_outlets,
        df_sell_through_counts_per_outlet,
        dft_most_freq_sf_code,
    ]
