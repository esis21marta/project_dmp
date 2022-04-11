from typing import Any, Dict, List

import numpy as np
import pandas as pd
import pyspark
import pyspark.sql.functions as f
from pyspark.ml.feature import QuantileDiscretizer
from pyspark.sql import SparkSession


def psi_baseline_bining(num_cols: List[str], *tables: pyspark.sql.DataFrame) -> List[pyspark.sql.DataFrame]:
    res = []
    for table in tables:
        for col, type_ in table.dtypes:
            if "decimal" in type_:
                table = table.withColumn(col, table[col].cast("float"))
        table = (
            table.withColumnRenamed(
                "fea_int_app_usage_onlinebanking_data_vol_01w",
                "fea_int_app_usage_banking_data_vol_01w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_onlinebanking_data_vol_02w",
                "fea_int_app_usage_banking_data_vol_02w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_onlinebanking_data_vol_03w",
                "fea_int_app_usage_banking_data_vol_03w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_onlinebanking_data_vol_01m",
                "fea_int_app_usage_banking_data_vol_01m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_onlinebanking_data_vol_02m",
                "fea_int_app_usage_banking_data_vol_02m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_onlinebanking_data_vol_03m",
                "fea_int_app_usage_banking_data_vol_03m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_onlinebanking_accessed_apps_01w",
                "fea_int_app_usage_banking_accessed_apps_01w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_onlinebanking_accessed_apps_02w",
                "fea_int_app_usage_banking_accessed_apps_02w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_onlinebanking_accessed_apps_03w",
                "fea_int_app_usage_banking_accessed_apps_03w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_onlinebanking_accessed_apps_01m",
                "fea_int_app_usage_banking_accessed_apps_01m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_onlinebanking_accessed_apps_02m",
                "fea_int_app_usage_banking_accessed_apps_02m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_onlinebanking_accessed_apps_03m",
                "fea_int_app_usage_banking_accessed_apps_03m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_finance_sites_data_vol_01w",
                "fea_int_app_usage_loans_data_vol_01w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_finance_sites_data_vol_02w",
                "fea_int_app_usage_loans_data_vol_02w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_finance_sites_data_vol_03w",
                "fea_int_app_usage_loans_data_vol_03w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_finance_sites_data_vol_01m",
                "fea_int_app_usage_loans_data_vol_01m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_finance_sites_data_vol_02m",
                "fea_int_app_usage_loans_data_vol_02m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_finance_sites_data_vol_03m",
                "fea_int_app_usage_loans_data_vol_03m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_finance_sites_accessed_apps_01w",
                "fea_int_app_usage_loans_accessed_apps_01w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_finance_sites_accessed_apps_02w",
                "fea_int_app_usage_loans_accessed_apps_02w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_finance_sites_accessed_apps_03w",
                "fea_int_app_usage_loans_accessed_apps_03w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_finance_sites_accessed_apps_01m",
                "fea_int_app_usage_loans_accessed_apps_01m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_finance_sites_accessed_apps_02m",
                "fea_int_app_usage_loans_accessed_apps_02m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_finance_sites_accessed_apps_03m",
                "fea_int_app_usage_loans_accessed_apps_03m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_social_media_sites_data_vol_01w",
                "fea_int_app_usage_social_media_data_vol_01w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_social_media_sites_data_vol_02w",
                "fea_int_app_usage_social_media_data_vol_02w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_social_media_sites_data_vol_03w",
                "fea_int_app_usage_social_media_data_vol_03w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_social_media_sites_data_vol_01m",
                "fea_int_app_usage_social_media_data_vol_01m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_social_media_sites_data_vol_02m",
                "fea_int_app_usage_social_media_data_vol_02m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_social_media_sites_data_vol_03m",
                "fea_int_app_usage_social_media_data_vol_03m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_social_media_sites_accessed_apps_01w",
                "fea_int_app_usage_social_media_accessed_apps_01w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_social_media_sites_accessed_apps_02w",
                "fea_int_app_usage_social_media_accessed_apps_02w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_social_media_sites_accessed_apps_03w",
                "fea_int_app_usage_social_media_accessed_apps_03w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_social_media_sites_accessed_apps_01m",
                "fea_int_app_usage_social_media_accessed_apps_01m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_social_media_sites_accessed_apps_02m",
                "fea_int_app_usage_social_media_accessed_apps_02m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_social_media_sites_accessed_apps_03m",
                "fea_int_app_usage_social_media_accessed_apps_03m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_social_media_sites_days_since_first_usage",
                "fea_int_app_usage_social_media_days_since_first_usage",
            )
            .withColumnRenamed(
                "fea_int_app_usage_social_media_sites_days_since_last_usage",
                "fea_int_app_usage_social_media_days_since_last_usage",
            )
            .withColumnRenamed(
                "fea_int_app_usage_payments_data_vol_01w",
                "fea_int_app_usage_wallets_and_payments_data_vol_01w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_payments_data_vol_02w",
                "fea_int_app_usage_wallets_and_payments_data_vol_02w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_payments_data_vol_03w",
                "fea_int_app_usage_wallets_and_payments_data_vol_03w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_payments_data_vol_01m",
                "fea_int_app_usage_wallets_and_payments_data_vol_01m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_payments_data_vol_02m",
                "fea_int_app_usage_wallets_and_payments_data_vol_02m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_payments_data_vol_03m",
                "fea_int_app_usage_wallets_and_payments_data_vol_03m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_payments_accessed_apps_01w",
                "fea_int_app_usage_wallets_and_payments_accessed_apps_01w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_payments_accessed_apps_02w",
                "fea_int_app_usage_wallets_and_payments_accessed_apps_02w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_payments_accessed_apps_03w",
                "fea_int_app_usage_wallets_and_payments_accessed_apps_03w",
            )
            .withColumnRenamed(
                "fea_int_app_usage_payments_accessed_apps_01m",
                "fea_int_app_usage_wallets_and_payments_accessed_apps_01m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_payments_accessed_apps_02m",
                "fea_int_app_usage_wallets_and_payments_accessed_apps_02m",
            )
            .withColumnRenamed(
                "fea_int_app_usage_payments_accessed_apps_03m",
                "fea_int_app_usage_wallets_and_payments_accessed_apps_03m",
            )
            .withColumnRenamed(
                "fea_int_usage_daily_avg_kb_data_usage_03m",
                "fea_int_usage_avg_kb_data_usage_03m",
            )
            .withColumnRenamed(
                "fea_broadband_revenue_avg_6m",
                "fea_network_broadband_revenue_avg_21to0w",
            )
            .withColumnRenamed("fea_ccsr_cs_avg_6m", "fea_network_ccsr_cs_avg_21to0w")
            .withColumnRenamed("fea_ccsr_ps_avg_6m", "fea_network_ccsr_ps_avg_21to0w")
            .withColumnRenamed("fea_cssr_cs_avg_6m", "fea_network_cssr_cs_avg_21to0w")
            .withColumnRenamed("fea_cssr_ps_avg_6m", "fea_network_cssr_ps_avg_21to0w")
            .withColumnRenamed(
                "fea_downlink_traffic_volume_mb_avg_6m",
                "fea_network_downlink_traffic_volume_mb_avg_21to0w",
            )
            .withColumnRenamed(
                "fea_edge_mbyte_avg_6m", "fea_network_edge_mbyte_avg_21to0w"
            )
            .withColumnRenamed(
                "fea_gprs_mbyte_avg_6m", "fea_network_gprs_mbyte_avg_21to0w"
            )
            .withColumnRenamed(
                "fea_hosr_num_sum_avg_6m", "fea_network_hosr_num_sum_avg_21to0w"
            )
            .withColumnRenamed(
                "fea_hsdpa_accesability_avg_6m",
                "fea_network_hsdpa_accesability_avg_21to0w",
            )
            .withColumnRenamed(
                "fea_hsupa_accesability_avg_6m",
                "fea_network_hsupa_accesability_avg_21to0w",
            )
            .withColumnRenamed(
                "fea_hsupa_mean_user_avg_6m", "fea_network_hsupa_mean_user_avg_21to0w"
            )
            .withColumnRenamed("fea_ifhosr_avg_6m", "fea_network_ifhosr_avg_21to0w")
            .withColumnRenamed(
                "fea_payload_hspa_mbyte_avg_6m",
                "fea_network_payload_hspa_mbyte_avg_21to0w",
            )
            .withColumnRenamed(
                "fea_payload_psr99_mbyte_avg_6m",
                "fea_network_payload_psr99_mbyte_avg_21to0w",
            )
            .withColumnRenamed(
                "fea_total_payload_mb_avg_6m", "fea_network_total_payload_mb_avg_21to0w"
            )
            .withColumnRenamed(
                "fea_total_throughput_kbps_avg_6m",
                "fea_network_total_throughput_kbps_avg_21to0w",
            )
            .withColumnRenamed(
                "fea_voice_revenue_avg_6m", "fea_network_voice_revenue_avg_21to0w"
            )
            .withColumnRenamed(
                "fea_volume_voice_traffic_erl_avg_6m",
                "fea_network_volume_voice_traffic_erl_avg_21to0w",
            )
            .withColumnRenamed("fea_hosr_avg_6m", "fea_network_hosr_avg_21to0w")
            .withColumnRenamed(
                "fea_hosr_denum_sum_avg_6m", "fea_network_hosr_denum_sum_avg_21to0w"
            )
            .withColumnRenamed(
                "fea_network_broadband_revenue_avg_6m",
                "fea_network_broadband_revenue_avg_21to0w",
            )
            .withColumnRenamed(
                "fea_network_ccsr_cs_avg_6m", "fea_network_ccsr_cs_avg_21to0w"
            )
            .withColumnRenamed(
                "fea_network_ccsr_ps_avg_6m", "fea_network_ccsr_ps_avg_21to0w"
            )
            .withColumnRenamed(
                "fea_network_cssr_cs_avg_6m", "fea_network_cssr_cs_avg_21to0w"
            )
            .withColumnRenamed(
                "fea_network_cssr_ps_avg_6m", "fea_network_cssr_ps_avg_21to0w"
            )
            .withColumnRenamed(
                "fea_network_downlink_traffic_volume_mb_avg_6m",
                "fea_network_downlink_traffic_volume_mb_avg_21to0w",
            )
            .withColumnRenamed(
                "fea_network_edge_mbyte_avg_6m", "fea_network_edge_mbyte_avg_21to0w"
            )
            .withColumnRenamed(
                "fea_network_gprs_mbyte_avg_6m", "fea_network_gprs_mbyte_avg_21to0w"
            )
            .withColumnRenamed(
                "fea_network_hosr_num_sum_avg_6m", "fea_network_hosr_num_sum_avg_21to0w"
            )
            .withColumnRenamed(
                "fea_network_hsdpa_accesability_avg_6m",
                "fea_network_hsdpa_accesability_avg_21to0w",
            )
            .withColumnRenamed(
                "fea_network_hsupa_accesability_avg_6m",
                "fea_network_hsupa_accesability_avg_21to0w",
            )
            .withColumnRenamed(
                "fea_network_hsupa_mean_user_avg_6m",
                "fea_network_hsupa_mean_user_avg_21to0w",
            )
            .withColumnRenamed(
                "fea_network_ifhosr_avg_6m", "fea_network_ifhosr_avg_21to0w"
            )
            .withColumnRenamed(
                "fea_network_payload_hspa_mbyte_avg_6m",
                "fea_network_payload_hspa_mbyte_avg_21to0w",
            )
            .withColumnRenamed(
                "fea_network_payload_psr99_mbyte_avg_6m",
                "fea_network_payload_psr99_mbyte_avg_21to0w",
            )
            .withColumnRenamed(
                "fea_network_total_payload_mb_avg_6m",
                "fea_network_total_payload_mb_avg_21to0w",
            )
            .withColumnRenamed(
                "fea_network_total_throughput_kbps_avg_6m",
                "fea_network_total_throughput_kbps_avg_21to0w",
            )
            .withColumnRenamed(
                "fea_network_voice_revenue_avg_6m",
                "fea_network_voice_revenue_avg_21to0w",
            )
            .withColumnRenamed(
                "fea_network_volume_voice_traffic_erl_avg_6m",
                "fea_network_volume_voice_traffic_erl_avg_21to0w",
            )
            .withColumnRenamed("fea_network_hosr_avg_6m", "fea_network_hosr_avg_21to0w")
            .withColumnRenamed(
                "fea_network_hosr_denum_sum_avg_6m",
                "fea_network_hosr_denum_sum_avg_21to0w",
            )
            .select(num_cols)
            .drop("fea_handset_imeis")
            .fillna(0)
        )

        res.append(table)

    return res


def calculate_psi_baseline(df: pyspark.sql.DataFrame, num_unique: int):

    res = []

    df = df.select([col for col in df.columns if "fea_" in col])
    df = df.select(
        [
            col
            for col in df.columns
            if len(df.select(col).distinct().collect()) > num_unique
        ]
    )
    for col in df.columns:
        df = (
            QuantileDiscretizer(numBuckets=5, inputCol=col, outputCol=f"bin_{col}")
            .fit(df)
            .transform(df)
        )

    bin_col = [col for col in df.columns if "bin" not in col]
    for col in bin_col:
        binning = (
            df.groupby(f"bin_{col}")
            .agg(
                f.min(f.col(col)).alias("min"),
                f.max(f.col(col)).alias("max"),
                f.count(f.col(col)).alias("count_baseline"),
            )
            .toPandas()
        )
        binning["proportion"] = (
            binning["count_baseline"] / binning["count_baseline"].sum()
        )
        binning = binning.drop("count_baseline", axis=1)
        binning_list = binning.set_index([f"bin_{col}"]).to_dict("record")

        dict_of_bins = {}

        dict_of_bins[col] = {}
        n_element = len(binning_list)
        for num, bins in enumerate(binning_list):
            dict_of_bins[col][num] = {}
            dict_of_bins[col][num]["min"] = bins["min"] if num > 0 else "-inf"
            dict_of_bins[col][num]["max"] = (
                bins["max"] if num < (n_element - 1) else "inf"
            )
            dict_of_bins[col][num]["proportion"] = float(
                np.round(bins["proportion"], 4)
            )

        res.append(dict_of_bins)

    return res


def _psi(baseline_col: float, current_col: float) -> float:
    """
    PSI Formula

    :param baseline_col: The baseline proportion value
    :param current_col: The current proportion value
    :return: PSI
    """
    psi = (current_col - baseline_col) * np.log(current_col / baseline_col)
    return float(psi)


def calculate_psi(
    df: pyspark.sql.DataFrame, bin_threshold: Dict[Any, Any], column_to_bin: str
):
    """
    Calculate the PSI based on the pre-defined binning threshold

    :param df: Scoring dataframe which contains features and scores
    :param bin_threshold: Dictionary of bins and the min,max,proportion values for the bins
    :param column_to_bin: The name of the feature to bin
    :return: PSI per features and score
    """
    when_condition = []
    for key, val in bin_threshold.items():
        max_val = val["max"] if val["max"] != "inf" else np.inf
        min_val = val["min"] if val["min"] != "-inf" else -np.inf
        when_condition.append(
            f.when(f.col(column_to_bin).between(min_val, max_val), key)
        )

    df = df.select(*([f"{column_to_bin}"])).fillna(0)
    df = df.select("*", f.coalesce(*when_condition).alias("bins"))

    agg_bins = (
        df.groupby(["bins"])
        .count()
        .withColumn("dimension", f.lit(column_to_bin))
        .withColumnRenamed("count", "count_current")
        .toPandas()
    )

    return agg_bins


def calculate_psi_all(
    df: pyspark.sql.DataFrame, features_binning: Dict[str, Any]
) -> pyspark.sql.DataFrame:
    """
    Iterate over the features and calculate the PSI for each of them

    :param df: Scoring dataframe which contains features and scores
    :param features_binning: The binning thresholds for each features
    :return: PSI per features and score
    """

    psi_outputs = pd.DataFrame()
    for column_to_bin, bins in features_binning.items():
        psi_output = calculate_psi(df, bins, column_to_bin)

        n_repeat = len(bins.keys())
        pd_bins = pd.DataFrame(bins.keys(), columns=["bins"])

        base_bins = psi_output.drop(["bins", "count_current"], axis=1).head(1)
        base_bins_col = list(base_bins.columns)

        base_bins = pd.concat([base_bins] * n_repeat, ignore_index=True).join(pd_bins)
        psi_output = pd.merge(
            base_bins, psi_output, on=base_bins_col + ["bins"], how="left"
        ).fillna(1)
        psi_output["proportion_baseline"] = psi_output.apply(
            lambda x: features_binning[x["dimension"]][x["bins"]]["proportion"], axis=1
        )
        psi_output["proportion_current"] = (
            psi_output["count_current"] / psi_output["count_current"].sum()
        )
        psi_output["psi"] = psi_output.apply(
            lambda x: _psi(x["proportion_baseline"], x["proportion_current"]), axis=1
        )
        psi_outputs = pd.DataFrame.append(psi_outputs, psi_output)

    spark = SparkSession.builder.getOrCreate()
    psi_outputs = spark.createDataFrame(psi_outputs)

    return psi_outputs


def filter_psi_columns(
    df: pyspark.sql.DataFrame, columns_to_include: List
) -> pyspark.sql.DataFrame:
    """
    Get the summary of the PSI

    :param df: PSI tables
    :param columns_to_include: Include the columns
    :return: PSI summary report
    """

    df = df.select(columns_to_include)

    return df
