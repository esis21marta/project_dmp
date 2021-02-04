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

# import library
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from prince import PCA
from sklearn.preprocessing import StandardScaler


def numerical_categorical(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, List[str], List[str]]:
    """
    Separate numererical and categorical variables
    :param df: master table
    :return:
    """
    numerical_columns = df.select_dtypes(np.number).columns.tolist()
    categorical_columns = list(set(df.columns) - set(numerical_columns))
    df_num = df.loc[:, numerical_columns]
    df_cat = df.loc[:, categorical_columns]

    return df_num, df_cat, numerical_columns, categorical_columns


# PCA Functions for numerical data
def apply_pca(
    df: pd.DataFrame,
    _4g_dimension_reduction_parameter: Dict[str, str],
    path_to_save: str,
) -> Tuple[PCA, pd.DataFrame, List[float], StandardScaler]:
    """
    Fit a pca transformation (do not run `transform`) and obtain a correlations table
    Also fitting a `scaler` to normalize the results previously obtained.
    :param df: master table
    :param _4g_dimension_reduction_parameter: parameters
    :return:
    """
    pca = PCA(
        n_components=int(_4g_dimension_reduction_parameter["n_components_reduction"]),
        n_iter=int(_4g_dimension_reduction_parameter["n_iter_reduction"]),
        copy=bool(_4g_dimension_reduction_parameter["copy_reduction"]),
        check_input=bool(_4g_dimension_reduction_parameter["check_input_reduction"]),
        engine=_4g_dimension_reduction_parameter["engine_reduction"],
        random_state=int(_4g_dimension_reduction_parameter["random_state_reduction"]),
    )
    # transform
    scaler = StandardScaler()
    scaler.fit(df)
    df_scaled = scaler.transform(df)
    # dimension reduction
    pcas = pca.fit(df_scaled)  # fitting PCA
    pcas_correlations = pcas.column_correlations(df_scaled)  # dataframe
    pcas_correlations_ = pd.DataFrame(pcas_correlations.values.T, columns=df.columns).T
    pcas_explained_inertia = pca.explained_inertia_  # list
    print(pcas_correlations)
    # plot pca corr matrix
    print(pcas_correlations_)
    print(pcas_explained_inertia)
    fig, ax = plt.subplots(figsize=(15, 25))
    sns_plot = sns.heatmap(
        pcas_correlations_,
        annot=True,
        annot_kws={"size": 10},
        cmap="PiYG",
        vmin=-1,
        vmax=1,
    )
    sns.set(font_scale=1)
    ax.set_title("PC Weight Transformation")
    plt.savefig(path_to_save)

    return pcas, pcas_correlations_, pcas_explained_inertia, scaler


# TODO modulirize this function better
# transform FAMD PCA MCA, apply clusters
def transform_dimension_reduction(
    df: pd.DataFrame,
    numerical_columns: List[str],
    pca: PCA,
    scaler_pca: StandardScaler,
    fg_score_or_train_tag,
) -> pd.DataFrame:

    if fg_score_or_train_tag == "train":
        # separate num and cat
        df_num = df.loc[:, numerical_columns]
        # tranform scaler
        df_scaled_pca = scaler_pca.transform(df_num)
        # transform dimension reduction (PCA,MCA,FAMD)
        X_pca = pd.DataFrame(pca.transform(df_scaled_pca))

        return X_pca
    else:
        df = df.rename(
            columns={
                "fea_int_app_usage_onlinebanking_data_vol_01w": "fea_int_app_usage_banking_data_vol_01w",
                "fea_int_app_usage_onlinebanking_data_vol_02w": "fea_int_app_usage_banking_data_vol_02w",
                "fea_int_app_usage_onlinebanking_data_vol_03w": "fea_int_app_usage_banking_data_vol_03w",
                "fea_int_app_usage_onlinebanking_data_vol_01m": "fea_int_app_usage_banking_data_vol_01m",
                "fea_int_app_usage_onlinebanking_data_vol_02m": "fea_int_app_usage_banking_data_vol_02m",
                "fea_int_app_usage_onlinebanking_data_vol_03m": "fea_int_app_usage_banking_data_vol_03m",
                "fea_int_app_usage_onlinebanking_accessed_apps_01w": "fea_int_app_usage_banking_accessed_apps_01w",
                "fea_int_app_usage_onlinebanking_accessed_apps_02w": "fea_int_app_usage_banking_accessed_apps_02w",
                "fea_int_app_usage_onlinebanking_accessed_apps_03w": "fea_int_app_usage_banking_accessed_apps_03w",
                "fea_int_app_usage_onlinebanking_accessed_apps_01m": "fea_int_app_usage_banking_accessed_apps_01m",
                "fea_int_app_usage_onlinebanking_accessed_apps_02m": "fea_int_app_usage_banking_accessed_apps_02m",
                "fea_int_app_usage_onlinebanking_accessed_apps_03m": "fea_int_app_usage_banking_accessed_apps_03m",
                "fea_int_app_usage_finance_sites_data_vol_01w": "fea_int_app_usage_loans_data_vol_01w",
                "fea_int_app_usage_finance_sites_data_vol_02w": "fea_int_app_usage_loans_data_vol_02w",
                "fea_int_app_usage_finance_sites_data_vol_03w": "fea_int_app_usage_loans_data_vol_03w",
                "fea_int_app_usage_finance_sites_data_vol_01m": "fea_int_app_usage_loans_data_vol_01m",
                "fea_int_app_usage_finance_sites_data_vol_02m": "fea_int_app_usage_loans_data_vol_02m",
                "fea_int_app_usage_finance_sites_data_vol_03m": "fea_int_app_usage_loans_data_vol_03m",
                "fea_int_app_usage_finance_sites_accessed_apps_01w": "fea_int_app_usage_loans_accessed_apps_01w",
                "fea_int_app_usage_finance_sites_accessed_apps_02w": "fea_int_app_usage_loans_accessed_apps_02w",
                "fea_int_app_usage_finance_sites_accessed_apps_03w": "fea_int_app_usage_loans_accessed_apps_03w",
                "fea_int_app_usage_finance_sites_accessed_apps_01m": "fea_int_app_usage_loans_accessed_apps_01m",
                "fea_int_app_usage_finance_sites_accessed_apps_02m": "fea_int_app_usage_loans_accessed_apps_02m",
                "fea_int_app_usage_finance_sites_accessed_apps_03m": "fea_int_app_usage_loans_accessed_apps_03m",
                "fea_int_app_usage_social_media_sites_data_vol_01w": "fea_int_app_usage_social_media_data_vol_01w",
                "fea_int_app_usage_social_media_sites_data_vol_02w": "fea_int_app_usage_social_media_data_vol_02w",
                "fea_int_app_usage_social_media_sites_data_vol_03w": "fea_int_app_usage_social_media_data_vol_03w",
                "fea_int_app_usage_social_media_sites_data_vol_01m": "fea_int_app_usage_social_media_data_vol_01m",
                "fea_int_app_usage_social_media_sites_data_vol_02m": "fea_int_app_usage_social_media_data_vol_02m",
                "fea_int_app_usage_social_media_sites_data_vol_03m": "fea_int_app_usage_social_media_data_vol_03m",
                "fea_int_app_usage_social_media_sites_accessed_apps_01w": "fea_int_app_usage_social_media_accessed_apps_01w",
                "fea_int_app_usage_social_media_sites_accessed_apps_02w": "fea_int_app_usage_social_media_accessed_apps_02w",
                "fea_int_app_usage_social_media_sites_accessed_apps_03w": "fea_int_app_usage_social_media_accessed_apps_03w",
                "fea_int_app_usage_social_media_sites_accessed_apps_01m": "fea_int_app_usage_social_media_accessed_apps_01m",
                "fea_int_app_usage_social_media_sites_accessed_apps_02m": "fea_int_app_usage_social_media_accessed_apps_02m",
                "fea_int_app_usage_social_media_sites_accessed_apps_03m": "fea_int_app_usage_social_media_accessed_apps_03m",
                "fea_int_app_usage_social_media_sites_days_since_first_usage": "fea_int_app_usage_social_media_days_since_first_usage",
                "fea_int_app_usage_social_media_sites_days_since_last_usage": "fea_int_app_usage_social_media_days_since_last_usage",
                "fea_int_app_usage_payments_data_vol_01w": "fea_int_app_usage_wallets_and_payments_data_vol_01w",
                "fea_int_app_usage_payments_data_vol_02w": "fea_int_app_usage_wallets_and_payments_data_vol_02w",
                "fea_int_app_usage_payments_data_vol_03w": "fea_int_app_usage_wallets_and_payments_data_vol_03w",
                "fea_int_app_usage_payments_data_vol_01m": "fea_int_app_usage_wallets_and_payments_data_vol_01m",
                "fea_int_app_usage_payments_data_vol_02m": "fea_int_app_usage_wallets_and_payments_data_vol_02m",
                "fea_int_app_usage_payments_data_vol_03m": "fea_int_app_usage_wallets_and_payments_data_vol_03m",
                "fea_int_app_usage_payments_accessed_apps_01w": "fea_int_app_usage_wallets_and_payments_accessed_apps_01w",
                "fea_int_app_usage_payments_accessed_apps_02w": "fea_int_app_usage_wallets_and_payments_accessed_apps_02w",
                "fea_int_app_usage_payments_accessed_apps_03w": "fea_int_app_usage_wallets_and_payments_accessed_apps_03w",
                "fea_int_app_usage_payments_accessed_apps_01m": "fea_int_app_usage_wallets_and_payments_accessed_apps_01m",
                "fea_int_app_usage_payments_accessed_apps_02m": "fea_int_app_usage_wallets_and_payments_accessed_apps_02m",
                "fea_int_app_usage_payments_accessed_apps_03m": "fea_int_app_usage_wallets_and_payments_accessed_apps_03m",
                "fea_int_usage_daily_avg_kb_data_usage_03m": "fea_int_usage_avg_kb_data_usage_03m",
                "fea_network_broadband_revenue_avg_6m": "fea_network_broadband_revenue_avg_21to0w",
                "fea_network_ccsr_cs_avg_6m": "fea_network_ccsr_cs_avg_21to0w",
                "fea_network_ccsr_ps_avg_6m": "fea_network_ccsr_ps_avg_21to0w",
                "fea_network_cssr_cs_avg_6m": "fea_network_cssr_cs_avg_21to0w",
                "fea_network_cssr_ps_avg_6m": "fea_network_cssr_ps_avg_21to0w",
                "fea_network_downlink_traffic_volume_mb_avg_6m": "fea_network_downlink_traffic_volume_mb_avg_21to0w",
                "fea_network_edge_mbyte_avg_6m": "fea_network_edge_mbyte_avg_21to0w",
                "fea_network_gprs_mbyte_avg_6m": "fea_network_gprs_mbyte_avg_21to0w",
                "fea_network_hosr_num_sum_avg_6m": "fea_network_hosr_num_sum_avg_21to0w",
                "fea_network_hsdpa_accesability_avg_6m": "fea_network_hsdpa_accesability_avg_21to0w",
                "fea_network_hsupa_accesability_avg_6m": "fea_network_hsupa_accesability_avg_21to0w",
                "fea_network_hsupa_mean_user_avg_6m": "fea_network_hsupa_mean_user_avg_21to0w",
                "fea_network_ifhosr_avg_6m": "fea_network_ifhosr_avg_21to0w",
                "fea_network_payload_hspa_mbyte_avg_6m": "fea_network_payload_hspa_mbyte_avg_21to0w",
                "fea_network_payload_psr99_mbyte_avg_6m": "fea_network_payload_psr99_mbyte_avg_21to0w",
                "fea_network_total_payload_mb_avg_6m": "fea_network_total_payload_mb_avg_21to0w",
                "fea_network_total_throughput_kbps_avg_6m": "fea_network_total_throughput_kbps_avg_21to0w",
                "fea_network_voice_revenue_avg_6m": "fea_network_voice_revenue_avg_21to0w",
                "fea_network_volume_voice_traffic_erl_avg_6m": "fea_network_volume_voice_traffic_erl_avg_21to0w",
                "fea_network_hosr_avg_6m": "fea_network_hosr_avg_21to0w",
                "fea_network_hosr_denum_sum_avg_6m": "fea_network_hosr_denum_sum_avg_21to0w",
            }
        )

        # separate num and cat
        df_num = df.loc[:, numerical_columns].fillna(0)
        # tranform scaler
        df_scaled_pca = scaler_pca.transform(df_num.fillna(0))
        # transform dimension reduction (PCA,MCA,FAMD)
        X_pca = pd.DataFrame(pca.transform(df_scaled_pca))

        return X_pca
