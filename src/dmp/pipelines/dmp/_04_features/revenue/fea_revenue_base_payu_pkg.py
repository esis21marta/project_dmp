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

from utils import (
    get_config_based_features,
    get_config_based_features_column_names,
    get_end_date,
    get_required_output_columns,
    get_start_date,
    join_all,
    sum_of_columns_over_weekstart_window,
)


def fea_revenue_base_payu_pkg(
    df_revenue_weekly: pyspark.sql.DataFrame,
    df_revenue_alt_weekly: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Calculates base payu package revenue for each msisdn:

    Args:
        df_revenue_weekly: Revenue weekly data
        df_revenue_alt_weekly: Revenue Alt weekly data

    Returns:
        df_features: Dataframe with features, revenue
            - msisdn: Unique Id
            - weekstart: Weekly obervation point
            - fea_rev_tot_sum_*d: revenue in the window
            - fea_rev_*_payu_sum_*d: payu SMS, Voice, Data, DLS, Roam revenue in the window
            - fea_rev_dls_*_payu_sum_*d: payu DLS by type revenue in the window
            - fea_rev_*_pkg_sum_*d: payu SMS, Voice, Data, Roam revenue in the window
            - fea_rev_roam_tot_sum_*d: total Roam revenue in the window
    """
    config_feature_revenue = config_feature["revenue"]

    # selecting specific agg columns to improve performance
    df_revenue_weekly = df_revenue_weekly.join(
        df_revenue_alt_weekly, ["msisdn", "weekstart"], how="outer"
    )
    df_revenue_weekly = df_revenue_weekly.select(SELECT_AGG_COLUMNS)

    df_features = get_base_payu_pkg_rev(df_revenue_weekly, config_feature_revenue)

    df_features.cache()

    df_features = join_all(
        [
            df_features,
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_rev_dls_mms_cnt_payu_sum"],
                sum_of_columns_over_weekstart_window(["rev_payu_dls_mms_cnt"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_rev_dls_mms_p2p_payu_sum"],
                sum_of_columns_over_weekstart_window(["rev_payu_dls_mms_p2p"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_rev_dls_music_payu_sum"],
                sum_of_columns_over_weekstart_window(["rev_payu_dls_music"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_rev_dls_other_vas_payu_sum"],
                sum_of_columns_over_weekstart_window(["rev_payu_dls_other_vas"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_rev_dls_rbt_payu_sum"],
                sum_of_columns_over_weekstart_window(["rev_payu_dls_rbt"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_rev_dls_sms_nonp2p_payu_sum"],
                sum_of_columns_over_weekstart_window(["rev_payu_dls_sms_nonp2p"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_rev_dls_tp_payu_sum"],
                sum_of_columns_over_weekstart_window(["rev_payu_dls_tp"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_rev_dls_ussd_payu_sum"],
                sum_of_columns_over_weekstart_window(["rev_payu_dls_ussd"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_rev_dls_videocall_payu_sum"],
                sum_of_columns_over_weekstart_window(["rev_payu_dls_videocall"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_rev_dls_voice_nonp2p_payu_sum"],
                sum_of_columns_over_weekstart_window(["rev_payu_dls_voice_nonp2p"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_rev_alt_digital_mms_cnt_sum"],
                sum_of_columns_over_weekstart_window(["rev_alt_digital_mms_cnt"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_rev_alt_digital_mms_p2p_sum"],
                sum_of_columns_over_weekstart_window(["rev_alt_digital_mms_p2p"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_rev_alt_digital_music_sum"],
                sum_of_columns_over_weekstart_window(["rev_alt_digital_music"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_rev_alt_digital_other_vas_sum"],
                sum_of_columns_over_weekstart_window(["rev_alt_digital_other_vas"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_rev_alt_digital_rbt_sum"],
                sum_of_columns_over_weekstart_window(["rev_alt_digital_rbt"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_rev_alt_digital_sms_nonp2p_sum"],
                sum_of_columns_over_weekstart_window(["rev_alt_digital_sms_nonp2p"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_rev_alt_digital_tp_sum"],
                sum_of_columns_over_weekstart_window(["rev_alt_digital_tp"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_rev_alt_digital_ussd_sum"],
                sum_of_columns_over_weekstart_window(["rev_alt_digital_ussd"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_rev_alt_digital_videocall_sum"],
                sum_of_columns_over_weekstart_window(["rev_alt_digital_videocall"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_features,
                config_feature_revenue["fea_rev_alt_digital_voice_nonp2p_sum"],
                sum_of_columns_over_weekstart_window(["rev_alt_digital_voice_nonp2p"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["weekstart", "msisdn"],
        how="outer",
    )

    output_features = get_config_based_features_column_names(
        config_feature_revenue["fea_rev_tot_sum"],
        config_feature_revenue["fea_rev_roam_tot_sum"],
        config_feature_revenue["fea_rev_voice_payu_sum"],
        config_feature_revenue["fea_rev_voice_pkg_sum"],
        config_feature_revenue["fea_rev_data_payu_sum"],
        config_feature_revenue["fea_rev_data_pkg_sum"],
        config_feature_revenue["fea_rev_sms_payu_sum"],
        config_feature_revenue["fea_rev_sms_pkg_sum"],
        config_feature_revenue["fea_rev_roam_payu_sum"],
        config_feature_revenue["fea_rev_roam_pkg_sum"],
        config_feature_revenue["fea_rev_dls_mms_cnt_payu_sum"],
        config_feature_revenue["fea_rev_dls_mms_p2p_payu_sum"],
        config_feature_revenue["fea_rev_dls_music_payu_sum"],
        config_feature_revenue["fea_rev_dls_other_vas_payu_sum"],
        config_feature_revenue["fea_rev_dls_rbt_payu_sum"],
        config_feature_revenue["fea_rev_dls_sms_nonp2p_payu_sum"],
        config_feature_revenue["fea_rev_dls_tp_payu_sum"],
        config_feature_revenue["fea_rev_dls_ussd_payu_sum"],
        config_feature_revenue["fea_rev_dls_videocall_payu_sum"],
        config_feature_revenue["fea_rev_dls_voice_nonp2p_payu_sum"],
        config_feature_revenue["fea_rev_alt_digital_mms_cnt_sum"],
        config_feature_revenue["fea_rev_alt_digital_mms_p2p_sum"],
        config_feature_revenue["fea_rev_alt_digital_music_sum"],
        config_feature_revenue["fea_rev_alt_digital_other_vas_sum"],
        config_feature_revenue["fea_rev_alt_digital_rbt_sum"],
        config_feature_revenue["fea_rev_alt_digital_sms_nonp2p_sum"],
        config_feature_revenue["fea_rev_alt_digital_tp_sum"],
        config_feature_revenue["fea_rev_alt_digital_ussd_sum"],
        config_feature_revenue["fea_rev_alt_digital_videocall_sum"],
        config_feature_revenue["fea_rev_alt_digital_voice_nonp2p_sum"],
        config_feature_revenue["fea_rev_alt_tot_sum"],
        config_feature_revenue["fea_rev_alt_voice_tot_sum"],
        config_feature_revenue["fea_rev_alt_digital_tot_sum"],
        config_feature_revenue["fea_rev_alt_roam_tot_sum"],
        config_feature_revenue["fea_rev_alt_voice_pkg_sum"],
        config_feature_revenue["fea_rev_alt_data_pkg_sum"],
        config_feature_revenue["fea_rev_alt_sms_pkg_sum"],
        config_feature_revenue["fea_rev_alt_roam_pkg_sum"],
        config_feature_revenue["fea_rev_alt_non_loan_tot_sum"],
        config_feature_revenue["fea_rev_alt_loan_tot_sum"],
        config_feature_revenue["fea_rev_alt_airtime_loan_repayment_transactions"],
    )

    required_output_columns = get_required_output_columns(
        output_features=output_features,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    df_features = df_features.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return df_features.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )


def get_base_payu_pkg_rev(
    df_revenue: pyspark.sql.DataFrame, config_feature_revenue: dict
) -> pyspark.sql.DataFrame:
    df_revenue = get_tot_rev(df_revenue, config_feature_revenue)
    df_revenue.cache()
    df_revenue = join_all(
        [
            df_revenue,
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_voice_payu_sum"],
                sum_of_columns_over_weekstart_window(["rev_payu_voice"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_voice_pkg_sum"],
                sum_of_columns_over_weekstart_window(["rev_pkg_voice"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_data_payu_sum"],
                sum_of_columns_over_weekstart_window(["rev_payu_data"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_data_pkg_sum"],
                sum_of_columns_over_weekstart_window(["rev_pkg_data"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_sms_payu_sum"],
                sum_of_columns_over_weekstart_window(["rev_payu_sms"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_sms_pkg_sum"],
                sum_of_columns_over_weekstart_window(["rev_pkg_sms"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_roam_payu_sum"],
                sum_of_columns_over_weekstart_window(["rev_payu_roam"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_roam_pkg_sum"],
                sum_of_columns_over_weekstart_window(["rev_pkg_roam"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_alt_data_pkg_sum"],
                sum_of_columns_over_weekstart_window(["rev_alt_data_pkg"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_alt_sms_pkg_sum"],
                sum_of_columns_over_weekstart_window(["rev_alt_sms_pkg"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_alt_roam_pkg_sum"],
                sum_of_columns_over_weekstart_window(["rev_alt_roam_pkg"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_alt_voice_pkg_sum"],
                sum_of_columns_over_weekstart_window(["rev_alt_voice_pkg"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["weekstart", "msisdn"],
        how="outer",
    )
    return df_revenue


def get_tot_rev(
    df_revenue: pyspark.sql.DataFrame, config_feature_revenue: dict
) -> pyspark.sql.DataFrame:
    df_revenue.cache()
    df_revenue = join_all(
        [
            df_revenue,
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_tot_sum"],
                sum_of_columns_over_weekstart_window(["rev_payu_tot", "rev_pkg_tot"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_voice_tot_sum"],
                sum_of_columns_over_weekstart_window(["rev_voice"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_data_tot_sum"],
                sum_of_columns_over_weekstart_window(["rev_data"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_sms_tot_sum"],
                sum_of_columns_over_weekstart_window(["rev_sms"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_roam_tot_sum"],
                sum_of_columns_over_weekstart_window(["rev_payu_roam", "rev_pkg_roam"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_alt_tot_sum"],
                sum_of_columns_over_weekstart_window(["rev_alt_total"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_alt_non_loan_tot_sum"],
                sum_of_columns_over_weekstart_window(["non_loan_revenue"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_alt_loan_tot_sum"],
                sum_of_columns_over_weekstart_window(["loan_revenue"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue[
                    "fea_rev_alt_airtime_loan_repayment_transactions"
                ],
                sum_of_columns_over_weekstart_window(["loan_repayment_transactions"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_alt_voice_tot_sum"],
                sum_of_columns_over_weekstart_window(["rev_alt_voice"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_alt_data_tot_sum"],
                sum_of_columns_over_weekstart_window(["rev_alt_data"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_alt_sms_tot_sum"],
                sum_of_columns_over_weekstart_window(["rev_alt_sms"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_alt_digital_tot_sum"],
                sum_of_columns_over_weekstart_window(["rev_alt_digital"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
            get_config_based_features(
                df_revenue,
                config_feature_revenue["fea_rev_alt_roam_tot_sum"],
                sum_of_columns_over_weekstart_window(["rev_alt_roam"]),
                return_only_feature_columns=True,
                columns_to_keep_from_original_df=["msisdn", "weekstart"],
            ),
        ],
        on=["weekstart", "msisdn"],
        how="outer",
    )
    return df_revenue


SELECT_AGG_COLUMNS = [
    "msisdn",
    "weekstart",
    "rev_voice",
    "rev_sms",
    "rev_data",
    "rev_payu_tot",
    "rev_payu_voice",
    "rev_payu_sms",
    "rev_payu_data",
    "rev_payu_roam",
    "rev_payu_dls_mms_cnt",
    "rev_payu_dls_mms_p2p",
    "rev_payu_dls_music",
    "rev_payu_dls_other_vas",
    "rev_payu_dls_rbt",
    "rev_payu_dls_sms_nonp2p",
    "rev_payu_dls_tp",
    "rev_payu_dls_ussd",
    "rev_payu_dls_videocall",
    "rev_payu_dls_voice_nonp2p",
    "rev_pkg_tot",
    "rev_pkg_voice",
    "rev_pkg_sms",
    "rev_pkg_data",
    "rev_pkg_roam",
    "rev_alt_total",
    "non_loan_revenue",
    "loan_revenue",
    "loan_repayment_transactions",
    "rev_alt_voice",
    "rev_alt_voice_pkg",
    "rev_alt_data",
    "rev_alt_data_pkg",
    "rev_alt_sms",
    "rev_alt_sms_pkg",
    "rev_alt_digital",
    "rev_alt_roam",
    "rev_alt_roam_pkg",
    "rev_alt_digital_mms_cnt",
    "rev_alt_digital_mms_p2p",
    "rev_alt_digital_music",
    "rev_alt_digital_other_vas",
    "rev_alt_digital_rbt",
    "rev_alt_digital_sms_nonp2p",
    "rev_alt_digital_tp",
    "rev_alt_digital_ussd",
    "rev_alt_digital_voice_nonp2p",
    "rev_alt_digital_videocall",
]
