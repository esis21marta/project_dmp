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

from src.dmp.pipelines.dmp._04_features.product.constants import (
    OUTPUT_COLUMNS_NON_MACRO,
)
from utils import (
    get_end_date,
    get_required_output_columns,
    get_rolling_window,
    get_start_date,
)


def fea_nonmacro_product(
    df_non_macro_product_weekly: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Calculates allowance on non-macro product for each msisdn:

    Args:
        df_non_macro_product_weekly: Non Macro Product allowance weekly data

    Returns:
        fea_df: Non-Macro Product Features
    """
    fea_df = (
        df_non_macro_product_weekly.withColumn("fea_revenue_sum_01w", f.col("revenue"))
        .withColumn(
            "fea_revenue_sum_01m",
            f.sum(f.col("revenue")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_revenue_sum_03m",
            f.sum(f.col("revenue")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_tcash_balance_sum_01w", f.col("tcash_balance"))
        .withColumn(
            "fea_allowance_tcash_balance_sum_01m",
            f.sum(f.col("tcash_balance")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_tcash_balance_sum_03m",
            f.sum(f.col("tcash_balance")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_voice_offnet_sum_01w", f.col("voice_offnet"))
        .withColumn(
            "fea_allowance_voice_offnet_sum_01m",
            f.sum(f.col("voice_offnet")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_voice_offnet_sum_03m",
            f.sum(f.col("voice_offnet")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_voice_allnet_sum_01w", f.col("voice_allnet"))
        .withColumn(
            "fea_allowance_voice_allnet_sum_01m",
            f.sum(f.col("voice_allnet")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_voice_allnet_sum_03m",
            f.sum(f.col("voice_allnet")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_voice_allopr_sum_01w", f.col("voice_allopr"))
        .withColumn(
            "fea_allowance_voice_allopr_sum_01m",
            f.sum(f.col("voice_allopr")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_voice_allopr_sum_03m",
            f.sum(f.col("voice_allopr")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_voice_onnet_malam_sum_01w", f.col("voice_onnet_malam")
        )
        .withColumn(
            "fea_allowance_voice_onnet_malam_sum_01m",
            f.sum(f.col("voice_onnet_malam")).over(
                get_rolling_window(28, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_allowance_voice_onnet_malam_sum_03m",
            f.sum(f.col("voice_onnet_malam")).over(
                get_rolling_window(91, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_allowance_voice_onnet_siang_sum_01w", f.col("voice_onnet_siang")
        )
        .withColumn(
            "fea_allowance_voice_onnet_siang_sum_01m",
            f.sum(f.col("voice_onnet_siang")).over(
                get_rolling_window(28, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_allowance_voice_onnet_siang_sum_03m",
            f.sum(f.col("voice_onnet_siang")).over(
                get_rolling_window(91, oby="weekstart")
            ),
        )
        .withColumn("fea_allowance_voice_roaming_mo_sum_01w", f.col("voice_roaming_mo"))
        .withColumn(
            "fea_allowance_voice_roaming_mo_sum_01m",
            f.sum(f.col("voice_roaming_mo")).over(
                get_rolling_window(28, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_allowance_voice_roaming_mo_sum_03m",
            f.sum(f.col("voice_roaming_mo")).over(
                get_rolling_window(91, oby="weekstart")
            ),
        )
        .withColumn("fea_allowance_voice_onnet_sum_01w", f.col("voice_onnet"))
        .withColumn(
            "fea_allowance_voice_onnet_sum_01m",
            f.sum(f.col("voice_onnet")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_voice_onnet_sum_03m",
            f.sum(f.col("voice_onnet")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_voice_idd_sum_01w", f.col("voice_idd"))
        .withColumn(
            "fea_allowance_voice_idd_sum_01m",
            f.sum(f.col("voice_idd")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_voice_idd_sum_03m",
            f.sum(f.col("voice_idd")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_voice_roaming_sum_01w", f.col("voice_roaming"))
        .withColumn(
            "fea_allowance_voice_roaming_sum_01m",
            f.sum(f.col("voice_roaming")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_voice_roaming_sum_03m",
            f.sum(f.col("voice_roaming")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_data_games_sum_01w", f.col("data_games"))
        .withColumn(
            "fea_allowance_data_games_sum_01m",
            f.sum(f.col("data_games")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_data_games_sum_03m",
            f.sum(f.col("data_games")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_data_video_sum_01w", f.col("data_video"))
        .withColumn(
            "fea_allowance_data_video_sum_01m",
            f.sum(f.col("data_video")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_data_video_sum_03m",
            f.sum(f.col("data_video")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_data_dpi_sum_01w", f.col("data_dpi"))
        .withColumn(
            "fea_allowance_data_dpi_sum_01m",
            f.sum(f.col("data_dpi")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_data_dpi_sum_03m",
            f.sum(f.col("data_dpi")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_data_4g_omg_sum_01w", f.col("data_4g_omg"))
        .withColumn(
            "fea_allowance_data_4g_omg_sum_01m",
            f.sum(f.col("data_4g_omg")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_data_4g_omg_sum_03m",
            f.sum(f.col("data_4g_omg")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_data_wifi_sum_01w", f.col("data_wifi"))
        .withColumn(
            "fea_allowance_data_wifi_sum_01m",
            f.sum(f.col("data_wifi")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_data_wifi_sum_03m",
            f.sum(f.col("data_wifi")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_data_roaming_sum_01w", f.col("data_roaming"))
        .withColumn(
            "fea_allowance_data_roaming_sum_01m",
            f.sum(f.col("data_roaming")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_data_roaming_sum_03m",
            f.sum(f.col("data_roaming")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_data_allnet_sum_01w", f.col("data_allnet"))
        .withColumn(
            "fea_allowance_data_allnet_sum_01m",
            f.sum(f.col("data_allnet")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_data_allnet_sum_03m",
            f.sum(f.col("data_allnet")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_data_4g_mds_sum_01w", f.col("data_4g_mds"))
        .withColumn(
            "fea_allowance_data_4g_mds_sum_01m",
            f.sum(f.col("data_4g_mds")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_data_4g_mds_sum_03m",
            f.sum(f.col("data_4g_mds")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_data_allnet_local_sum_01w", f.col("data_allnet_local")
        )
        .withColumn(
            "fea_allowance_data_allnet_local_sum_01m",
            f.sum(f.col("data_allnet_local")).over(
                get_rolling_window(28, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_allowance_data_allnet_local_sum_03m",
            f.sum(f.col("data_allnet_local")).over(
                get_rolling_window(91, oby="weekstart")
            ),
        )
        .withColumn("fea_allowance_unlimited_data_sum_01w", f.col("unlimited_data"))
        .withColumn(
            "fea_allowance_unlimited_data_sum_01m",
            f.sum(f.col("unlimited_data")).over(
                get_rolling_window(28, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_allowance_unlimited_data_sum_03m",
            f.sum(f.col("unlimited_data")).over(
                get_rolling_window(91, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_allowance_data_allnet_siang_sum_01w", f.col("data_allnet_siang")
        )
        .withColumn(
            "fea_allowance_data_allnet_siang_sum_01m",
            f.sum(f.col("data_allnet_siang")).over(
                get_rolling_window(28, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_allowance_data_allnet_siang_sum_03m",
            f.sum(f.col("data_allnet_siang")).over(
                get_rolling_window(91, oby="weekstart")
            ),
        )
        .withColumn("fea_allowance_data_music_sum_01w", f.col("data_music"))
        .withColumn(
            "fea_allowance_data_music_sum_01m",
            f.sum(f.col("data_music")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_data_music_sum_03m",
            f.sum(f.col("data_music")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_4g_data_pool_sum_01w", f.col("fg_data_pool"))
        .withColumn(
            "fea_allowance_4g_data_pool_sum_01m",
            f.sum(f.col("fg_data_pool")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_4g_data_pool_sum_03m",
            f.sum(f.col("fg_data_pool")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_data_onnet_sum_01w", f.col("data_onnet"))
        .withColumn(
            "fea_allowance_data_onnet_sum_01m",
            f.sum(f.col("data_onnet")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_data_onnet_sum_03m",
            f.sum(f.col("data_onnet")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_data_4g_sum_01w", f.col("data_4g"))
        .withColumn(
            "fea_allowance_data_4g_sum_01m",
            f.sum(f.col("data_4g")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_data_4g_sum_03m",
            f.sum(f.col("data_4g")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_data_mds_sum_01w", f.col("data_mds"))
        .withColumn(
            "fea_allowance_data_mds_sum_01m",
            f.sum(f.col("data_mds")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_data_mds_sum_03m",
            f.sum(f.col("data_mds")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_sms_allnet_sum_01w", f.col("sms_allnet"))
        .withColumn(
            "fea_allowance_sms_allnet_sum_01m",
            f.sum(f.col("sms_allnet")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_sms_allnet_sum_03m",
            f.sum(f.col("sms_allnet")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_sms_allopr_sum_01w", f.col("sms_allopr"))
        .withColumn(
            "fea_allowance_sms_allopr_sum_01m",
            f.sum(f.col("sms_allopr")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_sms_allopr_sum_03m",
            f.sum(f.col("sms_allopr")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_sms_offnet_sum_01w", f.col("sms_offnet"))
        .withColumn(
            "fea_allowance_sms_offnet_sum_01m",
            f.sum(f.col("sms_offnet")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_sms_offnet_sum_03m",
            f.sum(f.col("sms_offnet")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_sms_roaming_sum_01w", f.col("sms_roaming"))
        .withColumn(
            "fea_allowance_sms_roaming_sum_01m",
            f.sum(f.col("sms_roaming")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_sms_roaming_sum_03m",
            f.sum(f.col("sms_roaming")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_sms_onnet_sum_01w", f.col("sms_onnet"))
        .withColumn(
            "fea_allowance_sms_onnet_sum_01m",
            f.sum(f.col("sms_onnet")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_sms_onnet_sum_03m",
            f.sum(f.col("sms_onnet")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_sms_onnet_siang_sum_01w", f.col("sms_onnet_siang"))
        .withColumn(
            "fea_allowance_sms_onnet_siang_sum_01m",
            f.sum(f.col("sms_onnet_siang")).over(
                get_rolling_window(28, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_allowance_sms_onnet_siang_sum_03m",
            f.sum(f.col("sms_onnet_siang")).over(
                get_rolling_window(91, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_allowance_subscription_tribe_sum_01w", f.col("subscription_tribe")
        )
        .withColumn(
            "fea_allowance_subscription_tribe_sum_01m",
            f.sum(f.col("subscription_tribe")).over(
                get_rolling_window(28, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_allowance_subscription_tribe_sum_03m",
            f.sum(f.col("subscription_tribe")).over(
                get_rolling_window(91, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_allowance_subscription_hooq_sum_01w", f.col("subscription_hooq")
        )
        .withColumn(
            "fea_allowance_subscription_hooq_sum_01m",
            f.sum(f.col("subscription_hooq")).over(
                get_rolling_window(28, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_allowance_subscription_hooq_sum_03m",
            f.sum(f.col("subscription_hooq")).over(
                get_rolling_window(91, oby="weekstart")
            ),
        )
        .withColumn("fea_allowance_subscription_viu_sum_01w", f.col("subscription_viu"))
        .withColumn(
            "fea_allowance_subscription_viu_sum_01m",
            f.sum(f.col("subscription_viu")).over(
                get_rolling_window(28, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_allowance_subscription_viu_sum_03m",
            f.sum(f.col("subscription_viu")).over(
                get_rolling_window(91, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_allowance_subscription_bein_sum_01w", f.col("subscription_bein")
        )
        .withColumn(
            "fea_allowance_subscription_bein_sum_01m",
            f.sum(f.col("subscription_bein")).over(
                get_rolling_window(28, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_allowance_subscription_bein_sum_03m",
            f.sum(f.col("subscription_bein")).over(
                get_rolling_window(91, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_allowance_subscription_music_sum_01w", f.col("subscription_music")
        )
        .withColumn(
            "fea_allowance_subscription_music_sum_01m",
            f.sum(f.col("subscription_music")).over(
                get_rolling_window(28, oby="weekstart")
            ),
        )
        .withColumn(
            "fea_allowance_subscription_music_sum_03m",
            f.sum(f.col("subscription_music")).over(
                get_rolling_window(91, oby="weekstart")
            ),
        )
        .withColumn("fea_allowance_monbal_monbal_sum_01w", f.col("monbal_monbal"))
        .withColumn(
            "fea_allowance_monbal_monbal_sum_01m",
            f.sum(f.col("monbal_monbal")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_monbal_monbal_sum_03m",
            f.sum(f.col("monbal_monbal")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_monbal_siang_sum_01w", f.col("monbal_siang"))
        .withColumn(
            "fea_allowance_monbal_siang_sum_01m",
            f.sum(f.col("monbal_siang")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_monbal_siang_sum_03m",
            f.sum(f.col("monbal_siang")).over(get_rolling_window(91, oby="weekstart")),
        )
        .withColumn("fea_allowance_monbal_onnet_sum_01w", f.col("monbal_onnet"))
        .withColumn(
            "fea_allowance_monbal_onnet_sum_01m",
            f.sum(f.col("monbal_onnet")).over(get_rolling_window(28, oby="weekstart")),
        )
        .withColumn(
            "fea_allowance_monbal_onnet_sum_03m",
            f.sum(f.col("monbal_onnet")).over(get_rolling_window(91, oby="weekstart")),
        )
    )

    required_output_columns = get_required_output_columns(
        output_features=OUTPUT_COLUMNS_NON_MACRO,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    fea_df = fea_df.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return fea_df.filter(f.col("weekstart").between(first_week_start, last_week_start))
