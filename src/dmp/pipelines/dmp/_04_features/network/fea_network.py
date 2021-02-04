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
    get_end_date,
    get_required_output_columns,
    get_rolling_window,
    get_start_date,
)


def _calculate_feature(
    df_data: pyspark.sql.DataFrame, feature: str, key: str
) -> pyspark.sql.DataFrame:
    """
    :param df_data: Network Data
    :param feature: Feature Column
    :param key: [msisdn, lac, lac_ci]
    :return: Network Features
    """

    fea_df = (
        df_data.withColumn(f"fea_network_{feature}_1w", f.col(feature),)
        .withColumn(
            f"fea_network_{feature}_2w",
            f.avg(feature).over(get_rolling_window(7 * 2, key=key, oby="weekstart")),
        )
        .withColumn(
            f"fea_network_{feature}_3w",
            f.avg(feature).over(get_rolling_window(7 * 3, key=key, oby="weekstart")),
        )
        .withColumn(
            f"fea_network_{feature}_1m",
            f.avg(feature).over(get_rolling_window(7 * 4, key=key, oby="weekstart")),
        )
        .withColumn(
            f"fea_network_{feature}_2m",
            f.avg(feature).over(get_rolling_window(7 * 8, key=key, oby="weekstart")),
        )
        .withColumn(
            f"fea_network_{feature}_3m",
            f.avg(feature).over(get_rolling_window(7 * 13, key=key, oby="weekstart")),
        )
        .withColumn(
            f"fea_network_{feature}_6m",
            f.avg(feature).over(get_rolling_window(7 * 26, key=key, oby="weekstart")),
        )
        .select(
            key,
            "weekstart",
            f"fea_network_{feature}_1w",
            f"fea_network_{feature}_2w",
            f"fea_network_{feature}_3w",
            f"fea_network_{feature}_1m",
            f"fea_network_{feature}_2m",
            f"fea_network_{feature}_3m",
            f"fea_network_{feature}_6m",
        )
    )

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return fea_df.filter(f.col("weekstart").between(first_week_start, last_week_start))


def fea_network(key: str):
    def _fea_network(
        df_network_data: pyspark.sql.DataFrame,
        feature_mode: str,
        required_output_features: List[str],
    ) -> pyspark.sql.DataFrame:
        """
        :param df_network_data: Network Data
        :return: Network Features
        """
        feature_columns = [
            "broadband_revenue_avg",
            "ccsr_cs_avg",
            "ccsr_ps_avg",
            "cssr_cs_avg",
            "cssr_ps_avg",
            "downlink_traffic_volume_mb_avg",
            "edge_mbyte_avg",
            "gprs_mbyte_avg",
            "hosr_num_sum_avg",
            "hsdpa_accesability_avg",
            "hsupa_accesability_avg",
            "hsupa_mean_user_avg",
            "ifhosr_avg",
            "max_occupancy_pct_avg",
            "max_of_traffic_voice_erl_avg",
            "capable_4g_avg",
            "payload_hspa_mbyte_avg",
            "payload_psr99_mbyte_avg",
            "total_payload_mb_avg",
            "total_throughput_kbps_avg",
            "voice_revenue_avg",
            "volume_voice_traffic_erl_avg",
            "cap_hr30_pct_frside_erl_avg",
            "capable_2g_avg",
            "capable_3g_avg",
            "hosr_avg",
            "dongle_avg",
            "hosr_denum_sum_avg",
        ]

        df_data = df_network_data.select(key, "weekstart", feature_columns[0])
        fea_df = _calculate_feature(
            df_data=df_data, feature=feature_columns[0], key=key
        )

        for feature_column in feature_columns[1:]:
            df_data = df_network_data.select(key, "weekstart", feature_column)

            df = _calculate_feature(df_data=df_data, feature=feature_column, key=key)
            fea_df = fea_df.join(df, [key, "weekstart"], how="full")

        required_output_columns = get_required_output_columns(
            output_features=fea_df.columns,
            feature_mode=feature_mode,
            feature_list=required_output_features,
            extra_columns_to_keep=[key, "weekstart"],
        )

        fea_df = fea_df.select(required_output_columns)

        return fea_df

    return _fea_network
