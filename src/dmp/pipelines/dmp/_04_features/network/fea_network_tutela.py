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
    join_all,
)


def fea_network_tutela(
    df_tutela_weekly: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:

    gap_cols = [
        "4g_device_share_gap",
        "2g_3g_device_share_gap",
        "4g_enodeb_share_gap",
        "4g_coverage_km2_gap",
        "2g_3g_coverage_km2_gap",
        "4g_coverage_cell_bandwidth_gap",
        "2g_3g_coverage_cell_bandwidth_gap",
        "tsel_4g_device_share",
        "tsel_4g_enodeb_share",
        "tsel_2g_3g_device_share",
    ]

    df_share_gap = df_tutela_weekly.withColumn(
        f"fea_network_{gap_cols[0]}_03m",
        f.avg(gap_cols[0]).over(
            get_rolling_window(
                7 * 12, key="location", oby="weekstart"
            )  # 1m is defined as 4 weeks in Tutela, so 3m is defined as 7 * 12 days for tutela.
        ),
    )
    for col in gap_cols[1:]:
        df_share_gap = df_share_gap.withColumn(
            f"fea_network_{col}_03m",
            f.avg(col).over(
                get_rolling_window(
                    7 * 12, key="location", oby="weekstart"
                )  # 1m is defined as 4 weeks in Tutela, so 3m is defined as 7 * 12 days for tutela.
            ),
        )

    sample_quality_cols = [
        "tsel_4g_avg_excellent_quality",
        "tsel_4g_avg_hd_quality",
        "tsel_4g_avg_good_quality",
        "tsel_4g_avg_game_parameter",
        "tsel_4g_avg_video_score",
        "tsel_2g_3g_avg_excellent_quality",
        "tsel_2g_3g_avg_hd_quality",
        "tsel_2g_3g_avg_good_quality",
        "tsel_2g_3g_avg_game_parameter",
        "tsel_2g_3g_avg_video_score",
        "4g_avg_download_throughput_gap",
        "2g_3g_avg_download_throughput_gap",
        "4g_avg_upload_throughput_gap",
        "2g_3g_avg_upload_throughput_gap",
        "4g_avg_latency_gap",
        "2g_3g_avg_latency_gap",
        "tsel_4g_avg_download_throughput",
        "tsel_2g_3g_avg_download_throughput",
        "tsel_4g_avg_upload_throughput",
        "tsel_2g_3g_avg_upload_throughput",
        "tsel_4g_avg_latency",
        "tsel_2g_3g_avg_latency",
    ]

    df_sample_quality = df_tutela_weekly.withColumn(
        f"fea_network_{sample_quality_cols[0]}_01m",
        f.avg(sample_quality_cols[0]).over(
            get_rolling_window(7 * 4, key="location", oby="weekstart")
        ),
    )
    for col in sample_quality_cols[1:]:
        df_sample_quality = df_sample_quality.withColumn(
            f"fea_network_{col}_01m",
            f.avg(col).over(get_rolling_window(7 * 4, key="location", oby="weekstart")),
        )

    signal_cols = [
        # signal
        "tsel_4g_signal_good",
        "tsel_4g_sample",
        "tsel_4g_signal_fair",
        "tsel_4g_signal_bad",
        "tsel_2g_3g_signal_good",
        "tsel_2g_3g_sample",
        "tsel_2g_3g_signal_fair",
        "tsel_2g_3g_signal_bad",
    ]

    df_signal_share = df_tutela_weekly.withColumn(
        f"fea_network_{signal_cols[0]}_01m",
        f.sum(signal_cols[0]).over(
            get_rolling_window(7 * 4, key="location", oby="weekstart")
        ),
    )
    for col in signal_cols[1:]:
        df_signal_share = df_signal_share.withColumn(
            f"fea_network_{col}_01m",
            f.sum(col).over(get_rolling_window(7 * 4, key="location", oby="weekstart")),
        )

    df_signal_share = (
        df_signal_share.withColumn(
            "fea_network_tsel_4g_share_of_signal_good_01m",
            f.col("fea_network_tsel_4g_signal_good_01m")
            / f.col("fea_network_tsel_4g_sample_01m"),
        )
        .withColumn(
            "fea_network_tsel_4g_share_of_signal_fair_01m",
            f.col("fea_network_tsel_4g_signal_fair_01m")
            / f.col("fea_network_tsel_4g_sample_01m"),
        )
        .withColumn(
            "fea_network_tsel_4g_share_of_signal_bad_01m",
            f.col("fea_network_tsel_4g_signal_bad_01m")
            / f.col("fea_network_tsel_4g_sample_01m"),
        )
        .withColumn(
            "fea_network_tsel_2g_3g_share_of_signal_good_01m",
            f.col("fea_network_tsel_2g_3g_signal_good_01m")
            / f.col("fea_network_tsel_2g_3g_sample_01m"),
        )
        .withColumn(
            "fea_network_tsel_2g_3g_share_of_signal_fair_01m",
            f.col("fea_network_tsel_2g_3g_signal_fair_01m")
            / f.col("fea_network_tsel_2g_3g_sample_01m"),
        )
        .withColumn(
            "fea_network_tsel_2g_3g_share_of_signal_bad_01m",
            f.col("fea_network_tsel_2g_3g_signal_bad_01m")
            / f.col("fea_network_tsel_2g_3g_sample_01m"),
        )
    )

    throughput_cols = [
        # 4g download
        "tsel_4g_download_p10",
        "tsel_4g_download_p25",
        "tsel_4g_avg_download_throughput",
        "tsel_4g_download_p75",
        "tsel_4g_download_p90",
        "competitor_4g_avg_download_throughput",
        # 2g_3g download
        "tsel_2g_3g_download_p10",
        "tsel_2g_3g_download_p25",
        "tsel_2g_3g_avg_download_throughput",
        "tsel_2g_3g_download_p75",
        "tsel_2g_3g_download_p90",
        "competitor_2g_3g_avg_download_throughput",
        # 4g upload
        "tsel_4g_upload_p10",
        "tsel_4g_upload_p25",
        "tsel_4g_avg_upload_throughput",
        "tsel_4g_upload_p75",
        "tsel_4g_upload_p90",
        "competitor_4g_avg_upload_throughput",
        # 2g_3g upload
        "tsel_2g_3g_upload_p10",
        "tsel_2g_3g_upload_p25",
        "tsel_2g_3g_avg_upload_throughput",
        "tsel_2g_3g_upload_p75",
        "tsel_2g_3g_upload_p90",
        "competitor_2g_3g_avg_upload_throughput",
        # 4g latency
        "tsel_4g_latency_p10",
        "tsel_4g_latency_p25",
        "tsel_4g_avg_latency",
        "tsel_4g_latency_p75",
        "tsel_4g_latency_p90",
        "competitor_4g_avg_latency",
        # 2g_3g latency
        "tsel_2g_3g_latency_p10",
        "tsel_2g_3g_latency_p25",
        "tsel_2g_3g_avg_latency",
        "tsel_2g_3g_latency_p75",
        "tsel_2g_3g_latency_p90",
        "competitor_2g_3g_avg_latency",
    ]

    df_max_pctl_features = df_tutela_weekly.withColumn(
        f"fea_network_max_{throughput_cols[0]}_01m",
        f.max(throughput_cols[0]).over(
            get_rolling_window(7 * 4, key="location", oby="weekstart")
        ),
    )
    for col in throughput_cols[1:]:
        df_max_pctl_features = df_max_pctl_features.withColumn(
            f"fea_network_max_{col}_01m",
            f.max(col).over(get_rolling_window(7 * 4, key="location", oby="weekstart")),
        )

    df_inflection_pctl_features = (
        df_max_pctl_features.withColumn(
            "fea_network_4g_download_inflection_pctl_wrt_competitor_01m",
            f.when(
                f.col("fea_network_max_tsel_4g_download_p10_01m")
                > f.col("fea_network_max_competitor_4g_avg_download_throughput_01m"),
                "p10",
            )
            .when(
                f.col("fea_network_max_tsel_4g_download_p25_01m")
                > f.col("fea_network_max_competitor_4g_avg_download_throughput_01m"),
                "p25",
            )
            .when(
                f.col("fea_network_max_tsel_4g_avg_download_throughput_01m")
                > f.col("fea_network_max_competitor_4g_avg_download_throughput_01m"),
                "avg",
            )
            .when(
                f.col("fea_network_max_tsel_4g_download_p75_01m")
                > f.col("fea_network_max_competitor_4g_avg_download_throughput_01m"),
                "p75",
            )
            .when(
                f.col("fea_network_max_tsel_4g_download_p90_01m")
                > f.col("fea_network_max_competitor_4g_avg_download_throughput_01m"),
                "p90",
            )
            .otherwise("lower_than_competitor_p10"),
        )
        .withColumn(
            "fea_network_4g_upload_inflection_pctl_wrt_competitor_01m",
            f.when(
                f.col("fea_network_max_tsel_4g_upload_p10_01m")
                > f.col("fea_network_max_competitor_4g_avg_upload_throughput_01m"),
                "p10",
            )
            .when(
                f.col("fea_network_max_tsel_4g_upload_p25_01m")
                > f.col("fea_network_max_competitor_4g_avg_upload_throughput_01m"),
                "p25",
            )
            .when(
                f.col("fea_network_max_tsel_4g_avg_upload_throughput_01m")
                > f.col("fea_network_max_competitor_4g_avg_upload_throughput_01m"),
                "avg",
            )
            .when(
                f.col("fea_network_max_tsel_4g_upload_p75_01m")
                > f.col("fea_network_max_competitor_4g_avg_upload_throughput_01m"),
                "p75",
            )
            .when(
                f.col("fea_network_max_tsel_4g_upload_p90_01m")
                > f.col("fea_network_max_competitor_4g_avg_upload_throughput_01m"),
                "p90",
            )
            .otherwise("lower_than_competitor_p10"),
        )
        .withColumn(
            "fea_network_4g_latency_inflection_pctl_wrt_competitor_01m",
            f.when(
                f.col("fea_network_max_tsel_4g_latency_p10_01m")
                > f.col("fea_network_max_competitor_4g_avg_latency_01m"),
                "p10",
            )
            .when(
                f.col("fea_network_max_tsel_4g_latency_p25_01m")
                > f.col("fea_network_max_competitor_4g_avg_latency_01m"),
                "p25",
            )
            .when(
                f.col("fea_network_max_tsel_4g_avg_latency_01m")
                > f.col("fea_network_max_competitor_4g_avg_latency_01m"),
                "avg",
            )
            .when(
                f.col("fea_network_max_tsel_4g_latency_p75_01m")
                > f.col("fea_network_max_competitor_4g_avg_latency_01m"),
                "p75",
            )
            .when(
                f.col("fea_network_max_tsel_4g_latency_p90_01m")
                > f.col("fea_network_max_competitor_4g_avg_latency_01m"),
                "p90",
            )
            .otherwise("lower_than_competitor_p10"),
        )
        .withColumn(
            "fea_network_2g_3g_download_inflection_pctl_wrt_competitor_01m",
            f.when(
                f.col("fea_network_max_tsel_2g_3g_download_p10_01m")
                > f.col("fea_network_max_competitor_2g_3g_avg_download_throughput_01m"),
                "p10",
            )
            .when(
                f.col("fea_network_max_tsel_2g_3g_download_p25_01m")
                > f.col("fea_network_max_competitor_2g_3g_avg_download_throughput_01m"),
                "p25",
            )
            .when(
                f.col("fea_network_max_tsel_2g_3g_avg_download_throughput_01m")
                > f.col("fea_network_max_competitor_2g_3g_avg_download_throughput_01m"),
                "avg",
            )
            .when(
                f.col("fea_network_max_tsel_2g_3g_download_p75_01m")
                > f.col("fea_network_max_competitor_2g_3g_avg_download_throughput_01m"),
                "p75",
            )
            .when(
                f.col("fea_network_max_tsel_2g_3g_download_p90_01m")
                > f.col("fea_network_max_competitor_2g_3g_avg_download_throughput_01m"),
                "p90",
            )
            .otherwise("lower_than_competitor_p10"),
        )
        .withColumn(
            "fea_network_2g_3g_upload_inflection_pctl_wrt_competitor_01m",
            f.when(
                f.col("fea_network_max_tsel_2g_3g_upload_p10_01m")
                > f.col("fea_network_max_competitor_2g_3g_avg_upload_throughput_01m"),
                "p10",
            )
            .when(
                f.col("fea_network_max_tsel_2g_3g_upload_p25_01m")
                > f.col("fea_network_max_competitor_2g_3g_avg_upload_throughput_01m"),
                "p25",
            )
            .when(
                f.col("fea_network_max_tsel_2g_3g_avg_upload_throughput_01m")
                > f.col("fea_network_max_competitor_2g_3g_avg_upload_throughput_01m"),
                "avg",
            )
            .when(
                f.col("fea_network_max_tsel_2g_3g_upload_p75_01m")
                > f.col("fea_network_max_competitor_2g_3g_avg_upload_throughput_01m"),
                "p75",
            )
            .when(
                f.col("fea_network_max_tsel_2g_3g_upload_p90_01m")
                > f.col("fea_network_max_competitor_2g_3g_avg_upload_throughput_01m"),
                "p90",
            )
            .otherwise("lower_than_competitor_p10"),
        )
        .withColumn(
            "fea_network_2g_3g_latency_inflection_pctl_wrt_competitor_01m",
            f.when(
                f.col("fea_network_max_tsel_2g_3g_latency_p10_01m")
                > f.col("fea_network_max_competitor_2g_3g_avg_latency_01m"),
                "p10",
            )
            .when(
                f.col("fea_network_max_tsel_2g_3g_latency_p25_01m")
                > f.col("fea_network_max_competitor_2g_3g_avg_latency_01m"),
                "p25",
            )
            .when(
                f.col("fea_network_max_tsel_2g_3g_avg_latency_01m")
                > f.col("fea_network_max_competitor_2g_3g_avg_latency_01m"),
                "avg",
            )
            .when(
                f.col("fea_network_max_tsel_2g_3g_latency_p75_01m")
                > f.col("fea_network_max_competitor_2g_3g_avg_latency_01m"),
                "p75",
            )
            .when(
                f.col("fea_network_max_tsel_2g_3g_latency_p90_01m")
                > f.col("fea_network_max_competitor_2g_3g_avg_latency_01m"),
                "p90",
            )
            .otherwise("lower_than_competitor_p10"),
        )
    )

    df_final = join_all(
        [df_share_gap, df_sample_quality, df_signal_share, df_inflection_pctl_features],
        on=["location", "weekstart"],
        how="outer",
    )

    df_final = df_final.withColumn(
        "fea_network_tsel_4g_avg_download_throughput_higher_than_competitor_01m",
        f.when(
            f.col("fea_network_tsel_4g_avg_download_throughput_01m")
            > f.col("fea_network_max_competitor_4g_avg_download_throughput_01m"),
            1,
        ).otherwise(0),
    )
    df_final = df_final.withColumn(
        "fea_network_tsel_4g_avg_upload_throughput_higher_than_competitor_01m",
        f.when(
            f.col("fea_network_tsel_4g_avg_upload_throughput_01m")
            > f.col("fea_network_max_competitor_4g_avg_upload_throughput_01m"),
            1,
        ).otherwise(0),
    )
    df_final = df_final.withColumn(
        "fea_network_tsel_4g_avg_latency_higher_than_competitor_01m",
        f.when(
            f.col("fea_network_tsel_4g_avg_latency_01m")
            > f.col("fea_network_max_competitor_4g_avg_latency_01m"),
            1,
        ).otherwise(0),
    )
    df_final = df_final.withColumn(
        "fea_network_tsel_2g_3g_avg_download_throughput_higher_than_competitor_01m",
        f.when(
            f.col("fea_network_tsel_2g_3g_avg_download_throughput_01m")
            > f.col("fea_network_max_competitor_2g_3g_avg_download_throughput_01m"),
            1,
        ).otherwise(0),
    )
    df_final = df_final.withColumn(
        "fea_network_tsel_2g_3g_avg_upload_throughput_higher_than_competitor_01m",
        f.when(
            f.col("fea_network_tsel_2g_3g_avg_upload_throughput_01m")
            > f.col("fea_network_max_competitor_2g_3g_avg_upload_throughput_01m"),
            1,
        ).otherwise(0),
    )
    df_final = df_final.withColumn(
        "fea_network_tsel_2g_3g_avg_latency_higher_than_competitor_01m",
        f.when(
            f.col("fea_network_tsel_2g_3g_avg_latency_01m")
            > f.col("fea_network_max_competitor_2g_3g_avg_latency_01m"),
            1,
        ).otherwise(0),
    )

    output_features = [
        "fea_network_4g_device_share_gap_03m",
        "fea_network_2g_3g_device_share_gap_03m",
        "fea_network_4g_enodeb_share_gap_03m",
        "fea_network_4g_coverage_km2_gap_03m",
        "fea_network_2g_3g_coverage_km2_gap_03m",
        "fea_network_4g_coverage_cell_bandwidth_gap_03m",
        "fea_network_2g_3g_coverage_cell_bandwidth_gap_03m",
        "fea_network_tsel_4g_device_share_03m",
        "fea_network_tsel_4g_enodeb_share_03m",
        "fea_network_tsel_2g_3g_device_share_03m",
        "fea_network_4g_avg_download_throughput_gap_01m",
        "fea_network_2g_3g_avg_download_throughput_gap_01m",
        "fea_network_4g_avg_upload_throughput_gap_01m",
        "fea_network_2g_3g_avg_upload_throughput_gap_01m",
        "fea_network_4g_avg_latency_gap_01m",
        "fea_network_2g_3g_avg_latency_gap_01m",
        "fea_network_tsel_4g_share_of_signal_good_01m",
        "fea_network_tsel_4g_share_of_signal_fair_01m",
        "fea_network_tsel_4g_share_of_signal_bad_01m",
        "fea_network_tsel_2g_3g_share_of_signal_good_01m",
        "fea_network_tsel_2g_3g_share_of_signal_fair_01m",
        "fea_network_tsel_2g_3g_share_of_signal_bad_01m",
        "fea_network_4g_download_inflection_pctl_wrt_competitor_01m",
        "fea_network_4g_upload_inflection_pctl_wrt_competitor_01m",
        "fea_network_4g_latency_inflection_pctl_wrt_competitor_01m",
        "fea_network_2g_3g_download_inflection_pctl_wrt_competitor_01m",
        "fea_network_2g_3g_upload_inflection_pctl_wrt_competitor_01m",
        "fea_network_2g_3g_latency_inflection_pctl_wrt_competitor_01m",
        "fea_network_tsel_4g_avg_download_throughput_higher_than_competitor_01m",
        "fea_network_tsel_4g_avg_upload_throughput_higher_than_competitor_01m",
        "fea_network_tsel_4g_avg_latency_higher_than_competitor_01m",
        "fea_network_tsel_2g_3g_avg_download_throughput_higher_than_competitor_01m",
        "fea_network_tsel_2g_3g_avg_upload_throughput_higher_than_competitor_01m",
        "fea_network_tsel_2g_3g_avg_latency_higher_than_competitor_01m",
        "fea_network_tsel_4g_avg_excellent_quality_01m",
        "fea_network_tsel_4g_avg_hd_quality_01m",
        "fea_network_tsel_4g_avg_good_quality_01m",
        "fea_network_tsel_4g_avg_game_parameter_01m",
        "fea_network_tsel_4g_avg_video_score_01m",
        "fea_network_tsel_2g_3g_avg_excellent_quality_01m",
        "fea_network_tsel_2g_3g_avg_hd_quality_01m",
        "fea_network_tsel_2g_3g_avg_good_quality_01m",
        "fea_network_tsel_2g_3g_avg_game_parameter_01m",
        "fea_network_tsel_2g_3g_avg_video_score_01m",
    ]

    required_output_columns = get_required_output_columns(
        output_features=output_features,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["location", "weekstart"],
    )

    df_final = df_final.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return df_final.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )
