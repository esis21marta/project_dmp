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

from utils import get_end_date, get_required_output_columns, get_start_date, join_all


def count_unique_list_msisdn(
    df: pyspark.sql.DataFrame, array_col: str, alias: str
) -> pyspark.sql.DataFrame:

    df_unique_list = df.groupBy("msisdn").agg(
        f.size(f.array_distinct(f.flatten(f.collect_set(f.col(array_col))))).alias(
            alias
        )
    )

    return df_unique_list


def fea_voice_calling_covid(
    df_voice_calling_pre_covid: pyspark.sql.DataFrame,
    df_voice_calling_covid: pyspark.sql.DataFrame,
    df_voice_calling_post_covid: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Voice Calling Feature Data

    Args:
        df_voice_calling_pre_covid: Voice Calling Weekly Aggregated Data
        df_voice_calling_covid: Voice Calling Weekly Aggregated Data
        df_voice_calling_post_covid: Voice Calling Weekly Aggregated Data

    Returns:
        df_voice_features: Voice Calling Feature Data
    """

    df_voice_calling_pre_covid = df_voice_calling_pre_covid.withColumn(
        "not_any_out_call", f.when(f.size(f.col("out_call_nums")) > 0, 0).otherwise(1)
    ).withColumn(
        "not_any_inc_call", f.when(f.size(f.col("in_call_nums")) > 0, 0).otherwise(1)
    )

    df_voice_features = df_voice_calling_pre_covid.groupBy("msisdn").agg(
        f.sum(f.col("not_any_inc_call")).alias("fea_voice_tot_not_any_inc_pre_covid"),
        f.sum(f.col("not_any_out_call")).alias("fea_voice_tot_not_any_out_pre_covid"),
    )

    df_voice_features = join_all(
        [
            df_voice_features,
            count_unique_list_msisdn(
                df_voice_calling_pre_covid,
                "out_call_nums",
                "count_unique_out_calls_pre_covid",
            ),
            count_unique_list_msisdn(
                df_voice_calling_covid, "out_call_nums", "count_unique_out_calls_covid"
            ),
            count_unique_list_msisdn(
                df_voice_calling_post_covid,
                "out_call_nums",
                "count_out_calls_post_covid",
            ),
        ],
        on=["msisdn"],
        how="outer",
    )

    df_voice_features = df_voice_features.withColumn(
        "fea_voice_uniq_out_call_ratio_pre_covid_covid",
        f.col("count_unique_out_calls_pre_covid")
        / (
            f.col("count_unique_out_calls_pre_covid")
            + f.col("count_unique_out_calls_covid")
        ),
    ).withColumn(
        "fea_voice_uniq_out_call_ratio_pre_covid_post_covid",
        f.col("count_unique_out_calls_pre_covid")
        / (
            f.col("count_unique_out_calls_pre_covid")
            + f.col("count_out_calls_post_covid")
        ),
    )

    output_features = [
        "fea_voice_tot_not_any_inc_pre_covid",
        "fea_voice_tot_not_any_out_pre_covid",
        "fea_voice_uniq_out_call_ratio_pre_covid_covid",
        "fea_voice_uniq_out_call_ratio_pre_covid_post_covid",
    ]

    required_output_columns = get_required_output_columns(
        output_features=output_features,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn"],
    )

    df_voice_features = df_voice_features.select(required_output_columns)

    return df_voice_features


def fea_voice_calling_covid_final(
    df_voice_calling: pyspark.sql.DataFrame,
    df_voice_calling_covid_final: pyspark.sql.DataFrame,
    pre_covid_last_weekstart: str,
    covid_last_weekstart: str,
    post_covid_last_weekstart: str,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Voice Calling Covid Feature Data

    Args:
        df_voice_calling: Voice Calling Weekly Aggregated Data
        df_voice_calling_covid: Voice Calling covid feature with single value of msisdn (without weekstart)

    Returns:
        df_voice_covid_features: Voice Calling Covid Feature with weekstart
    """
    df_voice_calling = df_voice_calling.select("msisdn", "weekstart")

    df_voice_features_pre_covid = join_all(
        [
            df_voice_calling.filter(f.col("weekstart") > pre_covid_last_weekstart),
            df_voice_calling_covid_final.select(
                "msisdn",
                "fea_voice_tot_not_any_inc_pre_covid",
                "fea_voice_tot_not_any_out_pre_covid",
            ),
        ],
        on=["msisdn"],
        how="left",
    )

    df_voice_features_post_covid = join_all(
        [
            df_voice_calling.filter(f.col("weekstart") > post_covid_last_weekstart),
            df_voice_calling_covid_final.select(
                "msisdn",
                "fea_voice_uniq_out_call_ratio_pre_covid_covid",
                "fea_voice_uniq_out_call_ratio_pre_covid_post_covid",
            ),
        ],
        on=["msisdn"],
        how="left",
    )

    df_voice_features_all = join_all(
        [df_voice_calling, df_voice_features_pre_covid, df_voice_features_post_covid,],
        on=["msisdn", "weekstart"],
        how="left",
    )

    output_features = [
        "fea_voice_tot_not_any_inc_pre_covid",
        "fea_voice_tot_not_any_out_pre_covid",
        "fea_voice_uniq_out_call_ratio_pre_covid_covid",
        "fea_voice_uniq_out_call_ratio_pre_covid_post_covid",
    ]

    required_output_columns = get_required_output_columns(
        output_features=output_features,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    df_voice_features_all = df_voice_features_all.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return df_voice_features_all.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )
