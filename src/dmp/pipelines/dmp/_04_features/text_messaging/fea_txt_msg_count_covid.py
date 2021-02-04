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


def fea_txt_msg_count_covid(
    df_txt_msg_weekly_pre_covid: pyspark.sql.DataFrame,
    df_txt_msg_weekly_covid: pyspark.sql.DataFrame,
    df_txt_msg_weekly_post_covid: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    create features base on weekly aggregated dataframe

    Args:
        df_txt_msg_weekly: data frame for text message base on weekly aggregation dataframe

    Returns:
        dataframe base on weekly aggregated dataframe
    """

    df_txt_msg_weekly_pre_covid = df_txt_msg_weekly_pre_covid.withColumn(
        "not_any_out_msg", f.when(f.col("count_txt_msg_outgoing") > 0, 0).otherwise(1)
    ).withColumn(
        "not_any_inc_msg", f.when(f.col("count_txt_msg_incoming") > 0, 0).otherwise(1)
    )

    df_features = df_txt_msg_weekly_pre_covid.groupBy("msisdn").agg(
        f.sum(f.col("not_any_out_msg")).alias("fea_txt_msg_not_any_out_msg_pre_covid"),
        f.sum(f.col("not_any_inc_msg")).alias("fea_txt_msg_not_any_inc_msg_pre_covid"),
    )

    output_features = [
        "fea_txt_msg_not_any_inc_msg_pre_covid",
        "fea_txt_msg_not_any_out_msg_pre_covid",
    ]

    required_output_columns = get_required_output_columns(
        output_features=output_features,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn"],
    )

    df_features = df_features.select(required_output_columns)

    return df_features


def fea_txt_msg_count_covid_final(
    df_txt_msg: pyspark.sql.DataFrame,
    df_txt_msg_covid_final: pyspark.sql.DataFrame,
    pre_covid_last_weekstart: str,
    covid_last_weekstart: str,
    post_covid_last_weekstart: str,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Text Message Covid Feature Data

    Args:
        df_txt_msg: Text Message Weekly Aggregated Data
        df_txt_msg_covid_final: Text Message covid feature with single value of msisdn (without weekstart)

    Returns:
        df_txt_msg_covid_features: Text Message Covid Feature with weekstart
    """

    df_txt_msg = df_txt_msg.select("msisdn", "weekstart")

    df_txt_msg_features_pre_covid = join_all(
        [
            df_txt_msg.filter(f.col("weekstart") > pre_covid_last_weekstart),
            df_txt_msg_covid_final,
        ],
        on=["msisdn"],
        how="left",
    )

    df_txt_msg_features_all = join_all(
        [df_txt_msg, df_txt_msg_features_pre_covid,],
        on=["msisdn", "weekstart"],
        how="left",
    )

    output_features = [
        "fea_txt_msg_not_any_inc_msg_pre_covid",
        "fea_txt_msg_not_any_out_msg_pre_covid",
    ]

    required_output_columns = get_required_output_columns(
        output_features=output_features,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    df_txt_msg_features_all = df_txt_msg_features_all.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return df_txt_msg_features_all.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )
