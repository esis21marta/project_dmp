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


def rename_feature_df(fea_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Returns a dataframe with renamed features

    Args:
        fea_df: input dataframe with contain column features

    Returns:
        dataframe with renamed column features
    """
    columns = fea_df.columns
    for column in columns:
        if column not in ["msisdn", "weekstart"]:
            fea_df = fea_df.withColumnRenamed(
                column, "fea_txt_msg_" + column.replace(" ", "_")
            )
    return fea_df


def ratio_col_feat(
    df: pyspark.sql.DataFrame, list_df_a: list, list_df_b: list,
) -> pyspark.sql.DataFrame:
    """
    Returns a dataframe with ratio feature in every column listed

    Args:
        fea_df: input dataframe with contain column features
        list_df_a: input first list column
        list_df_b: input second list column

    Returns:
        dataframe with ratio feature
    """

    if len(list_df_a) == len(list_df_b):
        list_df_a.sort()
        list_df_b.sort()

        join_list = [[i, j] for i, j in zip(list_df_a, list_df_b)]

        fea_column_name = ["msisdn"]

        for column in join_list:

            col1 = column[0].replace("pre_covid", "")
            col1 = col1.replace("post_covid", "")
            col1 = col1.replace("covid", "")

            col2 = column[1].replace("pre_covid", "")
            col2 = col2.replace("post_covid", "")
            col2 = col2.replace("covid", "")

            if (column[0] not in ["msisdn", "weekstart"]) & (col1 == col2):
                if "post_covid" in column[1]:
                    fea_name = column[0].replace("count", "ratio")
                    fea_name = fea_name + "_post_covid"
                elif "covid" in column[1]:
                    fea_name = column[0].replace("count", "ratio")
                    fea_name = fea_name + "_covid"

                df = df.withColumn(
                    fea_name,
                    f.when(
                        (f.col(column[0]) != 0) & (f.col(column[1]) != 0),
                        (f.col(column[0])) / (f.col(column[0]) + f.col(column[1])),
                    ).otherwise(None),
                )

                fea_column_name.append(fea_name)
    else:
        raise Exception("Column not match. Stopping Pipeline")

    return df.select(*fea_column_name)


def fea_comm_text_messaging_covid(
    df_comm_txt_msg_weekly_pre_covid: pyspark.sql.DataFrame,
    df_comm_txt_msg_weekly_covid: pyspark.sql.DataFrame,
    df_comm_txt_msg_weekly_post_covid: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    create features base on weekly aggregated dataframe

    Args:
        df_comm_txt_msg_weekly_pre_covid: data frame for commercial text message base on weekly aggregation dataframe pre covid
        df_comm_txt_msg_weekly_covid: data frame for commercial text message base on weekly aggregation dataframe covid
        df_comm_txt_msg_weekly_post_covid: data frame for commercial text message base on weekly aggregation dataframe post covid

    Returns:
        dataframe base on weekly aggregated dataframe
    """
    # Pre Covid
    df_features_pre_covid = df_comm_txt_msg_weekly_pre_covid.groupBy(
        "msisdn", "category"
    ).agg(
        f.sum(f.col("incoming_count_07d")).alias("incoming_count_pre_covid"),
        f.size(f.array_distinct(f.flatten(f.collect_set(f.col("senders_07d"))))).alias(
            "count_unique_senders_pre_covid"
        ),
    )

    # Covid
    df_features_covid = df_comm_txt_msg_weekly_covid.groupBy("msisdn", "category").agg(
        f.sum(f.col("incoming_count_07d")).alias("incoming_count_covid"),
        f.size(f.array_distinct(f.flatten(f.collect_set(f.col("senders_07d"))))).alias(
            "count_unique_senders_covid"
        ),
    )

    # Post Covid
    df_features_post_covid = df_comm_txt_msg_weekly_post_covid.groupBy(
        "msisdn", "category"
    ).agg(
        f.sum(f.col("incoming_count_07d")).alias("incoming_count_post_covid"),
        f.size(f.array_distinct(f.flatten(f.collect_set(f.col("senders_07d"))))).alias(
            "count_unique_senders_post_covid"
        ),
    )

    ############# count data on each category ##################

    df_feat = join_all(
        [
            df_features_pre_covid.groupBy(f.col("msisdn"))
            .pivot("category")
            .agg(
                f.coalesce(f.sum(f.col("incoming_count_pre_covid")), f.lit(0)).alias(
                    "incoming_count_pre_covid"
                ),
                f.coalesce(
                    f.sum(f.col("count_unique_senders_pre_covid")), f.lit(0)
                ).alias("unique_senders_count_pre_covid"),
            ),
            df_features_covid.groupBy(f.col("msisdn"))
            .pivot("category")
            .agg(
                f.coalesce(f.sum(f.col("incoming_count_covid")), f.lit(0)).alias(
                    "incoming_count_covid"
                ),
                f.coalesce(f.sum(f.col("count_unique_senders_covid")), f.lit(0)).alias(
                    "unique_senders_count_covid"
                ),
            ),
            df_features_post_covid.groupBy(f.col("msisdn"))
            .pivot("category")
            .agg(
                f.coalesce(f.sum(f.col("incoming_count_post_covid")), f.lit(0)).alias(
                    "incoming_count_post_covid"
                ),
                f.coalesce(
                    f.sum(f.col("count_unique_senders_post_covid")), f.lit(0)
                ).alias("unique_senders_count_post_covid"),
            ),
        ],
        on=["msisdn"],
        how="full",
    )

    df_feat = rename_feature_df(df_feat)

    columns = df_feat.columns

    list_col_df_pre_covid = [k for k in columns if "_count_pre_covid" in k]
    list_col_df_covid = [k for k in columns if "_count_covid" in k]
    list_col_df_post_covid = [k for k in columns if "_count_post_covid" in k]

    fea_df = join_all(
        [
            ratio_col_feat(df_feat, list_col_df_pre_covid, list_col_df_covid,),
            ratio_col_feat(df_feat, list_col_df_pre_covid, list_col_df_post_covid,),
        ],
        on=["msisdn"],
        how="full",
    )

    required_output_columns = get_required_output_columns(
        output_features=fea_df.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn"],
    )

    fea_df = fea_df.select(required_output_columns)

    return fea_df


def fea_comm_text_messaging_covid_final(
    df_comm_txt_msg_weekly: pyspark.sql.DataFrame,
    df_comm_txt_msg_weekly_covid: pyspark.sql.DataFrame,
    pre_covid_last_weekstart: str,
    covid_last_weekstart: str,
    post_covid_last_weekstart: str,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Text Message Commercial Covid Feature Data

    Args:
        comm_txt_msg: Text Message Weekly Aggregated Data
        comm_txt_msg_covid_final: Text Message covid feature with single value of msisdn (without weekstart)

    Returns:
        df_comm_txt_msg_covid_features: Text Message Covid Feature with weekstart
    """

    df_comm_txt_msg_weekly = df_comm_txt_msg_weekly.select(
        "msisdn", "weekstart"
    ).dropDuplicates(["msisdn", "weekstart"])

    df_comm_txt_msg_features_post_covid = join_all(
        [
            df_comm_txt_msg_weekly.filter(
                f.col("weekstart") > post_covid_last_weekstart
            ),
            df_comm_txt_msg_weekly_covid,
        ],
        on=["msisdn"],
        how="left",
    )

    df_comm_txt_msg_features_all = join_all(
        [df_comm_txt_msg_weekly, df_comm_txt_msg_features_post_covid,],
        on=["msisdn", "weekstart"],
        how="left",
    )

    required_output_columns = get_required_output_columns(
        output_features=df_comm_txt_msg_features_all.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    df_comm_txt_msg_features_all = df_comm_txt_msg_features_all.select(
        required_output_columns
    )

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return df_comm_txt_msg_features_all.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )
