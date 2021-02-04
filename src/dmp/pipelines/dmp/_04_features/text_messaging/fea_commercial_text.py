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
    get_end_date,
    get_required_output_columns,
    get_rolling_window,
    get_start_date,
    max_over_weekstart_window,
)


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


def fea_comm_text_messaging(
    df_comm_txt_msg_weekly: pyspark.sql.DataFrame,
    df_kredivo_sms_recipients: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    create features base on weekly aggregated dataframe

    Args:
        df_comm_txt_msg_weekly: data frame for commercial text message base on weekly aggregation dataframe

    Returns:
        dataframe base on weekly aggregated dataframe
    """

    config_feature_sms = config_feature["sms"]

    df_kredivo_sms_recipients = df_kredivo_sms_recipients.withColumn(
        "kredivo_flag", f.lit(1)
    )
    df_kredivo_sms_recipients.cache()
    df_kredivo_msisdns = df_kredivo_sms_recipients.select("msisdn").distinct()
    df_kredivo_msisdns = df_kredivo_msisdns.withColumn(
        "fea_txt_msg_kredivo_flag_all_time", f.lit(1)
    )
    df_kredivo_msisdns.cache()
    df_government_tax_fea = df_comm_txt_msg_weekly.groupBy("msisdn", "weekstart").agg(
        f.sum(
            f.coalesce(f.col("count_txt_msg_incoming_government_tax"), f.lit(0))
        ).alias("fea_txt_msg_government_tax_incoming_count_01w")
    )

    def fea_txt_msg_government_tax_incoming_count(period_string, period):
        if period == 7:
            return f.col("fea_txt_msg_government_tax_incoming_count_01w")
        else:
            return f.sum(
                f.coalesce("fea_txt_msg_government_tax_incoming_count_01w", f.lit(0))
            ).over(get_rolling_window(period, oby="weekstart"))

    df_government_tax_fea = get_config_based_features(
        df=df_government_tax_fea,
        feature_config=config_feature_sms["fea_txt_msg_government_tax_incoming_count"],
        column_expression=fea_txt_msg_government_tax_incoming_count,
    )

    fea_df = (
        df_comm_txt_msg_weekly.withColumn(
            "incoming_count_14d",
            f.sum("incoming_count_07d").over(
                get_rolling_window(7 * 2, oby="weekstart", optional_keys=["category"])
            ),
        )
        .withColumn(
            "incoming_count_21d",
            f.sum("incoming_count_07d").over(
                get_rolling_window(7 * 3, oby="weekstart", optional_keys=["category"])
            ),
        )
        .withColumn(
            "incoming_count_01m",
            f.sum("incoming_count_07d").over(
                get_rolling_window(7 * 4, oby="weekstart", optional_keys=["category"])
            ),
        )
        .withColumn(
            "incoming_count_02m",
            f.sum("incoming_count_07d").over(
                get_rolling_window(7 * 8, oby="weekstart", optional_keys=["category"])
            ),
        )
        .withColumn(
            "incoming_count_03m",
            f.sum("incoming_count_07d").over(
                get_rolling_window(7 * 13, oby="weekstart", optional_keys=["category"])
            ),
        )
        .withColumn(
            "count_senders_07d", f.size(f.array_distinct(f.col("senders_07d"))),
        )
        .withColumn(
            "count_senders_14d",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("senders_07d")).over(
                            get_rolling_window(
                                7 * 2, oby="weekstart", optional_keys=["category"]
                            )
                        )
                    )
                )
            ),
        )
        .withColumn(
            "count_senders_21d",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("senders_07d")).over(
                            get_rolling_window(
                                7 * 3, oby="weekstart", optional_keys=["category"]
                            )
                        )
                    )
                )
            ),
        )
        .withColumn(
            "count_senders_01m",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("senders_07d")).over(
                            get_rolling_window(
                                7 * 4, oby="weekstart", optional_keys=["category"]
                            )
                        )
                    )
                )
            ),
        )
        .withColumn(
            "count_senders_02m",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("senders_07d")).over(
                            get_rolling_window(
                                7 * 8, oby="weekstart", optional_keys=["category"]
                            )
                        )
                    )
                )
            ),
        )
        .withColumn(
            "count_senders_03m",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("senders_07d")).over(
                            get_rolling_window(
                                7 * 13, oby="weekstart", optional_keys=["category"]
                            )
                        )
                    )
                )
            ),
        )
    )

    fea_df = (
        fea_df.groupBy(f.col("weekstart"), f.col("msisdn"))
        .pivot("category")
        .agg(
            f.coalesce(f.sum(f.col("incoming_count_07d")), f.lit(0)).alias(
                "incoming_count_01w"
            ),
            f.coalesce(f.sum(f.col("incoming_count_14d")), f.lit(0)).alias(
                "incoming_count_02w"
            ),
            f.coalesce(f.sum(f.col("incoming_count_21d")), f.lit(0)).alias(
                "incoming_count_03w"
            ),
            f.coalesce(f.sum(f.col("incoming_count_01m")), f.lit(0)).alias(
                "incoming_count_01m"
            ),
            f.coalesce(f.sum(f.col("incoming_count_02m")), f.lit(0)).alias(
                "incoming_count_02m"
            ),
            f.coalesce(f.sum(f.col("incoming_count_03m")), f.lit(0)).alias(
                "incoming_count_03m"
            ),
            f.coalesce(f.sum(f.col("count_senders_07d")), f.lit(0)).alias(
                "unique_senders_count_01w"
            ),
            f.coalesce(f.sum(f.col("count_senders_14d")), f.lit(0)).alias(
                "unique_senders_count_02w"
            ),
            f.coalesce(f.sum(f.col("count_senders_21d")), f.lit(0)).alias(
                "unique_senders_count_03w"
            ),
            f.coalesce(f.sum(f.col("count_senders_01m")), f.lit(0)).alias(
                "unique_senders_count_01m"
            ),
            f.coalesce(f.sum(f.col("count_senders_02m")), f.lit(0)).alias(
                "unique_senders_count_02m"
            ),
            f.coalesce(f.sum(f.col("count_senders_03m")), f.lit(0)).alias(
                "unique_senders_count_03m"
            ),
        )
    )
    fea_df = rename_feature_df(fea_df)
    fea_df = (
        fea_df.join(df_kredivo_sms_recipients, ["msisdn", "weekstart"], how="left")
        .join(df_kredivo_msisdns, ["msisdn"], how="left")
        .join(df_government_tax_fea, ["msisdn", "weekstart"], how="left")
    )

    fea_df = get_config_based_features(
        df=fea_df,
        feature_config=config_feature_sms["fea_txt_msg_kredivo_flag"],
        column_expression=max_over_weekstart_window("kredivo_flag"),
    )

    # fill null with 0 for _kredivo_flag_ features
    fea_df = fea_df.fillna(
        0, subset=[i for i in fea_df.columns if "_kredivo_flag_" in i]
    )

    required_output_columns = get_required_output_columns(
        output_features=fea_df.drop("kredivo_flag").columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    fea_df = fea_df.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return fea_df.filter(f.col("weekstart").between(first_week_start, last_week_start))
