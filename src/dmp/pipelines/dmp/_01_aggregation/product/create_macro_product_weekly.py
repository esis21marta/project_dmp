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

import logging
from datetime import timedelta

import pyspark
import pyspark.sql.functions as f
import pyspark.sql.types as t
from deprecated import deprecated
from pyspark.sql.window import Window

from src.dmp.pipelines.dmp._01_aggregation.product.constants import VALIDITY_MAPPING
from utils import (
    get_config_parameters,
    get_end_date,
    get_start_date,
    next_week_start_day,
)
from utils.spark_data_set_helper import get_file_path

log = logging.getLogger(__name__)


@deprecated(version="0.1", reason="Some of product tables not productionized")
def union_macro_product_list(
    bid_macro_product_mapping_base_df: pyspark.sql.DataFrame,
    bid_macro_product_mapping_new_df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Union the base macroproduct mapping with the new one

    Args:
        bid_macro_product_mapping_base_df: Base macroproduct mapping
        bid_macro_product_mapping_new_df: New macroproduct mapping

    Returns:
        Dataframe of Macroproduct Mapping
    """
    bid_macro_product_mapping_base_df = bid_macro_product_mapping_base_df.select(
        "bid", "macroproduct"
    )
    bid_macro_product_mapping_new_df = bid_macro_product_mapping_new_df.select(
        "bid", "macroproduct", "rev_share"
    )
    bid_base = bid_macro_product_mapping_base_df.select("bid").distinct()
    bid_macro_product_mapping_new_df = bid_macro_product_mapping_new_df.join(
        f.broadcast(bid_base), ["bid"], how="left_anti"
    )
    bid_macro_product_mapping_new_df = (
        (
            bid_macro_product_mapping_new_df.withColumn(
                "rn",
                f.row_number().over(
                    Window()
                    .partitionBy(f.col("bid"))
                    .orderBy(f.col("rev_share").desc())
                ),
            )
        )
        .filter(f.col("rn") == 1)
        .select("bid", "macroproduct")
    )
    macro_product = bid_macro_product_mapping_base_df.union(
        bid_macro_product_mapping_new_df
    )

    return macro_product


@deprecated(version="0.1", reason="Some of product tables not productionized")
def _weekly_aggregation(
    base_ifrs_c2p_df: pyspark.sql.DataFrame,
    base_ifrs_c2p_reject_df: pyspark.sql.DataFrame,
    bid_macro_product_mapping_df: pyspark.sql.DataFrame,
    macro_product_list: list,
) -> pyspark.sql.DataFrame:
    """
    Aggregate base_ifrs_c2p to msisdn and weekstart

    Args:
        base_ifrs_c2p_df: Dataframe of base_ifrs_c2p table
        base_ifrs_c2p_reject_df: Dataframe of base_ifrs_c2p_reject table
        bid_macro_product_mapping_df: Base macroproduct mapping
        macro_product_list: List of macroproduct

    Returns:
        Dataframe of Macroproduct Weekly Aggregated
    """
    base_ifrs_union = base_ifrs_c2p_df.union(base_ifrs_c2p_reject_df)

    base_ifrs_macro = base_ifrs_union.join(
        f.broadcast(bid_macro_product_mapping_df), ["bid"], how="left"
    )

    base_ifrs_macro = (
        base_ifrs_macro.withColumn(
            "validity_period_days",
            f.when(
                f.lower(f.col("validity_period_unit")).contains("day"),
                f.col("validity_period"),
            )
            .when(
                f.lower(f.col("validity_period_unit")).contains("month"),
                f.col("validity_period") * f.lit(30),
            )
            .when(
                f.lower(f.col("validity_period_unit")).contains("year"),
                f.col("validity_period") * f.lit(365),
            )
            .when(
                f.lower(f.col("validity_period_unit")).contains("hour"),
                f.col("validity_period") / f.lit(24),
            ),
        )
        .withColumn(
            "weekstart",
            next_week_start_day(
                f.to_date(f.col("event_date").cast(t.StringType()), "yyyy-MM-dd")
            ),
        )
        .replace(to_replace=VALIDITY_MAPPING, subset=["validity_period_str"])
    )
    base_ifrs_macro = base_ifrs_macro.groupBy(
        "msisdn", "transaction_id", "weekstart", "macroproduct"
    ).agg(
        f.first("validity_period_days").alias("validity_period_days"),
        f.first("validity_period_str").alias("validity_period_str"),
    )

    macroproduct_agg = (
        base_ifrs_macro.groupBy("msisdn", "weekstart", "macroproduct")
        .agg(
            f.count("macroproduct").alias("count"),
            f.min(f.col("validity_period_days")).alias(
                "fea_product_min_validity_period_days_01w"
            ),
            f.max(f.col("validity_period_days")).alias(
                "fea_product_max_validity_period_days_01w"
            ),
            f.sum(
                f.when(f.col("validity_period_str") == "0to4", 1).otherwise(f.lit(0))
            ).alias("fea_product_count_validity_1_to_4_days_01w"),
            f.sum(
                f.when(f.col("validity_period_str") == "5to10", 1).otherwise(f.lit(0))
            ).alias("fea_product_count_validity_5_to_10_days_01w"),
            f.sum(
                f.when(f.col("validity_period_str") == "11to22", 1).otherwise(f.lit(0))
            ).alias("fea_product_count_validity_11_to_22_days_01w"),
            f.sum(
                f.when(f.col("validity_period_str") == "23to45", 1).otherwise(f.lit(0))
            ).alias("fea_product_count_validity_23_to_45_days_01w"),
            f.sum(
                f.when(f.col("validity_period_str") == "46plus", 1).otherwise(f.lit(0))
            ).alias("fea_product_count_validity_46plus_days_01w"),
        )
        .withColumn(
            "macroproduct_count",
            f.array(
                f.col("macroproduct"),
                f.col("count"),
                f.col("fea_product_max_validity_period_days_01w"),
            ),
        )
    )
    macro_agg_expr = [
        f.sum(
            f.when(f.col("macroproduct") == i, f.col("count"),).otherwise(f.lit(0))
        ).alias("fea_product_count_{}_01w".format(i))
        for i in macro_product_list
    ]

    agg_expr = [
        f.min(f.col("fea_product_min_validity_period_days_01w")).alias(
            "fea_product_min_validity_period_days_01w"
        ),
        f.max(f.col("fea_product_max_validity_period_days_01w")).alias(
            "fea_product_max_validity_period_days_01w"
        ),
        f.sum(f.col("fea_product_count_validity_1_to_4_days_01w")).alias(
            "fea_product_count_validity_1_to_4_days_01w"
        ),
        f.sum(f.col("fea_product_count_validity_5_to_10_days_01w")).alias(
            "fea_product_count_validity_5_to_10_days_01w"
        ),
        f.sum(f.col("fea_product_count_validity_11_to_22_days_01w")).alias(
            "fea_product_count_validity_11_to_22_days_01w"
        ),
        f.sum(f.col("fea_product_count_validity_23_to_45_days_01w")).alias(
            "fea_product_count_validity_23_to_45_days_01w"
        ),
        f.sum(f.col("fea_product_count_validity_46plus_days_01w")).alias(
            "fea_product_count_validity_46plus_days_01w"
        ),
        f.collect_list("macroproduct_count").alias("macroproduct_count_list"),
    ] + macro_agg_expr
    product_agg = macroproduct_agg.groupBy("msisdn", "weekstart").agg(*agg_expr)

    return product_agg


@deprecated(version="0.1", reason="Some of product tables not productionized")
def create_macro_product_weekly(
    base_ifrs_c2p_df: pyspark.sql.DataFrame,
    base_ifrs_c2p_reject_df: pyspark.sql.DataFrame,
    bid_macro_product_mapping_base_df: pyspark.sql.DataFrame,
    bid_macro_product_mapping_new_df: pyspark.sql.DataFrame,
    macro_product_list: list,
) -> None:
    """
    Aggregate base_ifrs_c2p to msisdn and weekstart

    Args:
        base_ifrs_c2p_df: Dataframe of base_ifrs_c2p table
        base_ifrs_c2p_reject_df: Dataframe of base_ifrs_c2p_reject table
        bid_macro_product_mapping_base_df: Base macroproduct mapping
        bid_macro_product_mapping_new_df: New macroproduct mapping
        macro_product_list: List of macroproduct

    Returns:
        Dataframe of Macroproduct Weekly Aggregated
    """
    select_cols = [
        "msisdn",
        "event_date",
        "validity_period",
        "validity_period_unit",
        "transaction_id",
        f.col("sigma_business_id").alias("bid"),
        f.concat("validity_period", f.lit(" "), "validity_period_unit").alias(
            "validity_period_str"
        ),
    ]
    base_ifrs_c2p_df = base_ifrs_c2p_df.select(select_cols)
    base_ifrs_c2p_reject_df = base_ifrs_c2p_reject_df.select(select_cols)

    bid_macro_product_mapping_df = union_macro_product_list(
        bid_macro_product_mapping_base_df, bid_macro_product_mapping_new_df
    )

    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date()
    end_date = get_end_date()

    weekly_agg_catalog = conf_catalog["l2_macroproduct_weekly"]

    load_args_1 = conf_catalog["l1_base_ifrs_c2p"]["load_args"]
    load_args_2 = conf_catalog["l1_base_ifrs_c2p_reject"]["load_args"]
    save_args = weekly_agg_catalog["save_args"]
    save_args.pop("partitionBy", None)
    file_path = get_file_path(filepath=weekly_agg_catalog["filepath"])
    file_format = weekly_agg_catalog["file_format"]
    partitions = int(weekly_agg_catalog["partitions"])
    log.info(
        "Starting Weekly Aggregation for WeekStart {start_date} to {end_date}".format(
            start_date=(start_date + timedelta(days=7)).strftime("%Y-%m-%d"),
            end_date=(end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
        )
    )
    log.info(f"Load Args 1: {load_args_1}")
    log.info(f"Load Args 2: {load_args_2}")
    log.info(f"File Path: {file_path}")
    log.info(f"File Format: {file_format}")
    log.info(f"Save Args: {save_args}")
    log.info(f"Partitions: {partitions}")

    # Caching bid_macro_product_mapping_df, So that Spark won't end up reading this DataFrame from HDFS in each iteration
    bid_macro_product_mapping_df.cache()

    while start_date < end_date:
        week_start = (start_date + timedelta(days=7)).strftime("%Y-%m-%d")
        sdate_1 = start_date.strftime(load_args_1["partition_date_format"])
        edate_1 = (start_date + timedelta(days=6)).strftime(
            load_args_1["partition_date_format"]
        )
        sdate_2 = start_date.strftime(load_args_2["partition_date_format"])
        edate_2 = (start_date + timedelta(days=6)).strftime(
            load_args_2["partition_date_format"]
        )

        log.info("Starting Weekly Aggregation for WeekStart: {}".format(week_start))

        df_data_1 = base_ifrs_c2p_df.filter(
            f.col(load_args_1["partition_column"]).between(sdate_1, edate_1)
        )
        df_data_2 = base_ifrs_c2p_reject_df.filter(
            f.col(load_args_2["partition_column"]).between(sdate_2, edate_2)
        )

        df = _weekly_aggregation(
            base_ifrs_c2p_df=df_data_1,
            base_ifrs_c2p_reject_df=df_data_2,
            bid_macro_product_mapping_df=bid_macro_product_mapping_df,
            macro_product_list=macro_product_list,
        ).drop(f.col("weekstart"))

        partition_file_path = "{file_path}/weekstart={weekstart}".format(
            file_path=file_path, weekstart=week_start
        )

        df.repartition(numPartitions=partitions).write.save(
            partition_file_path, file_format, **save_args
        )
        log.info(
            "Completed Weekly Aggregation for WeekStart: {}".format(
                (start_date + timedelta(days=7)).strftime("%Y-%m-%d")
            )
        )

        start_date += timedelta(days=7)
    bid_macro_product_mapping_df.unpersist()
