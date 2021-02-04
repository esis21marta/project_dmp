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
from datetime import date, timedelta

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
def create_macro_product_category(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Map the bid to Macro Product

    Args:
        df: Dataframe of Product information

    Returns:
        Dataframe with Macro Product information
    """
    df = (
        df.withColumn(
            "voice_bin",
            f.when(f.col("voice") <= 0, "voice0")
            .when(f.col("voice").between(0.001, 1200), "voice0to1200")
            .when(f.col("voice").between(1200.001, 400000), "voice1200to400000")
            .otherwise("voice400000to999999999"),
        )
        .withColumn(
            "data_bin",
            f.when(f.col("data") <= 0, "data0")
            .when(f.col("data").between(0.001, 1), "data0to1")
            .when(f.col("data").between(1.001, 3), "data1to3")
            .when(f.col("data").between(3.001, 7), "data3to7")
            .when(f.col("data").between(7.001, 15), "data7to15")
            .when(f.col("data").between(15.001, 32), "data15to32")
            .when(f.col("data").between(32.001, 1024), "data32to64"),
        )
        .withColumn(
            "data_dpi_bin", f.when(f.col("data_dpi") > 0, "dpi1").otherwise("dpi0")
        )
        .withColumn("sms_bin", f.when(f.col("sms") > 0, "sms1").otherwise("sms0"))
        .withColumn("validity", f.concat(f.lit("val"), f.col("validity")))
    )

    df = (
        df.filter(f.col("data_bin").isNotNull())
        .withColumn(
            "macroproduct",
            f.concat(
                f.col("data_bin"),
                f.lit("_"),
                f.col("data_dpi_bin"),
                f.lit("_"),
                f.col("voice_bin"),
                f.lit("_"),
                f.col("sms_bin"),
                f.lit("_"),
                f.col("validity"),
            ),
        )
        .withColumn(
            "weekstart",
            f.when(
                f.col("weekstart").isNull(),
                next_week_start_day(f.lit(str(date.today()))),
            ).otherwise(f.col("weekstart")),
        )
    )

    return df.select("bid", "macroproduct", "rev_share", "weekstart")


@deprecated(version="0.1", reason="Some of product tables not productionized")
def _weekly_aggregation(
    smy_usage_ocs_chg_dd: pyspark.sql.DataFrame,
    mck_sku_bucket_pivot: pyspark.sql.DataFrame,
    smy_product_catalogue_sku_c2c_dd: pyspark.sql.DataFrame,
    macro_product_map_base: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Create the mapping between BID and Macroproduct

    Args:
        smy_usage_ocs_chg_dd: Dataframe of smy.usage_ocs_chg_dd table
        mck_sku_bucket_pivot: Dataframe of mck.sku_bucket_pivot table
        smy_product_catalogue_sku_c2c_dd: Dataframe of smy.product_catalogue_sku_c2c_dd table
        bid_macroproduct_mapping_base_df: Base macroproduct mapping
        macro_product_map_base: Macro Product Mapping

    Returns:
        Dataframe with Macroproduct information
    """
    smy_usage_ocs_chg_dd = (
        (
            smy_usage_ocs_chg_dd.filter(f.col("pack_id").isNotNull())
            .groupBy(f.col("content_id").alias("bid"), f.col("pack_id").alias("sku"))
            .agg(
                f.sum(f.col("trx_c")).alias("trx"),
                f.sum(f.col("rev")).alias("rev"),
                f.first(f.col("event_date")).alias("event_date"),
            )
        )
        .filter(f.col("trx") > 10)
        .withColumnRenamed("bid", "bid_y")
        .withColumn(
            "weekstart",
            next_week_start_day(
                f.to_date(f.col("event_date").cast(t.StringType()), "yyyy-MM-dd")
            ),
        )
    )
    smy_product_catalogue_sku_c2c_dd = (
        smy_product_catalogue_sku_c2c_dd.groupBy(
            "business_id",
            "sku",
            "rules_name",
            "rules_id",
            "product_id",
            "product_type",
            "product_commercial_name",
            "validity",
            "rate",
        )
        .count()
        .withColumnRenamed("count", "ct")
        .withColumn(
            "rn",
            f.row_number().over(
                Window().partitionBy(f.col("sku")).orderBy(f.col("ct").desc())
            ),
        )
        .withColumnRenamed("business_id", "bid")
    ).filter(f.col("rn") == 1)
    sku_product = mck_sku_bucket_pivot.join(
        smy_product_catalogue_sku_c2c_dd, ["bid", "sku"], how="left"
    )
    sku_product_revenue = sku_product.join(smy_usage_ocs_chg_dd, ["sku"], how="left")
    revenue_by_bid = sku_product_revenue.groupBy("bid_y").agg(
        f.sum("rev").alias("sum_rev"),
    )
    prods_bid_reve = sku_product_revenue.join(
        revenue_by_bid, ["bid_y"], how="left"
    ).withColumn(
        "rev_share",
        f.when(f.col("sum_rev") == 0, 0).otherwise(f.col("rev") / f.col("sum_rev")),
    )

    prods_bid_reve = (
        prods_bid_reve.filter((f.col("bid") != f.col("sku")) & (f.col("rate") > 0))
        .orderBy("bid", "rev_share", ascending=False)
        .coalesce(1)
        .dropDuplicates(subset=["bid"])
    )

    voice = [col for col in prods_bid_reve.columns if "voice" in col]
    data = [col for col in prods_bid_reve.columns if "data" in col]
    sms = [col for col in prods_bid_reve.columns if "sms" in col]
    data_dpi = ["data_video", "data_games", "data_dpi", "data_4g_omg", "data_music"]

    for i in prods_bid_reve.columns:
        if i != "weekstart":
            prods_bid_reve = prods_bid_reve.withColumn(
                i, f.when(f.col(i) == "", 0).otherwise(f.col(i))
            )

    prods_bid_reve = (
        prods_bid_reve.withColumn("voice", sum(f.col(i) for i in voice))
        .withColumn("data", sum(f.col(i) for i in data))
        .withColumn("sms", sum(f.col(i) for i in sms))
        .withColumn("data_dpi", sum(f.col(i) for i in data_dpi))
    ).replace(to_replace=VALIDITY_MAPPING, subset=["validity"])

    prods_macro = prods_bid_reve.select(
        [
            "bid",
            "sms",
            "voice",
            "data",
            "data_dpi",
            "validity",
            "rev_share",
            "weekstart",
        ]
    )

    macroproduct_new = create_macro_product_category(prods_macro)
    macroproduct_base = macro_product_map_base.select("macroproduct").distinct()
    valid_macroproduct = macroproduct_new.join(
        f.broadcast(macroproduct_base), ["macroproduct"], how="inner"
    )

    return valid_macroproduct


@deprecated(version="0.1", reason="Some of product tables not productionized")
def create_macro_product_mapping(
    smy_usage_ocs_chg_dd: pyspark.sql.DataFrame,
    mck_sku_bucket_pivot: pyspark.sql.DataFrame,
    smy_product_catalogue_sku_c2c_dd: pyspark.sql.DataFrame,
    macro_product_map_base: pyspark.sql.DataFrame,
) -> None:
    """
    Create the mapping between BID and Macroproduct

    Args:
        smy_usage_ocs_chg_dd: Dataframe of smy.usage_ocs_chg_dd table
        mck_sku_bucket_pivot: Dataframe of mck.sku_bucket_pivot table
        smy_product_catalogue_sku_c2c_dd: Dataframe of smy.product_catalogue_sku_c2c_dd table
        bid_macroproduct_mapping_base_df: Base macroproduct mapping
        macro_product_map_base: Macro Product Mapping

    """
    mck_sku_bucket_pivot.cache()
    macro_product_map_base.cache()

    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date()
    end_date = get_end_date()

    weekly_agg_catalog = conf_catalog["l1_macroproduct_map_new"]

    load_args = conf_catalog["l1_smy_usage_ocs_chg_dd"]["load_args"]
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
    log.info(f"File Path: {file_path}")
    log.info(f"File Format: {file_format}")
    log.info(f"Load Args: {load_args}")
    log.info(f"Save Args: {save_args}")
    log.info(f"Partitions: {partitions}")

    # Caching So that Spark won't end up reading these DataFrame from HDFS in each iteration
    mck_sku_bucket_pivot.cache()
    macro_product_map_base.cache()

    while start_date < end_date:
        week_start = (start_date + timedelta(days=7)).strftime("%Y-%m-%d")
        sdate = start_date.strftime(load_args["partition_date_format"])
        edate = (start_date + timedelta(days=6)).strftime(
            load_args["partition_date_format"]
        )

        log.info("Starting Weekly Aggregation for WeekStart: {}".format(week_start))

        df_data = smy_usage_ocs_chg_dd.filter(
            f.col(load_args["partition_column"]).between(sdate, edate)
        )

        df_filter_smy_product = smy_product_catalogue_sku_c2c_dd.filter(
            f.col(load_args["partition_column"]).between(sdate, edate)
        )

        df = _weekly_aggregation(
            smy_usage_ocs_chg_dd=df_data,
            mck_sku_bucket_pivot=mck_sku_bucket_pivot,
            smy_product_catalogue_sku_c2c_dd=df_filter_smy_product,
            macro_product_map_base=macro_product_map_base,
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
    mck_sku_bucket_pivot.unpersist()
    macro_product_map_base.unpersist()
