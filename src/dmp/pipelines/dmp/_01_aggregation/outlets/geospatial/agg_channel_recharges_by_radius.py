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
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from utils import filter_incorrect_lat_long, get_neighbours


def _clean_datasets(
    df_urp: pyspark.sql.DataFrame,
    df_mkios: pyspark.sql.DataFrame,
    df_dnkm: pyspark.sql.DataFrame,
) -> List[pyspark.sql.DataFrame]:

    df_urp_cleaned = df_urp.filter(
        (f.col("bnum_lacci_id").isNotNull()) & (f.col("trx_date").isNotNull())
    )
    df_mkios_cleaned = df_mkios.filter(
        (f.col("rs_lacci_id").isNotNull()) & (f.col("trx_date").isNotNull())
    )

    df_dnkm = df_dnkm.filter(
        (f.col("lac_ci").isNotNull())
        & (f.col("lat").isNotNull())
        & (f.col("lon").isNotNull())
        & (f.col("lat") != "")
        & (f.col("lon") != "")
    ).filter((f.col("lat") != "0") | (f.col("lon") != "0"))

    df_dnkm_renamed = df_dnkm.withColumnRenamed("lat", "latitude").withColumnRenamed(
        "lon", "longitude"
    )
    df_dnkm_lat_long_sub = filter_incorrect_lat_long(df_dnkm_renamed)

    group_by_cols = ["lac_ci", "latitude", "longitude"]
    df_dnkm_lat_long_count = df_dnkm_lat_long_sub.groupBy(group_by_cols).agg(
        f.count("*").alias("count"), f.max("trx_date").alias("max_trx_date")
    )

    df_dnkm_lat_long_ranks = df_dnkm_lat_long_count.withColumn(
        "row_number",
        f.row_number().over(
            Window.partitionBy("lac_ci").orderBy(
                f.desc("count"), f.desc("max_trx_date")
            )
        ),
    )

    columns_of_interest = ["lac_ci", "latitude", "longitude"]
    df_dnkm_lat_long_mode = (
        df_dnkm_lat_long_ranks.withColumn(
            "lac_ci", f.regexp_replace("lac_ci", "-", "|")
        )
        .filter(f.col("row_number") == 1)
        .select(columns_of_interest)
    )

    return [df_urp_cleaned, df_mkios_cleaned, df_dnkm_lat_long_mode]


def agg_urp_channel_recharges_by_radius(
    df_urp: pyspark.sql.DataFrame,
    df_mkios: pyspark.sql.DataFrame,
    df_dnkm: pyspark.sql.DataFrame,
    df_outlet_with_neighb: pyspark.sql.DataFrame,
) -> List[pyspark.sql.DataFrame]:

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "10000")

    cleaned_datasets = _clean_datasets(df_urp, df_mkios, df_dnkm)
    df_urp_lacci_cleaned = cleaned_datasets[0]
    df_mkios_lacci_cleaned = cleaned_datasets[1]
    df_dnkm_lat_long_mode = cleaned_datasets[2]

    df_urp_lacci_cleaned.cache()
    df_mkios_lacci_cleaned.cache()
    df_dnkm_lat_long_mode.cache()
    df_outlet_with_neighb.cache()

    df_urp_agg = (
        df_urp_lacci_cleaned.withColumn(
            "channel_group",
            f.when(f.col("channel_group").isNull(), f.lit("null")).otherwise(
                f.col("channel_group")
            ),
        )
        .withColumn(
            "channel_group", f.regexp_replace(f.lower(f.col("channel_group")), " ", "_")
        )
        .withColumn(
            "channel_group",
            f.regexp_replace("channel_group", "retail_nasional", "modern_channel"),
        )
        .groupBy("bnum_lacci_id", "channel_group")
        .agg(
            f.coalesce(f.sum("rech"), f.lit(0)).alias("urp_rech"),
            f.coalesce(f.sum("trx_rech"), f.lit(0)).alias("urp_trx_rech"),
        )
    )

    df_urp_lacci_cleaned.unpersist()
    df_urp_agg.cache()

    df_mkios_agg = (
        # Replacing all channel with a single channel for mkios
        # mkios has only 2 channels, so grouping all into one
        df_mkios_lacci_cleaned.withColumn("channel_group", f.lit("mkios_channel"))
        .groupBy("rs_lacci_id", "channel_group")
        .agg(
            f.coalesce(f.sum("rech"), f.lit(0)).alias("mkios_rech"),
            f.coalesce(f.sum("trx_rech"), f.lit(0)).alias("mkios_trx_rech"),
        )
    )

    df_mkios_lacci_cleaned.unpersist()
    df_mkios_agg.cache()

    df_urp_loc = get_neighbours(
        df_urp_agg.join(
            df_dnkm_lat_long_mode, f.col("lac_ci") == f.col("bnum_lacci_id")
        )
    )
    df_urp_loc = (
        df_urp_loc.withColumn("neighbours_all", f.explode("neighbours_all"))
        .withColumnRenamed("latitude", "lat2")
        .withColumnRenamed("longitude", "long2")
    )

    df_mkios_loc = get_neighbours(
        df_mkios_agg.join(
            df_dnkm_lat_long_mode, f.col("lac_ci") == f.col("rs_lacci_id")
        )
    )
    df_mkios_loc = (
        df_mkios_loc.withColumn("neighbours_all", f.explode("neighbours_all"))
        .withColumnRenamed("latitude", "lat2")
        .withColumnRenamed("longitude", "long2")
    )

    df_urp_loc.cache()
    df_mkios_loc.cache()

    df_mkios_agg.unpersist()
    df_urp_agg.unpersist()
    df_dnkm_lat_long_mode.unpersist()

    df_mkios_neighb = df_mkios_loc.join(df_outlet_with_neighb, ["neighbours_all"])
    df_urp_neighb = df_urp_loc.join(df_outlet_with_neighb, ["neighbours_all"])

    df_outlet_with_neighb.unpersist()

    mkios_cols_of_interest = [
        "outlet_id",
        "rs_lacci_id",
        "channel_group",
        "mkios_rech",
        "mkios_trx_rech",
        "lat1",
        "long1",
        "lat2",
        "long2",
    ]
    urp_cols_of_interest = [
        "outlet_id",
        "bnum_lacci_id",
        "channel_group",
        "urp_rech",
        "urp_trx_rech",
        "lat1",
        "long1",
        "lat2",
        "long2",
    ]

    df_mkios_neighb_uniq = df_mkios_neighb.select(mkios_cols_of_interest).distinct()
    df_urp_neighb_uniq = df_urp_neighb.select(urp_cols_of_interest).distinct()

    return [df_mkios_neighb_uniq, df_urp_neighb_uniq]
