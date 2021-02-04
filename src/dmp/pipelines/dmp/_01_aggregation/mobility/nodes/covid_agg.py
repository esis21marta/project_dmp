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

import pyspark
from pyspark.sql import functions as f
from pyspark.sql import types as t

from utils import encode


def mobility_covid_agg(
    covid_data: pyspark.sql.DataFrame, village_coord_data: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    # defining the udf for geohash encode and neighbour geohash
    geo_encode_udf = f.udf(lambda x, y: encode(x, y, precision=5), t.StringType())

    # home stay point processing

    df_covid = (
        covid_data.withColumn("mo_id", f.date_format(f.col("file_date"), "yyyy-MM"))
        .groupBy("ID_KEL", "province", "city", "kecamatan", "kelurahan", "mo_id")
        .agg(
            f.avg(f.col("suspect")).alias("suspect_avg"),
            f.avg(f.col("positive")).alias("positive_avg"),
            f.avg(f.col("probable")).alias("probable_avg"),
            f.avg(f.col("positive_dead")).alias("dead_avg"),
            f.max(f.col("suspect")).alias("suspect_max"),
            f.max(f.col("positive")).alias("positive_max"),
            f.max(f.col("probable")).alias("probable_max"),
            f.max(f.col("positive_dead")).alias("dead_max"),
        )
    )

    df_join = df_covid.join(
        village_coord_data.selectExpr(
            "Desa", "Latitude", "Longitude", "Kecamatan as vill_kecamatan"
        ),
        (
            (f.lower(f.col("kelurahan")) == f.lower(f.col("Desa")))
            & (f.lower(f.col("kecamatan")) == f.lower(f.col("vill_kecamatan")))
        ),
        how="left",
    ).drop("vill_kecamatan")

    df_final = (df_join.where(f.col("Desa").isNotNull()).drop("Desa")).unionByName(
        df_join.where(f.col("Desa").isNull())
        .select(*df_covid.columns)
        .withColumn(
            "kecamatan",
            f.when(
                f.col("kecamatan") == "KEP. SERIBU UTARA", "KEPULAUAN SERIBU UTARA"
            ).otherwise(f.col("kecamatan")),
        )
        .withColumn(
            "kecamatan",
            f.when(
                f.col("kecamatan") == "KEP. SERIBU SELATAN", "KEPULAUAN SERIBU SELATAN"
            ).otherwise(f.col("kecamatan")),
        )
        .join(
            (
                village_coord_data.selectExpr(
                    "Latitude", "Longitude", "Kecamatan as vill_kecamatan"
                ).distinct()
            ),
            f.lower(f.col("kecamatan")) == f.lower(f.col("vill_kecamatan")),
        )
        .drop("vill_kecamatan")
    )

    df_covid_geohash = (
        df_final.withColumn(
            "geohash", (geo_encode_udf(df_final.Latitude, df_final.Longitude))
        )
        .withColumn(
            "covid_cluster_flag",
            f.when(
                (
                    (f.col("suspect_avg") >= 10)
                    | (f.col("positive_avg") >= 10)
                    | (f.col("probable_avg") >= 8)
                    | (f.col("dead_avg") >= 1)
                    | (f.col("suspect_max") >= 10)
                    | (f.col("positive_max") >= 10)
                    | (f.col("probable_max") >= 8)
                    | (f.col("dead_max") >= 1)
                ),
                1,
            ).otherwise(0),
        )
        .groupBy("mo_id", "geohash",)
        .agg(
            f.first("Latitude").alias("lat2"),
            f.first("Longitude").alias("long2"),
            f.avg("suspect_avg").alias("suspect_avg"),
            f.avg("positive_avg").alias("positive_avg"),
            f.avg("probable_avg").alias("probable_avg"),
            f.avg("dead_avg").alias("dead_avg"),
            f.max("suspect_max").alias("suspect_max"),
            f.max("positive_max").alias("positive_max"),
            f.max("probable_max").alias("probable_max"),
            f.max("dead_max").alias("dead_max"),
            f.max("covid_cluster_flag").alias("covid_cluster_flag"),
        )
    )

    return df_covid_geohash
