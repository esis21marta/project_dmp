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

from utils import encode, neighbors


def mobility_poi_agg(
    ext_poi_big: pyspark.sql.DataFrame,
    ext_osm_pois: pyspark.sql.DataFrame,
    category1_dict,
    category2_dict,
) -> pyspark.sql.DataFrame:

    # defining the udf for geohash encode and neighbour geohash
    geo_encode_udf = f.udf(lambda x, y: encode(x, y, precision=5), t.StringType())

    neighbor_udf = f.udf(lambda x: neighbors(x), t.ArrayType(t.StringType()))

    # poi dataframe processing

    df_poi_big = ext_poi_big.selectExpr(
        "lower(subcategory) as category", "longitude", "latitude"
    ).distinct()
    df_poi_osm = ext_osm_pois.selectExpr("category", "longitude", "latitude").distinct()

    # union poi data frame
    df_poi = df_poi_big.unionByName(df_poi_osm)

    # Add rows for mapping the pois with category_1 dict
    cat1_func = f.udf(lambda x: category1_dict.get(x), t.StringType())
    df_poi = df_poi.withColumn(
        "category_type_1", f.trim(f.lower(cat1_func(f.col("category"))))
    ).where(f.col("category_type_1").isNotNull())
    # Add rows for mapping the pois with category_2 dict
    cat2_func = f.udf(lambda x: category2_dict.get(x), t.StringType())
    df_poi = df_poi.withColumn(
        "category_type_2", f.trim(f.lower(cat2_func(f.col("category"))))
    ).where(f.col("category_type_2").isNotNull())

    df_pois = (
        df_poi.withColumn(
            "poi_geohash", (geo_encode_udf(df_poi.latitude, df_poi.longitude))
        )
        .withColumn("poi_geohash_neighbor", (neighbor_udf(f.col("poi_geohash"))))
        .withColumn("poi_neighbor_geohash", (f.explode(f.col("poi_geohash_neighbor"))))
        .drop("poi_geohash_neighbor")
    )

    return df_pois
