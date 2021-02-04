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

from utils import (
    encode,
    filter_incorrect_lat_long,
    get_custom_window,
    get_neighbours,
    rename_columns,
)


def _pick_outlets_with_highest_count_for_lat_log(
    df_outlet: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Outlets have multiple lat, long. Picks up the Outlet and most frequently
    occuring lat long (outlet_id, latitude, longitude, kabupaten), also removes
    rows where outlet_id, latitude, longitude may be empty strings or nulls.

    :param df_outlet: Outlet raw data.
    :return: outlet_id attached with most frequent lat/long and other outlet
    characteristic variables (branch, sub-branch etc..).
    """
    geo_udf = f.udf(lambda x, y, z: encode(float(x), float(y), z))
    df_outlet = (
        df_outlet.filter(~f.col("outlet_id").startswith("6"))
        .filter(
            (f.lower(f.col("outlet_id")) != "jabar")
            & (f.col("outlet_id").isNotNull())
            & (f.col("outlet_id") != "")
            & (f.col("lattitude") != "")
            & (f.col("lattitude").isNotNull())
            & (f.col("longitude") != "")
            & (f.col("longitude").isNotNull())
        )
        .withColumnRenamed("lattitude", "latitude")
    )

    df_outlet = filter_incorrect_lat_long(df_outlet)

    df_outlets_with_multiple_latlong_and_type = (
        df_outlet.withColumn(
            "latlong", f.concat(f.col("latitude"), f.lit("|"), f.col("longitude"))
        )
        .groupBy("outlet_id")
        .agg(
            f.countDistinct("latlong").alias("latlong_count"),
            f.countDistinct("tipe_outlet").alias("type_count"),
        )
        .filter((f.col("latlong_count") > 1) | (f.col("type_count") > 1))
    )

    df_outlet = df_outlet.join(
        df_outlets_with_multiple_latlong_and_type, ["outlet_id"], how="left_anti"
    )

    df_outlets_stats = (
        df_outlet.groupBy("outlet_id")
        .agg(
            f.first("latitude").alias("latitude"),
            f.first("longitude").alias("longitude"),
            f.first("kabupaten").alias("kabupaten"),
            f.first("tipe_outlet").alias("outlet_type"),
            f.first("cluster").alias("cluster"),
            f.first("branch").alias("fea_outlets_branch"),
            f.first("sub_branch").alias("fea_outlets_sub_branch"),
            f.first("kabupaten").alias("fea_outlets_kabupaten"),
            f.first("kecamatan").alias("fea_outlets_kecamatan"),
            f.first("kelurahan").alias("fea_outlets_kelurahan"),
        )
        .withColumn(
            "fea_outlets_geohash",
            geo_udf(f.col("latitude"), f.col("longitude"), f.lit(5)),
        )
    )

    cols_of_interest = [
        "outlet_id",
        "latitude",
        "longitude",
        "kabupaten",
        "outlet_type",
        "cluster",
        "fea_outlets_geohash",
        "fea_outlets_branch",
        "fea_outlets_sub_branch",
        "fea_outlets_kecamatan",
        "fea_outlets_kelurahan",
    ]
    return df_outlets_stats.select(cols_of_interest)


def clean_outlets(df_outlet: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Cleaning outlets dataframe.

    :param df_outlet: Outlet raw data.
    :return: Outlets dataframe cleaned.
    """
    df_outlet_cleaned = _pick_outlets_with_highest_count_for_lat_log(df_outlet)
    return df_outlet_cleaned


def get_most_recent_outlet_classification(
    df_outlet: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Gets most recent outlet classification.

    :param df_outlet: Outlet raw data.
    :return: outlet_id, trx_date with the most recent classification.
    """
    window = get_custom_window(
        partition_by_cols=["outlet_id"], order_by_col="trx_date", asc=False
    )
    df_outlet_classification = (
        df_outlet.withColumnRenamed("klasifikasi", "classification")
        .select("outlet_id", "trx_date", "classification")
        .withColumn("row_number", f.row_number().over(window))
        .filter(f.col("row_number") == 1)
    )
    return df_outlet_classification.repartition(10)


def clean_pois(
    df_big_poi: pyspark.sql.DataFrame,
    df_coast: pyspark.sql.DataFrame,
    df_prm_roads: pyspark.sql.DataFrame,
    df_prm_sec_roads: pyspark.sql.DataFrame,
    df_railways: pyspark.sql.DataFrame,
    df_osm_poi: pyspark.sql.DataFrame,
) -> List[pyspark.sql.DataFrame]:
    """
    Cleans POI (Point of interest) Dataframes, performs the following:
    1. Removes duplicate rows based on the columns of interests defined.
    2. Filters out incorrect lat/long or outliers beyond acceptable bounds.

    :param df_big_poi: POI data collected from BIG agency points.
    :param df_coast: Coast lat/long data points.
    :param df_prm_roads: Primary road boundary lat/long points.
    :param df_prm_sec_roads: Primary and seconday road boundary lat/long points.
    :param df_railways: Railway station lat/long points.
    :param df_osm_poi: Open street map POI lat/long points.
    :return: Cleans all the POIs and returns a list of cleaned Dataframes.
    """
    cleaned_dfs = []
    df_prm_roads = df_prm_roads.withColumnRenamed("road_type", "road_type_prm")
    df_prm_sec_roads = df_prm_sec_roads.withColumnRenamed("road_type", "road_type_sec")

    df_big_poi = df_big_poi.filter(f.col("shp_type") == "PT")

    # Both may have same subcategories which may result in duplicate column while creating master table
    df_big_poi = df_big_poi.withColumn(
        "subcategory", f.concat(f.col("subcategory"), f.lit("_big_poi"))
    )
    df_osm_poi = df_osm_poi.withColumn(
        "category", f.concat(f.col("category"), f.lit("_osm"))
    )

    all_dfs = [
        (df_big_poi, ["subcategory", "latitude", "longitude"]),
        (df_coast, ["country", "latitude", "longitude"]),
        (df_prm_roads, ["road_type_prm", "latitude", "longitude"]),
        (df_prm_sec_roads, ["road_type_sec", "latitude", "longitude"]),
        (df_railways, ["railways", "latitude", "longitude"]),
        (df_osm_poi, ["category", "latitude", "longitude"]),
    ]

    for df, columns_of_interest in all_dfs:
        df_distinct = df.select(columns_of_interest).distinct()
        df_distinct_filtered_lat_long = filter_incorrect_lat_long(df_distinct)
        cleaned_dfs.append(df_distinct_filtered_lat_long)

    return cleaned_dfs


def get_all_neighbours_for_outlet(
    df_outlet: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Gets all neighbouring geohashes for an outlet geohash.

    :param df_outlet: Outlet raw data.
    :return: 9 rows per outlet as there are 9 neigbouring geohashes for each
    outlet including the geohash in which the outlet is.
    """
    df_outlet_with_neighbors = get_neighbours(df_outlet)
    df_outlet_neighbours_exploded = df_outlet_with_neighbors.withColumn(
        "neighbours_all", f.explode("neighbours_all")
    )
    df_outlet_neighbours_exploded = df_outlet_neighbours_exploded.withColumn(
        "latitude", f.col("latitude").cast("float")
    ).withColumn("longitude", f.col("longitude").cast("float"))

    old_new_column_names_dict = {
        "latitude": "lat1",
        "longitude": "long1",
    }
    df_outlet_neighbours_exploded = rename_columns(
        df_outlet_neighbours_exploded, old_new_column_names_dict
    )
    return df_outlet_neighbours_exploded


def _get_all_neighbours_for_pois_helper(
    df_poi: pyspark.sql.DataFrame, old_new_column_names_dict_poi: dict
) -> pyspark.sql.DataFrame:
    """
    Gets all neighbouring geohashes for a POI (point of interest) geohash.

    :param df_poi: POI dataframe.
    :param old_new_column_names_dict_poi: Old and new column name dictionary.
    :return: 9 rows per POI as there are 9 neigbouring geohashes for each
    POI including the geohash in which the outlet is.
    """
    df_poi_with_neighbours = get_neighbours(df_poi)
    df_poi_neighbours_exploded = df_poi_with_neighbours.withColumn(
        "neighbours_all", f.explode("neighbours_all")
    )
    df_big_poi_neighbours_exploded = rename_columns(
        df_poi_neighbours_exploded, old_new_column_names_dict_poi
    )
    return df_big_poi_neighbours_exploded


def get_all_neighbours_for_poi(
    df_big_poi: pyspark.sql.DataFrame,
    df_osm_pois: pyspark.sql.DataFrame,
    df_conflict_pois: pyspark.sql.DataFrame,
) -> List[pyspark.sql.DataFrame]:
    """
    Gets all neighbouring geohashes for each POI geohash.

    :param df_big_poi: POI data collected from BIG agency points.
    :param df_osm_pois: Open street map POI lat/long points.
    :param df_conflict_pois: Approximate lat/long points of conflicts (riots etc..)
    :return: List of POI Dataframes with their neighbours.
    """
    old_new_column_names_dict_poi = {"latitude": "lat2", "longitude": "long2"}
    df_big_poi_neighbours_exploded = _get_all_neighbours_for_pois_helper(
        df_big_poi, old_new_column_names_dict_poi
    )

    old_new_column_names_dict_osm_poi = {
        "category": "subcategory",
        "Latitude": "lat2",
        "Longitude": "long2",
    }
    df_osm_pois_neighbours_exploded = _get_all_neighbours_for_pois_helper(
        df_osm_pois, old_new_column_names_dict_osm_poi
    )

    # Cleaning up Conflict POIS
    conflict_columns_of_interest = ["event_type", "latitude", "longitude"]
    df_conflict_pois_cleaned = df_conflict_pois.select(
        conflict_columns_of_interest
    ).withColumn(
        "event_type", f.regexp_replace(f.lower(f.col("event_type")), "[\\s\\/]", "_")
    )
    group_by_cols = ["event_type", "latitude", "longitude"]
    df_conflict_event_counts = (
        df_conflict_pois_cleaned.groupBy(group_by_cols)
        .count()
        .withColumnRenamed("count", "count_of_events")
    )
    df_conflict_pois_neighbours_exploded = _get_all_neighbours_for_pois_helper(
        df_conflict_event_counts, old_new_column_names_dict_poi
    )

    return [
        df_big_poi_neighbours_exploded,
        df_osm_pois_neighbours_exploded,
        df_conflict_pois_neighbours_exploded,
    ]


def get_all_poi_for_an_outlet(
    df_outlet_neighbours_exploded: pyspark.sql.DataFrame,
    df_big_poi_neighbours_exploded: pyspark.sql.DataFrame,
    df_osm_poi_neighbours_exploded: pyspark.sql.DataFrame,
    df_conflict_pois_neighbours_exploded: pyspark.sql.DataFrame,
) -> List[pyspark.sql.DataFrame]:
    """
    Gets all POI points (BIG, OSM, Conflicts) for an outlet which are neighbouring geohashes
    or at most the next nearest neighbour geohashes.

    :param df_outlet_neighbours_exploded: All outlets with neighbours
    :param df_big_poi_neighbours_exploded: POI data collected from BIG agency points with neighbours.
    :param df_osm_poi_neighbours_exploded: Open street map POI lat/long points with neighbours.
    :param df_conflict_pois_neighbours_exploded: Approximate lat/long points of conflicts (riots etc..) with neighbours.
    :return: Returns all neighbouring/next neighbouring POIs from an outlet.
    """
    dfj_pois_for_outlets = df_outlet_neighbours_exploded.join(
        df_big_poi_neighbours_exploded, ["neighbours_all"], how="inner"
    )
    dfj_osm_pois_for_outlets = df_outlet_neighbours_exploded.join(
        df_osm_poi_neighbours_exploded, ["neighbours_all"], how="inner"
    )
    dfj_conflict_pois_for_outlets = df_outlet_neighbours_exploded.join(
        df_conflict_pois_neighbours_exploded, ["neighbours_all"], how="inner"
    )

    big_pois_columns_of_interest = [
        "outlet_id",
        "subcategory",
        "lat1",
        "long1",
        "lat2",
        "long2",
    ]
    conflicts_columns_of_interest = [
        "outlet_id",
        "lat1",
        "long1",
        "event_type",
        "lat2",
        "long2",
        "count_of_events",
    ]
    outlet_columns_of_interest = [
        "outlet_id",
        "outlet_id_poi",
        "lat1",
        "long1",
        "lat2",
        "long2",
        "kabupaten",
        "outlet_poi_type",
    ]

    dfj_unique_poi_per_outlet = dfj_pois_for_outlets.select(
        big_pois_columns_of_interest
    ).distinct()
    dfj_unique_osm_poi_per_outlet = dfj_osm_pois_for_outlets.select(
        big_pois_columns_of_interest
    ).distinct()
    dfj_unique_conflicts_poi_per_outlet = dfj_conflict_pois_for_outlets.select(
        conflicts_columns_of_interest
    ).distinct()

    df_outlet_copy = (
        df_outlet_neighbours_exploded.withColumnRenamed("lat1", "lat2")
        .withColumnRenamed("long1", "long2")
        .withColumnRenamed("outlet_id", "outlet_id_poi")
        .withColumnRenamed("outlet_type", "outlet_poi_type")
        .select("outlet_id_poi", "lat2", "long2", "neighbours_all", "outlet_poi_type")
        .repartition(50)
    )

    df_outlet_neighbours_exploded = df_outlet_neighbours_exploded.repartition(25)

    # Outlets near an outlet
    dfj_outlet_for_outlet = df_outlet_neighbours_exploded.join(
        df_outlet_copy, ["neighbours_all"], how="inner",
    )
    dfj_uniq_outlet_for_outlet = dfj_outlet_for_outlet.select(
        outlet_columns_of_interest
    ).distinct()

    return [
        dfj_unique_poi_per_outlet,
        dfj_unique_osm_poi_per_outlet,
        dfj_unique_conflicts_poi_per_outlet,
        dfj_uniq_outlet_for_outlet,
    ]


def get_all_neighbours_for_fb_points(
    df_fb_pop: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Gets neighbours for all facebook data points.

    :param df_fb_pop: Facebook demography data.
    :return: Gets neighbouring geohashes from a given facebook data point.
    """
    df_fb_pop_with_neighbours = get_neighbours(df_fb_pop)
    df_fb_pop_with_neighbours = df_fb_pop_with_neighbours.withColumn(
        "neighbours_all", f.explode("neighbours_all")
    )
    old_new_column_names_dict = {
        "latitude": "lat2",
        "longitude": "long2",
    }
    df_fb_pop_with_neighbours = rename_columns(
        df_fb_pop_with_neighbours, old_new_column_names_dict
    )
    return df_fb_pop_with_neighbours


def get_all_fb_pop_for_an_outlet(
    df_outlet_exploded: pyspark.sql.DataFrame, df_fb_pop_exploded: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    """
    Gets all facebook data points which are in the neighbouring/next nearest neighbouring geohashes.

    :param df_outlet_exploded: Outlets with neighbouring geohases.
    :param df_fb_pop_exploded: Facebook populatulation data points with neighbours.
    :return: Gets all facebook points which are neighbours/next nearest neighbour to an outlet.
    """
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "10000")
    df_fbj = df_outlet_exploded.join(
        df_fb_pop_exploded, ["neighbours_all"], how="inner"
    )
    return df_fbj
