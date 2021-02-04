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

from utils import calc_distance_bw_2_lat_long, get_required_output_columns


def calc_closest_pois_for_outlets(
    df_with_dist: pyspark.sql.DataFrame, category_column: str
):
    """
    Gets closest distance of each POI for all outlets
    :param df_with_dist: Outlets data joined with POI data
    :param category_column: category column for all the pois
    :return: df_subcat_dist_pivot
    """
    df_closest_dist_to_poi = (
        df_with_dist.withColumn(
            category_column,
            f.concat(
                f.lit("fea_outlets_"),
                f.lower(f.regexp_replace(f.col(category_column), " ", "_")),
                f.lit("_min_dist"),
            ),
        )
        .groupBy("outlet_id")
        .pivot(category_column)
        .agg(f.min("distance"))
    )
    return df_closest_dist_to_poi


def calc_radius_based_features(
    df_with_dist: pyspark.sql.DataFrame,
    category_column: str,
    fea_prefix: str,
    sum_columns: list = None,
):
    """Calculates radius based features and sums by count or a given column. Eg: Number of
    subcategories defined by category_column (mosque, cafe etc..) within a radius of 200m, 500m..
    :param sum_columns: Columns to be summed up in aggregation
    :param fea_prefix: Feature column prefix
    :param category_column: Category column to be pivoted
    :param df_with_dist: Dataframe with distance column between Outlet and POI.
    :return: df_subcat_count_pivot
    """
    radiuses_in_m = [200, 500, 1000, 2500, 5000]
    df_dist_flag = df_with_dist
    for radius in radiuses_in_m:
        df_dist_flag = df_dist_flag.withColumn(
            f"under_{radius}m", (f.col("distance") <= radius).cast("integer")
        )

    _sum_by_col = lambda _radius, _col_name: f.coalesce(
        f.sum(f.when(f.col(f"under_{_radius}m") == 1, f.col(_col_name))), f.lit(0)
    ).alias(f"{_col_name}_under_{_radius}m")

    _sum_by_radius_flag = lambda _radius: f.coalesce(
        f.sum(f"under_{_radius}m"), f.lit(0)
    ).alias(f"under_{_radius}m")

    agg_func_list = []
    if sum_columns:
        for radius in radiuses_in_m:
            for col_name in sum_columns:
                agg_func_list.append(_sum_by_col(radius, col_name))
    else:
        for radius in radiuses_in_m:
            agg_func_list.append(_sum_by_radius_flag(radius))

    df_subcat_count_pivot = (
        df_dist_flag.withColumn(
            category_column,
            f.concat(
                f.lit(fea_prefix),
                f.lower(f.regexp_replace(f.col(category_column), " ", "_")),
            ),
        )
        .groupBy("outlet_id")
        .pivot(category_column)
        .agg(*agg_func_list)
        .fillna(0.0)
    )
    return df_subcat_count_pivot


def calc_outlet_to_outlet_features(
    dfj_outlets_near_outlet_with_distance: pyspark.sql.DataFrame,
):
    """Calculates number of outlets from an outlet in a radius and min
    distance to an outlet from a given outlet
    :param dfj_outlets_near_outlet_with_distance: Outlets joined with outlet table
    :return: outlet_with_min_dist_and_radius_counts
    """
    radiuses_in_m = [200, 500, 1000, 2500, 5000]
    agg_func_list = []
    _count_distinct_outlets = lambda _radius, _col: f.coalesce(
        f.countDistinct(f.when((f.col("distance") <= _radius), f.col(_col))), f.lit(0)
    ).alias(f"fea_outlets_under_{_radius}m")

    for _radius in radiuses_in_m:
        dfj_outlets_near_outlet_with_distance = dfj_outlets_near_outlet_with_distance.withColumn(
            f"under_{_radius}m", (f.col("distance") <= _radius).cast("integer")
        )
        agg_func_list.append(_count_distinct_outlets(_radius, "outlet_id_poi"))

    # Number of outlets in radiuses
    df_outlets_near_outlets_with_dist_flag = dfj_outlets_near_outlet_with_distance.groupBy(
        "outlet_id"
    ).agg(
        *agg_func_list
    )
    df_nearest_outlet_min_dist = dfj_outlets_near_outlet_with_distance.groupBy(
        "outlet_id"
    ).agg(f.min("distance").alias("fea_outlet_to_outlet_min_dist"))
    outlet_with_min_dist_and_radius_counts = {
        "min_dist": df_outlets_near_outlets_with_dist_flag,
        "radius_based_features": df_nearest_outlet_min_dist,
    }
    return outlet_with_min_dist_and_radius_counts


def calc_pois_outlet_features(
    dfj_big_poi_outlet: pyspark.sql.DataFrame,
    dfj_osm_poi_outlets: pyspark.sql.DataFrame,
    dfj_conflicts_poi_outlets: pyspark.sql.DataFrame,
    dfj_outlets_near_outlet: pyspark.sql.DataFrame,
    df_outlet_classification: pyspark.sql.DataFrame,
    df_mkios_daily_agg: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: List[str],
):
    """
    Calculates POI features for outlets
    :param dfj_osm_poi_outlets: OSM POI data joined with outlet
    :param dfj_big_poi_outlet: Other POI data joined with outlet
    :param dfj_conflicts_poi_outlets: Conflict POI data joined with outlet
    :param dfj_outlets_near_outlet: Outlets near an outlet
    :param df_outlet_classification: Outlet with most recent classification
    :param df_mkios_daily_agg: outlet_id, trx_date aggregated recharges data
    :return: df_closest_big_pois_to_outlet, df_total_big_pois_count_for_outlet
    """
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "1000")

    # Calculates distance between Outlet and the POI #
    dfj_big_poi_with_distance = calc_distance_bw_2_lat_long(dfj_big_poi_outlet)
    dfj_osm_poi_with_distance = calc_distance_bw_2_lat_long(dfj_osm_poi_outlets)
    dfj_conflicts_poi_outlets_with_distance = calc_distance_bw_2_lat_long(
        dfj_conflicts_poi_outlets
    )
    dfj_outlets_near_outlet_with_distance = calc_distance_bw_2_lat_long(
        dfj_outlets_near_outlet
    )

    # Min distance features from POI to Outlet #
    df_closest_big_pois_to_outlet = calc_closest_pois_for_outlets(
        dfj_big_poi_with_distance, category_column="subcategory"
    )
    df_closest_osm_pois_to_outlet = calc_closest_pois_for_outlets(
        dfj_osm_poi_with_distance, category_column="subcategory"
    )

    # Counts of POIs in given radius features from an Outlet #
    df_total_big_pois_count_for_outlet = calc_radius_based_features(
        dfj_big_poi_with_distance,
        category_column="subcategory",
        fea_prefix="fea_outlets_big_pois_",
    )

    df_total_conflicts_count_for_outlet = calc_radius_based_features(
        dfj_conflicts_poi_outlets_with_distance,
        category_column="event_type",
        fea_prefix="fea_outlets_conflicts_",
        sum_columns=["count_of_events"],
    )

    df_types_of_outlets_near_an_outlet = calc_radius_based_features(
        dfj_outlets_near_outlet_with_distance,
        category_column="outlet_poi_type",
        fea_prefix="fea_outlets_count_outlet_type",
    )

    # --------- Outlet Counts Near An Outlet In Radiuses Based On Classification ------- #
    df_outlet_classification = df_outlet_classification.withColumnRenamed(
        "outlet_id", "outet_id_classf"
    )
    df_outlet_classification_with_distance = df_outlet_classification.join(
        dfj_outlets_near_outlet_with_distance,
        (
            df_outlet_classification["outet_id_classf"]
            == dfj_outlets_near_outlet_with_distance["outlet_id_poi"]
        ),
        how="inner",
    )

    df_outlets_in_radiuses_based_on_classf = calc_radius_based_features(
        df_outlet_classification_with_distance,
        category_column="classification",
        fea_prefix="fea_outlets_count_outlet_classf_",
    )

    # ------------------- Outlet Cashflow Near An Outlet In Radiuses ------------------- #
    df_cashflow_amt_and_trx_per_outlet = (
        df_mkios_daily_agg.groupBy("outlet_id")
        .agg(
            f.coalesce(f.sum("total_cashflow_mkios"), f.lit(0)).alias(
                "total_cashflow_mkios"
            ),
            f.coalesce(f.sum("total_trx_cashflow_mkios"), f.lit(0)).alias(
                "total_trx_cashflow_mkios"
            ),
        )
        .withColumnRenamed("outlet_id", "outlet_id_poi")
    )

    df_outlet_cashflow_based_on_classf = dfj_outlets_near_outlet_with_distance.join(
        df_cashflow_amt_and_trx_per_outlet, ["outlet_id_poi"], how="inner"
    )

    df_outlets_cashflow_in_radiuses_based_on_classf = calc_radius_based_features(
        df_outlet_cashflow_based_on_classf,
        category_column="outlet_poi_type",
        fea_prefix="fea_outlets_",
        sum_columns=["total_cashflow_mkios", "total_trx_cashflow_mkios"],
    )

    # Outlet to outlet based features - outlet as pois
    # Excluding the same outlet-to-outlet join
    dfj_outlets_near_outlet_with_distance = dfj_outlets_near_outlet_with_distance.filter(
        f.col("distance") > 0
    )
    outlet_to_outlet_features = calc_outlet_to_outlet_features(
        dfj_outlets_near_outlet_with_distance
    )
    df_outlet_to_outlet_min_dist = outlet_to_outlet_features["min_dist"]
    df_outlet_counts_per_outlet_in_radius = outlet_to_outlet_features[
        "radius_based_features"
    ]

    required_output_columns_closest_big_pois_to_outlet = get_required_output_columns(
        output_features=df_closest_big_pois_to_outlet.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id"],
    )

    required_output_columns_closest_osm_pois_to_outlet = get_required_output_columns(
        output_features=df_closest_osm_pois_to_outlet.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id"],
    )

    required_output_columns_total_big_pois_count_for_outlet = get_required_output_columns(
        output_features=df_total_big_pois_count_for_outlet.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id"],
    )

    required_output_columns_total_conflicts_count_for_outlet = get_required_output_columns(
        output_features=df_total_conflicts_count_for_outlet.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id"],
    )

    required_output_columns_outlet_to_outlet_min_dist = get_required_output_columns(
        output_features=df_outlet_to_outlet_min_dist.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id"],
    )

    required_output_columns_outlet_counts_per_outlet_in_radius = get_required_output_columns(
        output_features=df_outlet_counts_per_outlet_in_radius.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id"],
    )

    required_output_columns_types_of_outlets_near_an_outlet = get_required_output_columns(
        output_features=df_types_of_outlets_near_an_outlet.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id"],
    )

    required_output_columns_outlets_in_radiuses_based_on_classf = get_required_output_columns(
        output_features=df_outlets_in_radiuses_based_on_classf.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id"],
    )

    required_output_columns_outlets_cashflow_in_radiuses_based_on_classf = get_required_output_columns(
        output_features=df_outlets_cashflow_in_radiuses_based_on_classf.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id"],
    )

    df_closest_big_pois_to_outlet = df_closest_big_pois_to_outlet.select(
        required_output_columns_closest_big_pois_to_outlet
    )
    df_closest_osm_pois_to_outlet = df_closest_osm_pois_to_outlet.select(
        required_output_columns_closest_osm_pois_to_outlet
    )
    df_total_big_pois_count_for_outlet = df_total_big_pois_count_for_outlet.select(
        required_output_columns_total_big_pois_count_for_outlet
    )
    df_total_conflicts_count_for_outlet = df_total_conflicts_count_for_outlet.select(
        required_output_columns_total_conflicts_count_for_outlet
    )
    df_outlet_to_outlet_min_dist = df_outlet_to_outlet_min_dist.select(
        required_output_columns_outlet_to_outlet_min_dist
    )
    df_outlet_counts_per_outlet_in_radius = df_outlet_counts_per_outlet_in_radius.select(
        required_output_columns_outlet_counts_per_outlet_in_radius
    )
    df_types_of_outlets_near_an_outlet = df_types_of_outlets_near_an_outlet.select(
        required_output_columns_types_of_outlets_near_an_outlet
    )
    df_outlets_in_radiuses_based_on_classf = df_outlets_in_radiuses_based_on_classf.select(
        required_output_columns_outlets_in_radiuses_based_on_classf
    )
    df_outlets_cashflow_in_radiuses_based_on_classf = df_outlets_cashflow_in_radiuses_based_on_classf.select(
        required_output_columns_outlets_cashflow_in_radiuses_based_on_classf
    )

    return [
        # Min distance features
        df_closest_big_pois_to_outlet,
        df_closest_osm_pois_to_outlet,
        # POI counts in radius features
        df_total_big_pois_count_for_outlet,
        df_total_conflicts_count_for_outlet,
        # Outlet to outlet min dist, radius based features
        df_outlet_to_outlet_min_dist,
        df_outlet_counts_per_outlet_in_radius,
        df_types_of_outlets_near_an_outlet,
        df_outlets_in_radiuses_based_on_classf,
        df_outlets_cashflow_in_radiuses_based_on_classf,
    ]


def calc_poi_channel_lacci_based_features(
    df_mkios_neighb_uniq: pyspark.sql.DataFrame,
    df_urp_neighb_uniq: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: List[str],
):
    df_mkios_neighb_uniq_dist = calc_distance_bw_2_lat_long(df_mkios_neighb_uniq)
    df_urp_neighb_uniq_dist = calc_distance_bw_2_lat_long(df_urp_neighb_uniq)

    df_mkios_closes_dist_to_lacci = df_mkios_neighb_uniq_dist.groupBy("outlet_id").agg(
        f.min("distance").alias("fea_outlets_min_dist_to_lacci")
    )

    df_urp_neighb_uniq_dist_to_lacci = df_urp_neighb_uniq_dist.groupBy("outlet_id").agg(
        f.min("distance").alias("fea_outlets_min_dist_to_lacci")
    )

    df_min_dist_to_lacci_from_outlet = (
        df_mkios_closes_dist_to_lacci.union(df_urp_neighb_uniq_dist_to_lacci)
        .groupBy("outlet_id")
        .agg(
            f.min("fea_outlets_min_dist_to_lacci").alias(
                "fea_outlets_min_dist_to_lacci"
            )
        )
    )

    df_mkios_channel_rech = calc_radius_based_features(
        df_mkios_neighb_uniq_dist,
        category_column="channel_group",
        fea_prefix="fea_outlets_",
        sum_columns=["mkios_rech", "mkios_trx_rech"],
    )

    df_urp_channel_rech = calc_radius_based_features(
        df_urp_neighb_uniq_dist,
        category_column="channel_group",
        fea_prefix="fea_outlets_",
        sum_columns=["urp_rech", "urp_trx_rech"],
    )

    required_output_columns_min_dist_to_lacci_from_outlet = get_required_output_columns(
        output_features=df_min_dist_to_lacci_from_outlet.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id"],
    )
    df_min_dist_to_lacci_from_outlet = df_min_dist_to_lacci_from_outlet.select(
        required_output_columns_min_dist_to_lacci_from_outlet
    )

    required_output_columns_mkios_channel_rech = get_required_output_columns(
        output_features=df_mkios_channel_rech.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id"],
    )
    df_mkios_channel_rech = df_mkios_channel_rech.select(
        required_output_columns_mkios_channel_rech
    )

    required_output_columns_urp_channel_rech = get_required_output_columns(
        output_features=df_urp_channel_rech.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id"],
    )
    df_urp_channel_rech = df_urp_channel_rech.select(
        required_output_columns_urp_channel_rech
    )

    return [
        df_min_dist_to_lacci_from_outlet,
        df_mkios_channel_rech,
        df_urp_channel_rech,
    ]
