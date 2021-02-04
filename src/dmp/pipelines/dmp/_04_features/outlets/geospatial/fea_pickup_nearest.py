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

import pyspark.sql.functions as f
from pyspark.sql.window import Window

from utils import (
    add_prefix_to_fea_columns,
    calc_distance_bw_2_lat_long,
    convert_cols_to_lc_and_rem_space,
    filter_incorrect_lat_long,
    full_join_all_dfs,
    get_neighbours,
    get_required_output_columns,
)


def get_nearest_poi_for_outlets(df_poi, fea_names, df_outlets):
    """
    Gets min distance of the POI from each outlet. For eg: if df_poi is
    a sports area, it gives the min distance to the sports arena for each outlet
    :param fea_names: feature names to pick up
    :param df_poi: Point of interest data
    :param df_outlets: Outlet data
    :return df_nearest_poi_from_outlet: Returns min distance from the outlet to
    the POI for each outlet
    """
    df_poi = filter_incorrect_lat_long(df_poi)
    df_poi = get_neighbours(df_poi)
    df_poi = df_poi.withColumn("neighbours_all", f.explode("neighbours_all"))
    df_poi = df_poi.withColumnRenamed("latitude", "lat2").withColumnRenamed(
        "longitude", "long2"
    )

    df_outlets_join_with_poi = df_outlets.join(df_poi, ["neighbours_all"], how="inner")
    df_outlets_dist_from_poi = calc_distance_bw_2_lat_long(df_outlets_join_with_poi)

    fea_names = fea_names + ["distance"]
    df_outlets_dist_from_poi = df_outlets_dist_from_poi.select(fea_names).distinct()

    window_get_closest_poi = Window.partitionBy("outlet_id").orderBy(f.col("distance"))
    df_nearest_poi_from_outlet = df_outlets_dist_from_poi.withColumn(
        "row_number", f.row_number().over(window_get_closest_poi)
    ).filter(f.col("row_number") == 1)

    return df_nearest_poi_from_outlet


def calc_nearest_poi_and_assign_helper(all_dfs, df_outlets):
    fea_dfs = []
    for df, fea_names in all_dfs:
        df_nearest_poi_for_outlets = get_nearest_poi_for_outlets(
            df, fea_names, df_outlets
        )
        df_nearest_poi_for_outlets = df_nearest_poi_for_outlets.select(fea_names)
        fea_dfs.append(df_nearest_poi_for_outlets)

    df_fea_merged_all = full_join_all_dfs(fea_dfs, "outlet_id")
    df_fea_merged_all = add_prefix_to_fea_columns(
        df_fea_merged_all, ["outlet_id"], "fea_outlets_"
    )
    return df_fea_merged_all


def calc_nearest_poi_dist_from_outlet_helper(all_dfs, df_outlets):
    fea_dfs = []
    for df, fea_name in all_dfs:
        df_nearest_poi_for_outlets = get_nearest_poi_for_outlets(
            df, ["outlet_id"], df_outlets
        )
        df_nearest_poi_for_outlets = df_nearest_poi_for_outlets.withColumnRenamed(
            "distance", fea_name
        )
        df_nearest_poi_for_outlets = df_nearest_poi_for_outlets.select(
            "outlet_id", fea_name
        )
        fea_dfs.append(df_nearest_poi_for_outlets)

    df_fea_merged_all = full_join_all_dfs(fea_dfs, "outlet_id")
    df_fea_merged_all = add_prefix_to_fea_columns(
        df_fea_merged_all, ["outlet_id"], "fea_outlets_"
    )
    return df_fea_merged_all


def calc_nearest_poi_and_assign(
    df_outlets,
    df_wp_dem,
    df_urbanicity,
    df_gar,
    df_elevation,
    df_rainfall,
    df_admin_div,
    df_poverty,
    df_cell_tower,
    df_hdi,
    df_gdp_per_capita,
    df_gdp,
    feature_mode: str,
    required_output_features: List[str],
):

    df_admin_div = convert_cols_to_lc_and_rem_space(df_admin_div)
    df_admin_div = df_admin_div.select(
        *["longitude", "latitude", "admin_4", "admin_3", "admin_2", "admin_1"]
    ).distinct()
    df_poverty = convert_cols_to_lc_and_rem_space(df_poverty)

    df_poverty_per_admin = df_admin_div.join(
        df_poverty,
        f.lower(df_poverty.province) == f.lower(df_admin_div.admin_1),
        how="full",
    )

    for col in df_cell_tower.columns:
        col_cleaned = col.lower().replace(" ", "_").replace("(", "").replace(")", "")
        df_cell_tower = df_cell_tower.withColumnRenamed(col, col_cleaned)

    all_dfs_pick_nearest_and_assign = [
        (df_wp_dem, ["outlet_id", "dependency_ratio", "births", "pregnancies"]),
        (df_urbanicity, ["outlet_id", "urbanicity"]),
        (
            df_gar,
            [
                "outlet_id",
                "emp_agr",
                "emp_gov",
                "emp_ind",
                "emp_ser",
                "ic_high",
                "ic_mhg",
                "ic_mlw",
                "ic_low",
                "tot_val",
            ],
        ),
        (df_elevation, ["outlet_id", "elevation_m"]),
        (df_rainfall, ["outlet_id", "rainfall_index"]),
        (
            df_poverty_per_admin,
            [
                "outlet_id",
                "admin_1",
                "admin_2",
                "admin_3",
                "admin_4",
                "extreme_poverty_rate_urban_2018",
                "extreme_poverty_rate_rural_2018",
                "extreme_poverty_rate_total_2018",
            ],
        ),
        (
            df_cell_tower,
            [
                "outlet_id",
                "closest_lte_km",
                "closest_cdma_km",
                "closest_umts_km",
                "closest_gsm_km",
            ],
        ),
        (df_hdi, ["outlet_id", "hdi_2015"]),
        (df_gdp_per_capita, ["outlet_id", "gdp_per_capita_ppp_2015_usd"]),
        (df_gdp, ["outlet_id", "gdp_ppp_2015_usd"]),
    ]
    df_pick_nearest_and_assign_features = calc_nearest_poi_and_assign_helper(
        all_dfs_pick_nearest_and_assign, df_outlets
    )

    required_output_columns = get_required_output_columns(
        output_features=df_pick_nearest_and_assign_features.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id"],
    )

    df_pick_nearest_and_assign_features = df_pick_nearest_and_assign_features.select(
        required_output_columns
    )

    return df_pick_nearest_and_assign_features


def calc_nearest_poi_dist_from_outlet(
    df_outlets,
    df_coast,
    df_prm_roads,
    df_prm_sec_roads,
    df_railways,
    feature_mode: str,
    required_output_features: List[str],
):
    all_dfs_get_nearest_distance = [
        (df_coast, "min_dist_to_coast"),
        (df_prm_roads, "min_dist_to_prm_road"),
        (df_prm_sec_roads, "min_dist_to_prm_or_sec_road"),
        (df_railways, "min_dist_to_railway"),
    ]
    df_get_nearest_dist_features = calc_nearest_poi_dist_from_outlet_helper(
        all_dfs_get_nearest_distance, df_outlets
    )

    required_output_columns = get_required_output_columns(
        output_features=df_get_nearest_dist_features.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id"],
    )
    df_get_nearest_dist_features = df_get_nearest_dist_features.select(
        required_output_columns
    )

    return df_get_nearest_dist_features
