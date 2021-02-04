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

from utils import calc_distance_bw_2_lat_long, get_required_output_columns


def generate_radius_features_with_fea_columns_given(
    df, radiuses_in_m, target_columns, _agg_func, fea_prefix
):
    """
    Generates radius based features for all target_columns based on _agg_func
    :param df: Base dataframe
    :param radiuses_in_m: All radiuses for which features are to be computed
    :param target_columns: Feature categories
    :param _agg_func: Aggregation function
    :param fea_prefix: Prefix for feature columns
    :return df_features: Radius based features
    """
    all_sum_agg_funcs = []

    # Create flags for 200m, 500m, 1000m, 2500m, 5000m by comparing with distance column
    for radius in radiuses_in_m:
        df = df.withColumn(
            f"under_{radius}m", (f.col("distance") <= radius).cast("integer")
        )

    # Sum the target column for cases when radius is under 200m, 500m, 1000m, 2500m, 5000m
    for radius in radiuses_in_m:
        for column in target_columns:
            agg_func = _agg_func(column, radius, fea_prefix)
            all_sum_agg_funcs.append(agg_func)

    # Compute all feature columns
    df_features = df.groupBy("outlet_id").agg(*all_sum_agg_funcs)
    return df_features


def calc_fb_pop_for_outlets(
    df_fbj: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: List[str],
):
    """
    Calculates Fb pop per category for outlets
    :param df_fbj: Outlets data joined with FB population data
    :return: df_fb_stat
    """
    radiuses_in_m = [200, 500, 1000, 2500, 5000]
    fea_prefix = "fea_outlets_under"
    target_columns = [
        "population_2015",
        "population_2020",
        "men",
        "women",
        "women_reproductive_age_15_49",
        "children_under_5",
        "youth_15_24",
        "elderly_60_plus",
        "distance",
    ]

    _max_agg_func = lambda _column: f.max(_column).alias(_column)
    _sum_agg_func = lambda _column, _radius, _fea_prefix: f.sum(
        f.when((f.col(f"under_{_radius}m") == 1), f.col(_column))
    ).alias(f"{_fea_prefix}_{_radius}m_{_column}")

    # Calculate distance between outlet and FB data point
    df_fbj = calc_distance_bw_2_lat_long(df_fbj)

    # Get max for all target columns
    all_max_agg_funcs = []
    for column in target_columns:
        agg_func = _max_agg_func(column)
        all_max_agg_funcs.append(agg_func)

    # Pick up only one FB point and exclude others as there may be duplicates due to join on 'neighbours_all'
    group_by_columns = ["outlet_id", "lat1", "long1", "lat2", "long2"]
    df_fb_rmv_dup = df_fbj.groupBy(group_by_columns).agg(*all_max_agg_funcs)

    # Remove 'distance' from feature column as it is not a feature
    target_columns.remove("distance")
    df_fb_stat = generate_radius_features_with_fea_columns_given(
        df_fb_rmv_dup, radiuses_in_m, target_columns, _sum_agg_func, fea_prefix
    )
    required_output_columns = get_required_output_columns(
        output_features=df_fb_stat.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id"],
    )
    df_fb_stat = df_fb_stat.select(required_output_columns)

    return df_fb_stat
