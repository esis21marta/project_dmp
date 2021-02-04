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


from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.dmp._04_features.outlets.geospatial.fea_fb_pop_for_outlets import (
    calc_fb_pop_for_outlets,
)
from src.dmp.pipelines.dmp._04_features.outlets.geospatial.fea_map_to_outlets import (
    map_features_to_outlets,
)
from src.dmp.pipelines.dmp._04_features.outlets.geospatial.fea_pickup_nearest import (
    calc_nearest_poi_and_assign,
    calc_nearest_poi_dist_from_outlet,
)
from src.dmp.pipelines.dmp._04_features.outlets.geospatial.fea_pois_for_outlets import (
    calc_poi_channel_lacci_based_features,
    calc_pois_outlet_features,
)
from src.dmp.pipelines.dmp._04_features.outlets.revenue_daily_level.fea_outlet_network import (
    calc_fea_outlet_network,
)
from src.dmp.pipelines.dmp._04_features.outlets.revenue_daily_level.fea_outlet_revenue_daily import (
    calc_fea_outlet_revenue_daily_scoring,
    calc_fea_outlet_revenue_daily_training,
    fill_time_series_outlet_revenue_master,
)


def create_pipeline(**kwargs) -> Pipeline:
    """ Create the Internet Usage Primary Pipeline.
    :param kwargs: Ignore any additional arguments added in the future.
    :return: A Pipeline object containing all the Internet Usage Primary Nodes.
    """

    return Pipeline(
        [
            # ----------------- Outlet GeoSpatial Features --------------------#
            node(
                func=calc_pois_outlet_features,
                inputs=[
                    "l1_dfj_big_poi_outlet",
                    "l1_dfj_osm_poi_outlet",
                    "l1_dfj_conflict_poi_outlet",
                    "l1_dfj_outlets_near_outlet",
                    "l1_outlets_with_most_recent_classf",
                    "l1_mkios_recharge_daily_aggregated",
                    "params:reseller_features_mode",
                    "params:output_features",
                ],
                outputs=[
                    "l2_closest_big_pois_to_outlet",
                    "l2_closest_osm_pois_to_outlet",
                    "l2_total_big_pois_count_for_outlet",
                    "l2_total_conflict_count_outlet",
                    "l2_outlet_to_outlet_min_dist",
                    "l2_outlet_counts_per_outlet_in_radius",
                    "l2_types_of_outlets_near_an_outlet",
                    "l2_outlets_in_radiuses_based_on_classf",
                    "l2_outlets_cashflow_in_radiuses_based_on_classf",
                ],
                name="calc_pois_outlet_features",
                tags=["fea_outlet_geo_pipeline", "scoring_fea_outlet_geo_pipeline"],
            ),
            node(
                func=calc_fb_pop_for_outlets,
                inputs=[
                    "l1_dfj_outlet_fb_pop",
                    "params:reseller_features_mode",
                    "params:output_features",
                ],
                outputs="l2_fb_pop_for_outlet",
                name="calc_fb_pop_for_outlets",
                tags=["fea_outlet_geo_pipeline", "scoring_fea_outlet_geo_pipeline"],
            ),
            node(
                func=map_features_to_outlets,
                inputs=[
                    "l1_unique_outlets",
                    # "l1_fb_share",
                    # "l1_4g_bts_share",
                    "l1_archetype",
                    "l1_dealer",
                    # "l1_ext_ookla",
                    # "l1_ext_opensignal",
                    "l1_digistar_sellthru_det",
                    "l1_salesforce",
                    "params:reseller_features_mode",
                    "params:output_features",
                ],
                outputs=[
                    # "l2_fb_share_for_outlet",
                    # "l2_4g_bts_share_for_outlet",
                    "l2_outlet_archetype",
                    "l2_outlet_dealer",
                    # "l2_outlet_ookla",
                    # "l2_outlet_opensignal",
                    "l2_outlet_digistar_sellthru_det",
                    "l2_outlet_salesforce",
                ],
                name="map_features_to_outlets",
                tags=["fea_outlet_geo_pipeline", "scoring_fea_outlet_geo_pipeline"],
            ),
            node(
                func=calc_nearest_poi_and_assign,
                inputs=[
                    "l1_outlets_with_neighbours",
                    "l1_ext_WP_demographics",
                    "l1_ext_GHS_urbanicity",
                    "l1_ext_GAR_demographics",
                    "l1_ext_WP_elevation",
                    "l1_ext_NEO_rainfall_index",
                    "l1_ext_HDX_administrative_divisions",
                    "l1_ext_BPS_extreme_poverty_percentages",
                    "l1_ext_ocid_cell_towers",
                    "l1_ext_Dryad_HDI",
                    "l1_ext_Dryad_GDP_per_capita",
                    "l1_ext_Dryad_GDP",
                    "params:reseller_features_mode",
                    "params:output_features",
                ],
                outputs="l2_outlet_geospatial_pick_nearest_and_assign",
                name="calc_nearest_poi_and_assign",
                tags=["fea_outlet_geo_pipeline", "scoring_fea_outlet_geo_pipeline"],
            ),
            node(
                func=calc_nearest_poi_dist_from_outlet,
                inputs=[
                    "l1_outlets_with_neighbours",
                    "l1_unique_ext_HDX_coast_points",
                    "l1_unique_ext_OSM_primary_roads",
                    "l1_unique_ext_OSM_primary_and_secondary_roads",
                    "l1_unique_ext_OSM_railways",
                    "params:reseller_features_mode",
                    "params:output_features",
                ],
                outputs="l2_outlet_geospatial_nearest_poi_dist_from_outlet",
                name="calc_nearest_poi_dist_from_outlet",
                tags=["fea_outlet_geo_pipeline", "scoring_fea_outlet_geo_pipeline"],
            ),
            node(
                func=calc_poi_channel_lacci_based_features,
                inputs=[
                    "l1_mkios_neighb_uniq",
                    "l1_urp_neighb_uniq",
                    "params:reseller_features_mode",
                    "params:output_features",
                ],
                outputs=[
                    "l2_min_dist_to_lacci_from_outlet",
                    "l2_mkios_channel_rech",
                    "l2_urp_channel_rech",
                ],
                name="calc_poi_channel_lacci_based_features",
                tags=["fea_outlet_geo_pipeline", "scoring_fea_outlet_geo_pipeline"],
            ),
            # ----------------- Outlet Revenue Features (Daily Level) -------------------- #
            node(
                func=calc_fea_outlet_network,
                inputs=[
                    "l1_dnkm_agg_by_lacci",
                    "l1_dfj_digipos_mkios",
                    "params:reseller_features_mode",
                    "params:output_features",
                ],
                outputs=["l2_fea_hsupa", "l2_fea_3g_and_payload"],
                name="calc_fea_outlet_network",
                tags=["fea_outlet_rev_pipeline", "scoring_fea_outlet_rev_pipeline"],
            ),
            node(
                func=calc_fea_outlet_revenue_daily_training,
                inputs=[
                    "l1_outlet_digipos_daily_aggregated",
                    "l1_mkios_recharge_daily_aggregated",
                    "l1_mkios_recharge_daily_mode_aggregated",
                    "l1_modern_daily_aggregated",
                    "l1_pv_inj_lacci_outlet_stats",
                    "l1_pv_inj_city_stats",
                    "l1_pv_inj_rs_msisdn_outlet_stats",
                    "l1_pv_red_lacci_outlet_stats",
                    "l1_pv_red_city_stats",
                    "l1_pv_red_rs_msisdn_outlet_stats",
                    "l1_zone_pricing",
                    "l1_total_modern_channel_recharges",
                    "l2_fea_hsupa",
                    "l2_fea_3g_and_payload",
                    "params:reseller_features_mode",
                    "params:output_features",
                ],
                outputs=None,
                name="outlet_revenue_master_training",
                tags=["fea_outlet_rev_pipeline"],
            ),
            node(
                func=calc_fea_outlet_revenue_daily_scoring,
                inputs=[
                    "l1_outlet_digipos_daily_aggregated",
                    "l1_mkios_recharge_daily_aggregated",
                    "l1_mkios_recharge_daily_mode_aggregated",
                    "l1_modern_daily_aggregated",
                    "l1_pv_red_lacci_outlet_stats",
                    "l1_pv_red_city_stats",
                    "l1_pv_red_rs_msisdn_outlet_stats",
                    "l2_fea_hsupa",
                    "l2_fea_3g_and_payload",
                    "params:reseller_features_mode",
                    "params:output_features",
                ],
                outputs=None,
                name="outlet_revenue_master_scoring",
                tags=["fea_outlet_rev_pipeline", "scoring_fea_outlet_rev_pipeline"],
            ),
            node(
                func=fill_time_series_outlet_revenue_master,
                inputs="l3_outlet_master_revenue_daily",
                outputs="l3_outlet_master_revenue_daily_tsf",
                name="fill_time_series_outlet_revenue_master",
                tags=[
                    "de_outlet_rev_master_pipeline",
                    "scoring_fea_outlet_rev_pipeline",
                ],
            ),
        ],
        tags=["fea_outlets", "outlets_pipeline"],
    )
