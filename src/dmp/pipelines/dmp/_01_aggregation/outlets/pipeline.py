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

from src.dmp.pipelines.dmp._01_aggregation.outlets.clean_tables import (
    clean_mkios,
    clean_urp,
)
from src.dmp.pipelines.dmp._01_aggregation.outlets.geospatial.agg_channel_recharges_by_radius import (
    agg_urp_channel_recharges_by_radius,
)
from src.dmp.pipelines.dmp._01_aggregation.outlets.geospatial.agg_outlet_geospatial import (
    clean_outlets,
    clean_pois,
    get_all_fb_pop_for_an_outlet,
    get_all_neighbours_for_fb_points,
    get_all_neighbours_for_outlet,
    get_all_neighbours_for_poi,
    get_all_poi_for_an_outlet,
    get_most_recent_outlet_classification,
)
from src.dmp.pipelines.dmp._01_aggregation.outlets.revenue_daily_level.agg_outlet_digipos import (
    agg_outlet_digipos_daily,
)
from src.dmp.pipelines.dmp._01_aggregation.outlets.revenue_daily_level.agg_outlet_network import (
    agg_outlet_network_dnkm,
    partition_dnkm,
)
from src.dmp.pipelines.dmp._01_aggregation.outlets.revenue_daily_level.agg_recharge_mkios import (
    agg_recharge_mkios_for_features,
    calc_modern_channel_recharges_for_all_lacci,
    calc_physical_voucher_inj,
    calc_physical_voucher_red,
    calc_recharges_by_modern_channel,
    subselect_mkios_by_digipos_rs_msisdns,
)


def create_pipeline(**kwargs) -> Pipeline:
    """ Create the Internet Usage Primary Pipeline.
    :param kwargs: Ignore any additional arguments added in the future.
    :return: A Pipeline object containing all the Internet Usage Primary Nodes.
    """

    return Pipeline(
        [
            # -------------------------- Common ----------------------- #
            node(
                func=clean_mkios,
                inputs="l1_rech_mkios_dd",
                outputs="l1_mkios_cleaned",
                name="clean_mkios",
                tags=[
                    "agg_outlet_common_pipeline",
                    "scoring_agg_outlet_common_pipeline",
                ],
            ),
            node(
                func=clean_urp,
                inputs="l1_rech_urp_dd",
                outputs="l1_urp_cleaned",
                name="clean_urp",
                tags=[
                    "agg_outlet_common_pipeline",
                    "scoring_agg_outlet_common_pipeline",
                ],
            ),
            node(
                func=partition_dnkm,
                inputs="l1_dnkm",
                outputs="l1_dnkm_paritioned_by_trx_date",
                name="partition_dnkm",
                tags=["agg_outlet_rev_pipeline", "scoring_agg_outlet_common_pipeline"],
            ),
            # ----------------- GeoSpatial Aggregation ---------------- #
            node(
                func=clean_outlets,
                inputs=["l1_outlet_dim_dd"],
                outputs="l1_unique_outlets",
                name="clean_outlets",
                tags=["agg_outlet_geo_pipeline", "scoring_agg_outlet_geo_pipeline"],
            ),
            node(
                func=get_most_recent_outlet_classification,
                inputs="l1_outlet_dim_dd",
                outputs="l1_outlets_with_most_recent_classf",
                name="get_most_recent_outlet_classification",
                tags=["agg_outlet_geo_pipeline", "scoring_agg_outlet_geo_pipeline"],
            ),
            node(
                func=clean_pois,
                inputs=[
                    "l1_big_poi",
                    "l1_ext_HDX_coast_points",
                    "l1_ext_OSM_primary_roads",
                    "l1_ext_OSM_primary_and_secondary_roads",
                    "l1_ext_OSM_railways",
                    "l1_ext_OSM_POIs",
                ],
                outputs=[
                    "l1_unique_big_pois",
                    "l1_unique_ext_HDX_coast_points",
                    "l1_unique_ext_OSM_primary_roads",
                    "l1_unique_ext_OSM_primary_and_secondary_roads",
                    "l1_unique_ext_OSM_railways",
                    "l1_unique_ext_OSM_POIs",
                ],
                name="clean_pois",
                tags=["agg_outlet_geo_pipeline", "scoring_agg_outlet_geo_pipeline"],
            ),
            node(
                func=get_all_neighbours_for_outlet,
                inputs=["l1_unique_outlets"],
                outputs="l1_outlets_with_neighbours",
                name="get_all_neighbours_for_outlet",
                tags=["agg_outlet_geo_pipeline", "scoring_agg_outlet_geo_pipeline"],
            ),
            node(
                func=get_all_neighbours_for_poi,
                inputs=[
                    "l1_unique_big_pois",
                    "l1_unique_ext_OSM_POIs",
                    "l1_ext_ACLED_conflict_events",
                ],
                outputs=[
                    "l1_big_pois_with_neighbours",
                    "l1_osm_pois_with_neighbours",
                    "l1_conflict_pois_with_neighbours",
                ],
                name="get_all_neighbours_for_poi",
                tags=["agg_outlet_geo_pipeline", "scoring_agg_outlet_geo_pipeline"],
            ),
            node(
                func=get_all_poi_for_an_outlet,
                inputs=[
                    "l1_outlets_with_neighbours",
                    "l1_big_pois_with_neighbours",
                    "l1_osm_pois_with_neighbours",
                    "l1_conflict_pois_with_neighbours",
                ],
                outputs=[
                    "l1_dfj_big_poi_outlet",
                    "l1_dfj_osm_poi_outlet",
                    "l1_dfj_conflict_poi_outlet",
                    "l1_dfj_outlets_near_outlet",
                ],
                name="get_all_poi_for_an_outlet",
                tags=["agg_outlet_geo_pipeline", "scoring_agg_outlet_geo_pipeline"],
            ),
            node(
                func=agg_urp_channel_recharges_by_radius,
                inputs=[
                    "l1_urp_cleaned",
                    "l1_mkios_cleaned",
                    "l1_dnkm",
                    "l1_outlets_with_neighbours",
                ],
                outputs=["l1_mkios_neighb_uniq", "l1_urp_neighb_uniq"],
                name="agg_urp_channel_recharges_by_radius",
                tags=["agg_outlet_geo_pipeline", "scoring_agg_outlet_geo_pipeline"],
            ),
            node(
                func=get_all_neighbours_for_fb_points,
                inputs=["l1_fb_pop"],
                outputs="l1_fb_points_with_neighbours",
                name="get_all_neighbours_for_fb_points",
                tags=["agg_outlet_geo_pipeline", "scoring_agg_outlet_geo_pipeline"],
            ),
            node(
                func=get_all_fb_pop_for_an_outlet,
                inputs=["l1_outlets_with_neighbours", "l1_fb_points_with_neighbours"],
                outputs="l1_dfj_outlet_fb_pop",
                name="get_all_fb_pop_for_an_outlet",
                tags=["agg_outlet_geo_pipeline", "scoring_agg_outlet_geo_pipeline"],
            ),
            # ----------------- Revenue Daily Aggregation ---------------- #
            node(
                func=agg_outlet_digipos_daily,
                inputs=["l1_outlet_dim_dd", "l1_digipos_outlet_reference_dd"],
                outputs=None,
                name="agg_outlet_digipos_daily",
                tags=["agg_outlet_rev_pipeline", "scoring_agg_outlet_rev_pipeline"],
            ),
            node(
                func=subselect_mkios_by_digipos_rs_msisdns,
                inputs=["l1_mkios_cleaned_rev", "l1_outlet_id_msisdn_mapping"],
                outputs="l1_dfj_digipos_mkios",
                name="subselect_mkios_by_digipos_rs_msisdns",
                tags=["agg_outlet_rev_pipeline", "scoring_agg_outlet_rev_pipeline"],
            ),
            node(
                func=agg_recharge_mkios_for_features,
                inputs=["l1_dfj_digipos_mkios"],
                outputs=None,
                name="agg_recharge_mkios_urp_to_daily",
                tags=["agg_outlet_rev_pipeline", "scoring_agg_outlet_rev_pipeline"],
            ),
            node(
                func=calc_physical_voucher_inj,
                inputs=[
                    "l1_claudia_enable",
                    "l1_mkios_cleaned_rev",
                    "l1_outlet_id_msisdn_mapping",
                ],
                outputs=[
                    "l1_pv_inj_lacci_outlet_stats",
                    "l1_pv_inj_city_stats",
                    "l1_pv_inj_rs_msisdn_outlet_stats",
                ],
                name="calc_physical_voucher_inj",
                tags=["agg_outlet_rev_pipeline"],
            ),
            node(
                func=calc_physical_voucher_red,
                inputs=[
                    "l1_claudia_daily_used",
                    "l1_mkios_cleaned_rev",
                    "l1_outlet_id_msisdn_mapping",
                ],
                outputs=[
                    "l1_pv_red_lacci_outlet_stats",
                    "l1_pv_red_city_stats",
                    "l1_pv_red_rs_msisdn_outlet_stats",
                ],
                name="calc_physical_voucher_red",
                tags=["agg_outlet_rev_pipeline", "scoring_agg_outlet_rev_pipeline"],
            ),
            node(
                func=calc_recharges_by_modern_channel,
                inputs=["l1_mkios_cleaned", "l1_urp_cleaned"],
                outputs=None,
                name="calc_recharges_by_modern_channel",
                tags=["agg_outlet_rev_pipeline", "scoring_agg_outlet_rev_pipeline"],
            ),
            node(
                func=agg_outlet_network_dnkm,
                inputs="l1_dnkm_paritioned_by_trx_date",
                outputs=None,
                name="agg_outlet_network_dnkm",
                tags=["agg_outlet_rev_pipeline", "scoring_agg_outlet_rev_pipeline"],
            ),
            node(
                func=calc_modern_channel_recharges_for_all_lacci,
                inputs=["l1_dfj_digipos_mkios", "l1_modern_daily_aggregated"],
                outputs=None,
                name="calc_modern_channel_recharges_for_all_lacci",
                tags=["agg_outlet_rev_pipeline"],
            ),
        ],
        tags=["agg_outlets", "outlets_pipeline",],
    )
