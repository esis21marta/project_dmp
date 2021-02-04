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

from src.dmp.pipelines.dmp._05_model_input.build_hive_master_table import (
    build_hive_master_table,
)
from src.dmp.pipelines.dmp._05_model_input.build_master import (
    build_master_dimension,
    build_master_table,
    build_master_table_node_generator,
    build_msisdn_master_table,
    build_outlet_geospatial_master_table,
)
from src.dmp.pipelines.dmp._05_model_input.unpacking_master_table import (
    unpacking_node_generator,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    Create the Master Table Pipeline.

    Returns:
        A Pipeline object containing all the Master Table Nodes.
    """
    return Pipeline(
        [
            node(
                func=build_master_dimension,
                inputs=["l1_weekly_scaffold", "partner_msisdns", "params:pipeline"],
                outputs="l6_master_dimension",
                name="build_master_table_dimension",
                tags=[
                    "de_msisdn_master_pipeline",
                    "de_pipeline",
                    "de_master_pipeline",
                    "master_scoring_pipeline",
                    "scoring_pipeline",
                ],
            ),
            *build_master_table_node_generator(
                pipeline="training",
                inputs=[
                    "params:pipeline",
                    "params:shuffle_partitions_2k",
                    "params:feature_master_msisdn",
                    "l6_master_dimension",
                    # Feature Tables
                    # "l2_fea_device_internal", source table is not productionised
                    "l2_fea_handset",
                    "l2_internet_usage",
                    "l4_fea_commercial_text_messaging",
                    "l4_fea_customer_los",
                    "l4_fea_handset_imei",
                    "l4_fea_mytsel",
                    "l4_fea_network_msisdn",
                    "l4_fea_nonmacro_product",
                    "l4_fea_payu_usage",
                    "l4_fea_revenue_base_payu_pkg",
                    "l4_fea_revenue_monthly_spend",
                    "l4_fea_revenue_mytsel",
                    "l4_fea_revenue_salary",
                    "l4_fea_revenue_spend_pattern",
                    "l4_fea_revenue_threshold_sms_dls_roam",
                    "l4_fea_revenue_threshold_voice_data",
                    "l4_fea_revenue_weekend",
                    "l4_fea_text_messaging",
                    "l4_fea_voice_calling",
                    "l5_airtime_loan_target_variable",
                    "l5_fea_airtime_loan",
                    "l5_fea_airtime_loan_offer_n_recharge",
                    "l5_fea_airtime_loan_offer_n_revenue",
                    "l5_fea_commercial_text_messaging_covid_final",
                    "l5_fea_customer_activation_outlet",
                    "l5_fea_customer_expiry",
                    "l5_fea_customer_nsb",
                    "l5_fea_customer_points",
                    "l5_fea_customer_profile",
                    "l5_fea_internet_app_usage",
                    "l5_fea_internet_app_usage_competitor",
                    "l5_fea_internet_connection",
                    "l5_fea_internet_app_usage_covid_final",
                    "l5_fea_internet_app_usage_covid_trend",
                    "l5_fea_map_msisdn_to_tutela_network",
                    "l5_fea_internet_usage_covid_trend",
                    "l5_fea_internet_usage_covid_final",
                    "l5_fea_recharge_covid_final",
                    "l5_fea_mobility",
                    "l5_fea_recharge",
                    "l5_fea_recharge_mytsel",
                    "l5_fea_text_messaging_covid_final",
                    "l5_fea_voice_calling_covid_final",
                ],
                tags=[
                    "build_training_master_table_msisdn",
                    "de_msisdn_master_pipeline",
                    "de_pipeline",
                    "de_master_pipeline",
                ],
            ),
            node(
                func=build_msisdn_master_table,
                inputs=[
                    "params:pipeline",
                    "params:feature_master_msisdn",
                    "l6_master_dimension",
                    "l4_fea_customer_los",
                    "l5_fea_customer_profile",
                    "l2_fea_handset",
                    "l5_fea_internet_app_usage",
                    "l5_fea_internet_connection",
                    "l5_fea_social_media",
                    "l2_internet_usage",
                    "l5_fea_recharge",
                    "l4_fea_revenue_weekend",
                    "l4_fea_revenue_base_payu_pkg",
                    "l4_fea_revenue_spend_pattern",
                    "l4_fea_revenue_monthly_spend",
                    "l4_fea_revenue_salary",
                    "l4_fea_revenue_threshold_voice_data",
                    "l4_fea_revenue_threshold_sms_dls_roam",
                    "l4_fea_network_msisdn",
                    "l4_fea_payu_usage",
                    "l4_fea_text_messaging",
                    "l4_fea_commercial_text_messaging",
                    "l4_fea_voice_calling",
                    "l4_fea_mytsel",
                    "l4_fea_revenue_mytsel",
                    "l5_fea_recharge_mytsel",
                    "l5_fea_map_msisdn_to_tutela_network",
                    "l5_fea_internet_app_usage_competitor",
                    "l5_fea_customer_expiry",
                    "l5_fea_customer_nsb",
                    "l5_fea_customer_points",
                    "l5_fea_customer_activation_outlet",
                    "l4_fea_handset_imei",
                    "l5_airtime_loan_target_variable",
                    "l5_fea_airtime_loan",
                    "l5_fea_airtime_loan_offer_n_recharge",
                    "l5_fea_airtime_loan_offer_n_revenue",
                ],
                outputs="l6_master_metadata",  # l6_master
                name="build_scoring_master_table",
                tags=[
                    "de_scoring_master_pipeline",
                    "master_scoring_pipeline",
                    "scoring_pipeline",
                ],
            ),
            node(
                func=build_hive_master_table("l6_master", "l6_master_hive"),
                inputs="params:pipeline",
                outputs=None,
                name="build_master_hive_table_msisdn",
                tags=[
                    "de_scoring_master_pipeline",
                    "master_scoring_pipeline",
                    "scoring_pipeline",
                ],
            ),
            *unpacking_node_generator(),
            node(
                func=build_master_table("lac_ci"),
                inputs=[
                    "l1_weekly_scaffold_lacci",
                    "params:feature_master_lacci",
                    "l4_fea_network_lacci",
                ],
                outputs="l6_lacci_master_static",
                name="build_master_table_lacci",
                tags=["de_lacci_master_pipeline", "de_pipeline", "de_master_pipeline"],
            ),
            node(
                func=build_master_table("lac"),
                inputs=[
                    "l1_weekly_scaffold_lac",
                    "params:feature_master_lac",
                    "l4_fea_network_lac",
                ],
                outputs="l6_lac_master_static",
                name="build_master_table_lac",
                tags=["de_lacci_master_pipeline", "de_pipeline", "de_master_pipeline"],
            ),
            node(
                func=build_outlet_geospatial_master_table,
                inputs=[
                    "params:reseller_features_mode",
                    "params:output_features",
                    "params:feature_master_outlet_geo",
                    "l1_unique_outlets",
                    # ------BPS Features------- #
                    "l1_ext_bps_susenas",
                    "l1_ext_bps_podes",
                    "l1_ext_bps_sakernas",
                    # ------POI Features------- #
                    "l2_closest_big_pois_to_outlet",
                    "l2_closest_osm_pois_to_outlet",
                    "l2_total_big_pois_count_for_outlet",
                    "l2_total_conflict_count_outlet",
                    "l2_fb_pop_for_outlet",
                    "l2_outlet_geospatial_pick_nearest_and_assign",
                    "l2_outlet_geospatial_nearest_poi_dist_from_outlet",
                    "l2_outlet_to_outlet_min_dist",
                    "l2_outlet_counts_per_outlet_in_radius",
                    "l2_types_of_outlets_near_an_outlet",
                    "l2_outlets_in_radiuses_based_on_classf",
                    "l2_outlets_cashflow_in_radiuses_based_on_classf",
                    "l2_min_dist_to_lacci_from_outlet",
                    "l2_mkios_channel_rech",
                    "l2_urp_channel_rech",
                    # ------Map To Outlet Features------- #
                    # "l2_fb_share_for_outlet",
                    # "l2_4g_bts_share_for_outlet",
                    "l2_outlet_archetype",
                    "l2_outlet_dealer",
                    # "l2_outlet_ookla",
                    # "l2_outlet_opensignal",
                    "l2_outlet_digistar_sellthru_det",
                    "l2_outlet_salesforce",
                ],
                outputs="l3_outlet_master_static",
                name="build_master_table_outlet",
                tags=[
                    "de_outlet_geo_master_pipeline",
                    "scoring_master_outlet_geo_pipeline",
                ],
            ),
            node(
                func=build_hive_master_table(
                    "l3_outlet_master_static", "l3_outlet_geospatial_master_static_hive"
                ),
                inputs="params:pipeline",
                outputs=None,
                name="build_master_outlet_geo_hive_table",
                tags=[
                    "de_outlet_geo_master_pipeline",
                    "scoring_master_outlet_geo_pipeline",
                ],
            ),
            node(
                func=build_hive_master_table(
                    "l3_outlet_master_revenue_daily", "l3_outlet_revenue_master_hive",
                ),
                inputs="params:pipeline",
                outputs=None,
                name="build_master_outlet_rev_hive_table",
                tags=[
                    "de_outlet_rev_master_pipeline",
                    "scoring_master_outlet_rev_pipeline",
                ],
            ),
        ],
    )
