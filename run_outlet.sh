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

set -e

#----------------------------------------------------------------------------------
# Aggregations Are Common For Both TRAINING & SCORING
#----------------------------------------------------------------------------------

###################################################################################
##################################### Scoring #####################################
###################################################################################

###################################################################################
###################################### Common #####################################
###################################################################################
kedro run --env=dmp/aggregation --node=clean_mkios
kedro run --env=dmp/aggregation --node=clean_urp
kedro run --env=dmp/aggregation --node=partition_dnkm
#
#
####################################################################################
############################## Revenue Aggregation #################################
####################################################################################
kedro run --env=dmp/aggregation --node=agg_outlet_digipos_daily --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/aggregation --node=subselect_mkios_by_digipos_rs_msisdns --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/aggregation --node=agg_recharge_mkios_urp_to_daily --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/aggregation --node=calc_physical_voucher_red --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/aggregation --node=calc_recharges_by_modern_channel --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/aggregation --node=agg_outlet_network_dnkm --params=first_month:2020-01-01,last_month:2020-01-01
#
#
####################################################################################
########################## Revenue Features  & Master ##############################
####################################################################################
kedro run --env=dmp/scoring --node=calc_fea_outlet_network --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/scoring --node=outlet_revenue_master_scoring --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/scoring --node=fill_time_series_outlet_revenue_master --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/aggregation --node=qa_get_additional_starting_outlet_numbers --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/aggregation --node=track_outlet_filtering
# kedro run --env=dmp/scoring --node=build_master_outlet_rev_hive_table
#
#
####################################################################################
########################### Geospatial Aggregation #################################
####################################################################################
kedro run --env=dmp/aggregation --node=clean_outlets --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/aggregation --node=get_most_recent_outlet_classification --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/aggregation --node=clean_pois
kedro run --env=dmp/aggregation --node=get_all_neighbours_for_outlet
kedro run --env=dmp/aggregation --node=get_all_neighbours_for_poi
kedro run --env=dmp/aggregation --node=get_all_poi_for_an_outlet
kedro run --env=dmp/aggregation --node=agg_urp_channel_recharges_by_radius
kedro run --env=dmp/aggregation --node=get_all_neighbours_for_fb_points
kedro run --env=dmp/aggregation --node=get_all_fb_pop_for_an_outlet
#
#
####################################################################################
####################### Geospatial Features & Master ###############################
####################################################################################
kedro run --env=dmp/scoring --node=calc_pois_outlet_features
kedro run --env=dmp/scoring --node=map_features_to_outlets
kedro run --env=dmp/scoring --node=calc_nearest_poi_and_assign
kedro run --env=dmp/scoring --node=calc_nearest_poi_dist_from_outlet
kedro run --env=dmp/scoring --node=calc_poi_channel_lacci_based_features
kedro run --env=dmp/scoring --node=calc_fb_pop_for_outlets
kedro run --env=dmp/scoring --node=build_master_table_outlet --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/scoring --node=build_master_outlet_geo_hive_table

#----------------------------------------------------------------------------------
#----------------------------------------------------------------------------------
#----------------------------------------------------------------------------------
#----------------------------------------------------------------------------------

###################################################################################
##################################### Training ####################################
###################################################################################

###################################################################################
###################################### Common #####################################
###################################################################################
kedro run --env=dmp/aggregation --node=clean_mkios
kedro run --env=dmp/aggregation --node=clean_urp
kedro run --env=dmp/aggregation --node=partition_dnkm
#
#
####################################################################################
############################## Revenue Aggregation #################################
####################################################################################
kedro run --env=dmp/aggregation --node=agg_outlet_digipos_daily --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/aggregation --node=subselect_mkios_by_digipos_rs_msisdns --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/aggregation --node=agg_recharge_mkios_urp_to_daily --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/aggregation --node=calc_physical_voucher_red --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/aggregation --node=calc_recharges_by_modern_channel --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/aggregation --node=agg_outlet_network_dnkm --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/aggregation --node=calc_physical_voucher_inj --params=first_month:2020-01-01,last_month:2020-01-01 # Extra In TRAINING
kedro run --env=dmp/aggregation --node=calc_modern_channel_recharges_for_all_lacci --params=first_month:2020-01-01,last_month:2020-01-01 # Extra In TRAINING
#
#
####################################################################################
########################## Revenue Features  & Master ##############################
####################################################################################
kedro run --env=dmp/training --node=calc_fea_outlet_network --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/training --node=outlet_revenue_master_training --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/training --node=fill_time_series_outlet_revenue_master --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/aggregation --node=qa_get_additional_starting_outlet_numbers --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/aggregation --node=track_outlet_filtering
# kedro run --env=dmp/training --node=build_master_outlet_rev_hive_table
#
#
####################################################################################
########################### Geospatial Aggregation #################################
####################################################################################
kedro run --env=dmp/aggregation --node=clean_outlets --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/aggregation --node=get_most_recent_outlet_classification --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/aggregation --node=clean_pois
kedro run --env=dmp/aggregation --node=get_all_neighbours_for_outlet
kedro run --env=dmp/aggregation --node=get_all_neighbours_for_poi
kedro run --env=dmp/aggregation --node=get_all_poi_for_an_outlet
kedro run --env=dmp/aggregation --node=agg_urp_channel_recharges_by_radius
kedro run --env=dmp/aggregation --node=get_all_neighbours_for_fb_points
kedro run --env=dmp/aggregation --node=get_all_fb_pop_for_an_outlet
#
#
####################################################################################
####################### Geospatial Features & Master ###############################
####################################################################################
kedro run --env=dmp/training --node=calc_pois_outlet_features
kedro run --env=dmp/training --node=map_features_to_outlets
kedro run --env=dmp/training --node=calc_nearest_poi_and_assign
kedro run --env=dmp/training --node=calc_nearest_poi_dist_from_outlet
kedro run --env=dmp/training --node=calc_poi_channel_lacci_based_features
kedro run --env=dmp/training --node=calc_fb_pop_for_outlets
kedro run --env=dmp/training --node=build_master_table_outlet --params=first_month:2020-01-01,last_month:2020-01-01
kedro run --env=dmp/training --node=build_master_outlet_geo_hive_table
