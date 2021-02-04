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

import pyspark.sql.functions as f


def network_aggregation(df, key):
    df = df.groupBy(key, "weekstart").agg(
        f.avg("broadband_revenue").alias("broadband_revenue_avg"),
        f.avg("ccsr_cs").alias("ccsr_cs_avg"),
        f.avg("ccsr_ps").alias("ccsr_ps_avg"),
        f.avg("cssr_cs").alias("cssr_cs_avg"),
        f.avg("cssr_ps").alias("cssr_ps_avg"),
        f.avg("downlink_traffic_volume_mb").alias("downlink_traffic_volume_mb_avg"),
        f.avg("edge_mbyte").alias("edge_mbyte_avg"),
        f.avg("gprs_mbyte").alias("gprs_mbyte_avg"),
        f.avg("hosr_num_sum").alias("hosr_num_sum_avg"),
        f.avg("hsdpa_accesability").alias("hsdpa_accesability_avg"),
        f.avg("hsupa_accesability").alias("hsupa_accesability_avg"),
        f.avg("hsupa_mean_user").alias("hsupa_mean_user_avg"),
        f.avg("ifhosr").alias("ifhosr_avg"),
        f.avg("max_occupancy_pct").alias("max_occupancy_pct_avg"),
        f.avg("max_of_traffic_voice_erl").alias("max_of_traffic_voice_erl_avg"),
        f.avg("payload_hspa_mbyte").alias("payload_hspa_mbyte_avg"),
        f.avg("payload_psr99_mbyte").alias("payload_psr99_mbyte_avg"),
        f.avg("total_payload_mb").alias("total_payload_mb_avg"),
        f.avg("total_throughput_kbps").alias("total_throughput_kbps_avg"),
        f.avg("voice_revenue").alias("voice_revenue_avg"),
        f.avg("volume_voice_traffic_erl").alias("volume_voice_traffic_erl_avg"),
        f.avg("cap_hr30_pct_frside_erl").alias("cap_hr30_pct_frside_erl_avg"),
        f.avg("capable_2g").alias("capable_2g_avg"),
        f.avg("capable_3g").alias("capable_3g_avg"),
        f.avg("dongle").alias("dongle_avg"),
        f.avg("hosr").alias("hosr_avg"),
        f.avg("hosr_denum_sum").alias("hosr_denum_sum_avg"),
        f.avg("other_revenue").alias("other_revenue"),
        f.avg("sms_revenue").alias("sms_revenue"),
        f.avg("capable_4g").alias("capable_4g_avg"),
        f.max("2g_avail").alias("fea_network_2g_avail"),
        f.max("3g_avail").alias("fea_network_3g_avail"),
        f.max("azimuth").alias("fea_network_azimuth"),
        f.max("band").alias("fea_network_band"),
        f.max("branch_name").alias("fea_network_branch_name"),
        f.max("bsc_rnc_name").alias("fea_network_bsc_rnc_name"),
        f.max("bts").alias("fea_network_bts"),
        f.max("bts_sign").alias("fea_network_bts_sign"),
        f.max("cell_name").alias("fea_network_cell_name"),
        f.max("cell_sign").alias("fea_network_cell_sign"),
        f.max("cluster_name").alias("fea_network_cluster_name"),
        f.max("desa").alias("fea_network_desa"),
        f.max("horizontal_beamwidth").alias("fea_network_horizontal_beamwidth"),
        f.max("id_area").alias("fea_network_id_area"),
        f.max("id_branch").alias("fea_network_id_branch"),
        f.max("id_cluster").alias("fea_network_id_cluster"),
        f.max("id_desa").alias("fea_network_id_desa"),
        f.max("id_kab").alias("fea_network_id_kab"),
        f.max("id_kec").alias("fea_network_id_kec"),
        f.max("id_pro").alias("fea_network_id_pro"),
        f.max("id_reg").alias("fea_network_id_reg"),
        f.max("id_subbranch").alias("fea_network_id_subbranch"),
        f.max("kabupaten").alias("fea_network_kabupaten"),
        f.max("kecamatan").alias("fea_network_kecamatan"),
        f.max("lat").alias("fea_network_lat"),
        f.max("lon").alias("fea_network_lon"),
        f.max("mc_class").alias("fea_network_mc_class"),
        f.max("province").alias("fea_network_province"),
        f.max("region").alias("fea_network_region"),
        f.max("site").alias("fea_network_site"),
        f.max("site_id").alias("fea_network_site_id"),
        f.max("site_name").alias("fea_network_site_name"),
        f.max("site_sign").alias("fea_network_site_sign"),
        f.max("vendor").alias("fea_network_vendor"),
        f.max("vertical_beamwidth").alias("fea_network_vertical_beamwidth"),
        f.max("resource_block_utilizing_rate").alias(
            "fea_network_resource_block_utilizing_rate"
        ),
        f.max("tch_trafic_erl_daily").alias("fea_network_tch_trafic_erl_daily"),
        f.max("4g_avail").alias("fea_network_4g_avail"),
        f.max("ne_id").alias("fea_network_ne_id"),
    )
    return df
