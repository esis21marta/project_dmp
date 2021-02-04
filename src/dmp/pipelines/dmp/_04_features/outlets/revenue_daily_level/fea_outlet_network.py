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

from utils import get_required_output_columns


def calc_fea_outlet_network(
    df_dnkm_agg: pyspark.sql.DataFrame,
    dfj_mkios_digipos: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: List[str],
):
    df_dnkm_agg = df_dnkm_agg.withColumnRenamed("trx_date", "trx_date_dnkm")
    dfj_dnkm_agg_mkios_digipos_by_subs_lacci_id = dfj_mkios_digipos.join(
        df_dnkm_agg,
        [
            df_dnkm_agg.lac_ci == dfj_mkios_digipos.subs_lacci_id,
            df_dnkm_agg.trx_date_dnkm == dfj_mkios_digipos.trx_date,
        ],
        how="inner",
    )

    df_hsupa_per_outlet_id_trx_date = dfj_dnkm_agg_mkios_digipos_by_subs_lacci_id.groupBy(
        "outlet_id", "trx_date"
    ).agg(
        f.avg("hsupa_mean_user").alias("fea_outlets_network_hsupa_mean_user_avg"),
        f.avg("hsdpa_accesability").alias("fea_outlets_network_hsdpa_accesability_avg"),
        f.sum("hsdpa_accesability_denum_sum").alias(
            "fea_outlets_network_hsdpa_accesability_denum_sum"
        ),
        f.sum("hsdpa_accesability_num_sum").alias(
            "fea_outlets_network_hsdpa_accesability_num_sum"
        ),
        f.sum("hsdpa_retainability_denum_sum").alias(
            "fea_outlets_network_hsdpa_retainability_denum_sum"
        ),
        f.sum("hsdpa_retainability_num_sum").alias(
            "fea_outlets_network_hsdpa_retainability_num_sum"
        ),
        f.avg("hsupa_accesability").alias("fea_outlets_network_hsupa_accesability_avg"),
        f.sum("hsupa_accesability_denum_sum").alias(
            "fea_outlets_network_hsupa_accesability_denum_sum"
        ),
        f.sum("hsupa_accesability_num_sum").alias(
            "fea_outlets_network_hsupa_accesability_num_sum"
        ),
        f.sum("hsupa_retainability_denum_sum").alias(
            "fea_outlets_network_hsupa_retainability_denum_sum"
        ),
        f.sum("hsupa_retainability_num_sum").alias(
            "fea_outlets_network_hsupa_retainability_num_sum"
        ),
        f.avg("hsdpa_retainability").alias(
            "fea_outlets_network_hsdpa_retainability_avg"
        ),
        f.avg("hsupa_retainability").alias(
            "fea_outlets_network_hsupa_retainability_avg"
        ),
    )

    dfj_dnkm_agg_mkios_digipos_by_rs_lacci_id = dfj_mkios_digipos.join(
        df_dnkm_agg,
        [
            df_dnkm_agg.lac_ci == dfj_mkios_digipos.rs_lacci_id,
            df_dnkm_agg.trx_date_dnkm == dfj_mkios_digipos.trx_date,
        ],
        how="inner",
    )

    df_3g_and_payload_per_outlet_id_trx_date = dfj_dnkm_agg_mkios_digipos_by_rs_lacci_id.groupBy(
        "outlet_id", "trx_date"
    ).agg(
        f.sum("3g_throughput_kbps").alias("fea_outlets_network_3g_throughput_kbps_sum"),
        f.sum("payload_hspa_mbyte").alias("fea_outlets_network_payload_hspa_mbyte_sum"),
        f.sum("payload_psr99_mbyte").alias(
            "fea_outlets_network_payload_psr99_mbyte_sum"
        ),
        f.avg("3g_throughput_kbps").alias("fea_outlets_network_3g_throughput_kbps_avg"),
        f.avg("payload_hspa_mbyte").alias("fea_outlets_network_payload_hspa_mbyte_avg"),
        f.avg("payload_psr99_mbyte").alias(
            "fea_outlets_network_payload_psr99_mbyte_avg"
        ),
    )

    required_output_columns_hsupa_per_outlet_id_trx_date = get_required_output_columns(
        output_features=df_hsupa_per_outlet_id_trx_date.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id", "trx_date"],
    )

    required_output_columns_3g_and_payload_per_outlet_id_trx_date = get_required_output_columns(
        output_features=df_3g_and_payload_per_outlet_id_trx_date.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id", "trx_date"],
    )

    df_hsupa_per_outlet_id_trx_date = df_hsupa_per_outlet_id_trx_date.select(
        required_output_columns_hsupa_per_outlet_id_trx_date
    )
    df_3g_and_payload_per_outlet_id_trx_date = df_3g_and_payload_per_outlet_id_trx_date.select(
        required_output_columns_3g_and_payload_per_outlet_id_trx_date
    )

    return [df_hsupa_per_outlet_id_trx_date, df_3g_and_payload_per_outlet_id_trx_date]
