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

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import FloatType, StringType, StructField, StructType

from src.dmp.pipelines.dmp._01_aggregation.network.create_network_lacci_weekly import (
    _weekly_aggregation as create_network_lacci_weekly,
)
from src.dmp.pipelines.dmp._01_aggregation.network.create_network_weekly import (
    _weekly_aggregation as create_network_weekly,
)

MCK_SALES_SCHEMA = StructType(
    [
        StructField("lac_ci", StringType()),
        StructField("event_date", StringType()),
        StructField("desa", StringType()),
        StructField("kecamatan", StringType()),
        StructField("broadband_revenue", FloatType()),
        StructField("voice_revenue", FloatType()),
        StructField("ccsr_cs", FloatType()),
        StructField("ccsr_ps", FloatType()),
        StructField("cssr_cs", FloatType()),
        StructField("cssr_ps", FloatType()),
        StructField("hosr_num_sum", FloatType()),
        StructField("hosr", FloatType()),
        StructField("downlink_traffic_volume_mb", FloatType()),
        StructField("edge_mbyte", FloatType()),
        StructField("gprs_mbyte", FloatType()),
        StructField("dongle", FloatType()),
        StructField("hsdpa_accesability", FloatType()),
        StructField("hsupa_accesability", FloatType()),
        StructField("hsupa_mean_user", FloatType()),
        StructField("ifhosr", FloatType()),
        StructField("max_occupancy_pct", FloatType()),
        StructField("max_of_traffic_voice_erl", FloatType()),
        StructField("payload_hspa_mbyte", FloatType()),
        StructField("payload_psr99_mbyte", FloatType()),
        StructField("total_payload_mb", FloatType()),
        StructField("total_throughput_kbps", FloatType()),
        StructField("volume_voice_traffic_erl", FloatType()),
        StructField("cap_hr30_pct_frside_erl", FloatType()),
        StructField("capable_2g", FloatType()),
        StructField("capable_3g", FloatType()),
        StructField("hosr_denum_sum", FloatType()),
        StructField("other_revenue", FloatType()),
        StructField("sms_revenue", FloatType()),
        StructField("capable_4g", FloatType()),
        StructField("2g_avail", StringType()),
        StructField("3g_avail", StringType()),
        StructField("azimuth", StringType()),
        StructField("band", StringType()),
        StructField("branch_name", StringType()),
        StructField("bsc_rnc_name", StringType()),
        StructField("bts", StringType()),
        StructField("bts_sign", StringType()),
        StructField("cell_name", StringType()),
        StructField("cell_sign", StringType()),
        StructField("cluster_name", StringType()),
        StructField("horizontal_beamwidth", StringType()),
        StructField("id_area", StringType()),
        StructField("id_branch", StringType()),
        StructField("id_cluster", StringType()),
        StructField("id_desa", StringType()),
        StructField("id_kab", StringType()),
        StructField("id_kec", StringType()),
        StructField("id_pro", StringType()),
        StructField("id_reg", StringType()),
        StructField("id_subbranch", StringType()),
        StructField("kabupaten", StringType()),
        StructField("lat", StringType()),
        StructField("lon", StringType()),
        StructField("mc_class", StringType()),
        StructField("province", StringType()),
        StructField("region", StringType()),
        StructField("site", StringType()),
        StructField("site_id", StringType()),
        StructField("site_name", StringType()),
        StructField("site_sign", StringType()),
        StructField("vendor", StringType()),
        StructField("vertical_beamwidth", StringType()),
        StructField("resource_block_utilizing_rate", StringType()),
        StructField("tch_trafic_erl_daily", StringType()),
        StructField("4g_avail", StringType()),
        StructField("ne_id", StringType()),
    ]
)


class TestNetworkAggregate:
    def test_agg_network_msisdn_to_weekly(self, spark_session: SparkSession):
        sc = spark_session.sparkContext
        all_null = [None for i in range(65)]
        mck_sales_dummy = sc.parallelize(
            [
                (
                    "AOOOD-AAAEC",
                    "2019-12-23",
                    "WAGOM SELATAN",
                    "FAKFAK",
                    66866.75,
                    79206.14,
                    *all_null,
                ),
                (
                    "AOOOD-ADDBA",
                    "2019-12-23",
                    "KAIMANA KOTA",
                    "KAIMANA",
                    47705.06,
                    148943.14,
                    *all_null,
                ),
                (
                    "AOOOD-ADDBG",
                    "2019-12-23",
                    "KAIMANA KOTA",
                    "KAIMANA",
                    135668.71,
                    365306.54,
                    *all_null,
                ),
                (
                    "AOOOD-AAAEC",
                    "2019-12-25",
                    "WAGOM SELATAN",
                    "FAKFAK",
                    57686.78,
                    30960.44,
                    *all_null,
                ),
                (
                    "AOOOD-ADDBA",
                    "2019-12-25",
                    "KAIMANA KOTA",
                    "KAIMANA",
                    35915.58,
                    175802.44,
                    *all_null,
                ),
            ]
        )
        cb_pre_dd = sc.parallelize(
            [
                (111, "2019-12-23", "AOOOD", "ADDBA"),
                (222, "2019-12-23", "AOOOD", "ADDBA"),
                (333, "2019-12-23", "AOOOD", "ADDBG"),
                (444, "2019-12-23", "AOOOD", "ADDBG"),
                (555, "2019-12-23", "AOOOD", "AAAEC"),
                (666, "2019-12-25", "AOOOD", "AAAEC"),
                (777, "2019-12-05", "AOOOD", "ADDBA"),
                (888, "2019-12-30", "AOOOD", "ADDBG"),
            ]
        )
        cb_post_dd = sc.parallelize(
            [
                (628177777777, "2019-12-25", "AOOOD", "ADDBA"),
                (628128888888, "2019-12-30", "AOOOD", "ADDBA"),
            ]
        )
        expected_results_msisdn = [
            [111, "2019-12-30", "KAIMANA KOTA", "KAIMANA", 47705.06, 148943.14,],
            [555, "2019-12-30", "WAGOM SELATAN", "FAKFAK", 66866.75, 79206.14],
            [222, "2019-12-30", "KAIMANA KOTA", "KAIMANA", 47705.06, 148943.14,],
            [444, "2019-12-30", "KAIMANA KOTA", "KAIMANA", 135668.7, 365306.53,],
            [666, "2019-12-30", "WAGOM SELATAN", "FAKFAK", 57686.78, 30960.44],
            [333, "2019-12-30", "KAIMANA KOTA", "KAIMANA", 135668.7, 365306.53,],
            [
                628177777777,
                "2019-12-30",
                "KAIMANA KOTA",
                "KAIMANA",
                35915.58,
                175802.44,
            ],
        ]

        expected_results_lacci = [
            [
                "AOOOD-ADDBA",
                "2019-12-30",
                "KAIMANA KOTA",
                "KAIMANA",
                41810.32,
                162372.79,
            ],
            [
                "AOOOD-ADDBG",
                "2019-12-30",
                "KAIMANA KOTA",
                "KAIMANA",
                135668.7,
                365306.53,
            ],
            [
                "AOOOD-AAAEC",
                "2019-12-30",
                "WAGOM SELATAN",
                "FAKFAK",
                62276.77,
                55083.29,
            ],
        ]

        df_mck_sales = spark_session.createDataFrame(
            mck_sales_dummy, schema=MCK_SALES_SCHEMA
        )
        cb_pre_dd = spark_session.createDataFrame(
            cb_pre_dd.map(
                lambda x: Row(msisdn=x[0], event_date=x[1], lac=x[2], ci=x[3])
            )
        )

        cb_post_dd = spark_session.createDataFrame(
            cb_post_dd.map(
                lambda x: Row(msisdn=x[0], event_date=x[1], lac=x[2], ci=x[3])
            )
        )
        res = create_network_weekly(df_mck_sales, cb_pre_dd, cb_post_dd)

        res_lacci, res_lac = create_network_lacci_weekly(df_mck_sales)

        res = res.select(
            "msisdn",
            "weekstart",
            "fea_network_desa",
            "fea_network_kecamatan",
            "broadband_revenue_avg",
            "voice_revenue_avg",
        )

        res_lacci = res_lacci.select(
            "lac_ci",
            "weekstart",
            "fea_network_desa",
            "fea_network_kecamatan",
            "broadband_revenue_avg",
            "voice_revenue_avg",
        )
        res = [
            [
                i[0],
                i[1].strftime("%Y-%m-%d"),
                i[2],
                i[3],
                round(i[4], 2),
                round(i[5], 2),
            ]
            for i in res.collect()
        ]

        res_lacci = [
            [
                i[0],
                i[1].strftime("%Y-%m-%d"),
                i[2],
                i[3],
                round(i[4], 2),
                round(i[5], 2),
            ]
            for i in res_lacci.collect()
        ]

        assert sorted(res) == sorted(expected_results_msisdn)
        assert sorted(res_lacci) == sorted(expected_results_lacci)
