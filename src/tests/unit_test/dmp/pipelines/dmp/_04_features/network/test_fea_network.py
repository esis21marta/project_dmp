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

from src.dmp.pipelines.dmp._04_features.network.fea_network import fea_network


class TestInternetUsageAggregate:
    def fea_network(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        df_1 = sc.parallelize(
            [("111", "2019-10-07", 1, 2), ("111", "2019-09-30", 1, 2),]
        )

        df_2 = sc.parallelize(
            [("111", "2019-10-07", 3, 4), ("111", "2019-09-30", 3, 4),]
        )

        df_3 = sc.parallelize(
            [("111", "2019-10-07", 5, 6), ("111", "2019-09-30", 5, 6),]
        )

        df_data = sc.parallelize(
            [
                ("222", "2019-10-07", "KAIMANA KOTA", "KAIMANA"),
                ("111", "2019-10-07", "WAGOM SELATAN", "FAKFAK"),
                ("111", "2019-09-30", "WAGOM SELATAN", "FAKFAK"),
            ]
        )

        df_1 = spark_session.createDataFrame(
            df_1.map(
                lambda x: Row(
                    msisdn=x[0],
                    weekstart=x[1],
                    fea_network_broadband_revenue_avg_01m_to_00w=x[2],
                    fea_network_ccsr_cs_avg_01m_to_00w=x[3],
                )
            )
        )

        df_2 = spark_session.createDataFrame(
            df_2.map(
                lambda x: Row(
                    msisdn=x[0],
                    weekstart=x[1],
                    fea_network_edge_mbyte_avg_01m_to_00w=x[2],
                    fea_network_gprs_mbyte_avg_01m_to_00w=x[3],
                )
            )
        )

        df_3 = spark_session.createDataFrame(
            df_3.map(
                lambda x: Row(
                    msisdn=x[0],
                    weekstart=x[1],
                    fea_network_hsupa_mean_user_avg_01m_to_00w=x[2],
                    fea_network_ifhosr_avg_01m_to_00w=x[3],
                )
            )
        )

        df_data = spark_session.createDataFrame(
            df_data.map(
                lambda x: Row(
                    msisdn=x[0],
                    weekstart=x[1],
                    fea_network_desa=x[2],
                    fea_network_kecamatan=x[3],
                )
            )
        )

        out = fea_network("msisdn")(df_data)

        out_list = [
            [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9],]
            for i in out.collect()
        ]

        expected_res = [
            ["111", "2019-09-30", "WAGOM SELATAN", "FAKFAK", 1, 2, 3, 4, 5, 6,],
            ["111", "2019-10-07", "WAGOM SELATAN", "FAKFAK", 1, 2, 3, 4, 5, 6,],
            [
                "222",
                "2019-10-07",
                "KAIMANA KOTA",
                "KAIMANA",
                None,
                None,
                None,
                None,
                None,
                None,
            ],
        ]

        assert sorted(out_list) == sorted(expected_res)
