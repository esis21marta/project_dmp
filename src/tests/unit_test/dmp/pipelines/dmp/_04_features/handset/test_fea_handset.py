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

import datetime
from unittest import mock

import yaml
from pyspark.sql import Row, SparkSession

from src.dmp.pipelines.dmp._04_features.handset.fea_handset import fea_handset


@mock.patch(
    "src.dmp.pipelines.dmp._04_features.handset.fea_handset.get_start_date",
    return_value=datetime.date(2018, 10, 10),
    autospec=True,
)
@mock.patch(
    "src.dmp.pipelines.dmp._04_features.handset.fea_handset.get_end_date",
    return_value=datetime.date(2020, 10, 10),
    autospec=True,
)
class TestFeatureHandset:
    config_feature = yaml.load(open("conf/dmp/training/parameters_feature.yml"))[
        "config_feature"
    ]

    def test_fea_device_all(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        sc = spark_session.sparkContext

        df_device_lj_scaff = sc.parallelize(
            [
                # Case A) Sanity test - 1 manufacturer, 1 imei, 1 device, 1 market for that msisdn-weekstart
                (
                    "111",
                    "2019-10-07",
                    ["manufacturer-1"],
                    ["tac-1000"],
                    ["devicetype-1"],
                    ["marketname-1"],
                ),
                # Case B) Sanity test - 2 manufacturer, 2 imei, 2 device, 2 market for that msisdn-weekstart
                (
                    "111",
                    "2019-10-14",
                    ["manufacturer-2", "manufacturer-3"],
                    ["tac-2000", "tac-3000"],
                    ["devicetype-3", "devicetype-2"],
                    ["marketname-2", "marketname-3"],
                ),
                # Case C) Edge case - No manufacturer, no imei, no device, no market for that msisdn-weekstart
                ("222", "2019-10-14", [], [], [], []),
            ]
        )

        df_device_lj_scaff = spark_session.createDataFrame(
            df_device_lj_scaff.map(
                lambda x: Row(
                    msisdn=x[0],
                    weekstart=x[1],
                    manufacturer=x[2],
                    imeis=x[3],
                    device_types=x[4],
                    market_names=x[5],
                )
            )
        )

        df_first_2_handset = sc.parallelize(
            [
                # For Case A and Case B
                ("111", "2019-10-01", "manufacturer-1", "manufacturer-2")
            ]
        )

        df_first_2_handset = spark_session.createDataFrame(
            df_first_2_handset.map(
                lambda x: Row(
                    msisdn=x[0],
                    month=x[1],
                    first_handset_manufacturer=x[2],
                    second_handset_manufacturer=x[3],
                )
            )
        )

        out_cols = [
            "msisdn",
            "weekstart",
            "fea_handset_market_name",
            "fea_handset_type",
            "fea_handset_imeis",
            "fea_handset_count_01m",
            "fea_handset_make_cur",
            "fea_handset_make_01m",
            "fea_handset_changed_count_01m",
            # TODO uncomment below when hotfix is removed in src.dmp.pipelines.dmp._04_features.handset.fea_handset.fea_handset
            # "fea_handset_first_handset_manufacturer",
            # "fea_handset_second_handset_manufacturer",
        ]

        out = fea_handset(df_device_lj_scaff, self.config_feature, "all", []).select(
            out_cols
        )
        out_list = [[i[idx] for idx in range(len(out_cols))] for i in out.collect()]

        # TODO uncomment below when hotfix is removed in src.dmp.pipelines.dmp._04_features.handset.fea_handset.fea_handset
        # assert(out_list == [
        #     ['111', '2019-10-07', None, None, ['tac-1000'], 1, None, None, 0, 'manufacturer-1', 'manufacturer-2'],
        #     ['111', '2019-10-14', 'marketname-1', 'devicetype-1', ['tac-2000', 'tac-3000'], 3, 'manufacturer-1', None, 2,
        #      'manufacturer-1', 'manufacturer-2'],
        #     ['222', '2019-10-14', None, None, [], 0, None, None, 0, None, None]
        # ])

        assert sorted(out_list) == [
            ["111", "2019-10-07", None, None, ["tac-1000"], 1, None, None, 0],
            [
                "111",
                "2019-10-14",
                "marketname-1",
                "devicetype-1",
                ["tac-2000", "tac-3000"],
                3,
                "manufacturer-1",
                None,
                2,
            ],
            ["222", "2019-10-14", None, None, [], 0, None, None, 0],
        ]
