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

from pyspark.sql import Row

from src.dmp.pipelines.dmp._05_model_input.build_master import build_master_table


class TestBuildMasterTable:
    def test_build_master(self, spark_session):
        """
        Pipleine type is training and incremenetal is set to False, all the feature table merged
        """
        sc = spark_session.sparkContext

        df_master_dimension = sc.parallelize(
            [
                ("789", "2019-10-14", "877"),
                ("456", "2019-10-07", "976"),
                ("123", "2019-10-07", "987"),
            ]
        )

        df_master_dimension = spark_session.createDataFrame(
            df_master_dimension.map(
                lambda x: Row(msisdn=x[0], weekstart=x[1], fea_los=x[2])
            )
        )

        df_feature_a = sc.parallelize(
            [
                ("123", "2019-10-07", "a"),
                ("456", "2019-10-07", "a"),
                ("456", "2019-10-19", "b"),
                ("789", "2019-10-14", "c"),
                ("998", "2019-10-14", "c"),
            ]
        )

        df_feature_b = sc.parallelize(
            [
                ("456", "2019-10-07", "a"),
                ("456", "2019-10-19", "b"),
                ("789", "2019-10-14", "c"),
                ("998", "2019-10-14", "c"),
            ]
        )

        df_feature_c = sc.parallelize(
            [
                ("123", "2019-10-07", "x"),
                ("456", "2019-10-07", "y"),
                ("789", "2019-10-14", "z"),
            ]
        )

        df_feature_a = spark_session.createDataFrame(
            df_feature_a.map(lambda x: Row(msisdn=x[0], weekstart=x[1], fea_a=x[2]))
        )

        df_feature_b = spark_session.createDataFrame(
            df_feature_b.map(lambda x: Row(msisdn=x[0], weekstart=x[1], fea_b=x[2]))
        )

        df_feature_c = spark_session.createDataFrame(
            df_feature_c.map(
                lambda x: Row(msisdn=x[0], weekstart=x[1], fea_c_edit=x[2])
            )
        )

        params = dict(
            fea_los="fea_los", fea_a="fea_a", fea_b="fea_b", fea_c_edit="fea_c"
        )

        actual_result_df = build_master_table("msisdn")(
            df_master_dimension, params, df_feature_a, df_feature_b, df_feature_c
        )

        actual_result_list = [
            [row.msisdn, row.weekstart, row.fea_c, row.fea_a, row.fea_b, row.fea_los]
            for row in actual_result_df.collect()
        ]

        expected_result_list = [
            ["123", "2019-10-07", "x", "a", None, "987"],
            ["789", "2019-10-14", "z", "c", "c", "877"],
            ["456", "2019-10-07", "y", "a", "a", "976"],
        ]

        assert sorted(actual_result_df.columns) == sorted(
            ["msisdn", "weekstart", "fea_los", "fea_c", "fea_b", "fea_a"]
        )
        assert sorted(actual_result_list) == sorted(expected_result_list)
