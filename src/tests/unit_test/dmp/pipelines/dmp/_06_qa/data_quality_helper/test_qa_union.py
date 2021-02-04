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

from src.dmp.pipelines.dmp._06_qa.data_quality_helper.qa_union import qa_union


class TestQAUnion:
    def test_union_all_by_name(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        df_1 = sc.parallelize(
            [
                ("2019-10-14", "2036794455", "Grand Marquis"),
                ("2019-10-21", "2036794456", "GT500"),
                ("2019-10-21", "2036794455", "62"),
            ]
        )

        df_1 = spark_session.createDataFrame(
            df_1.map(lambda x: Row(weekstart=x[0], msisdn=x[1], feature_a=x[2]))
        )

        df_2 = sc.parallelize(
            [
                ("2019-10-01", "1599677226", "Astor", 530),
                ("2019-10-01", "2036794456", "Sie", 934),
                ("2019-10-01", "7524080697", "Tribute", 200),
            ]
        )

        df_2 = spark_session.createDataFrame(
            df_2.map(
                lambda x: Row(
                    weekstart=x[0], msisdn=x[1], feature_a=x[2], feature_b=x[3]
                )
            )
        )

        df_3 = sc.parallelize(
            [
                ("2019-10-01", "1599677226", False),
                ("2019-10-01", "2036794456", True),
                ("2019-10-01", "7524080697", False),
            ]
        )

        df_3 = spark_session.createDataFrame(
            df_3.map(lambda x: Row(weekstart=x[0], msisdn=x[1], feature_c=x[2]))
        )

        df_result_actual = qa_union(table_1=df_1, table_2=df_2, table_3=df_3)

        actual_combined_df_list = [
            [
                row.msisdn,
                row.weekstart,
                row.feature_a,
                row.feature_b,
                row.feature_c,
                row.table_name,
            ]
            for row in df_result_actual.collect()
        ]

        expected_combined_df_list = [
            ["2036794455", "2019-10-14", "Grand Marquis", None, None, "table_1"],
            ["2036794456", "2019-10-21", "GT500", None, None, "table_1"],
            ["2036794455", "2019-10-21", "62", None, None, "table_1"],
            ["1599677226", "2019-10-01", "Astor", 530, None, "table_2"],
            ["2036794456", "2019-10-01", "Sie", 934, None, "table_2"],
            ["7524080697", "2019-10-01", "Tribute", 200, None, "table_2"],
            ["1599677226", "2019-10-01", None, None, False, "table_3"],
            ["2036794456", "2019-10-01", None, None, True, "table_3"],
            ["7524080697", "2019-10-01", None, None, False, "table_3"],
        ]

        assert sorted(df_result_actual.columns) == sorted(
            ["weekstart", "msisdn", "feature_a", "feature_b", "feature_c", "table_name"]
        )
        assert actual_combined_df_list == expected_combined_df_list
