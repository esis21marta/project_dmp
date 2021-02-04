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

from src.dmp.pipelines.dmp._06_qa.metrics.consistency import get_consistency


class TestConsistency:
    def test_consistency(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        df_new = sc.parallelize(
            [
                (
                    "2019-10-01",
                    "1599677226",
                    "Astor",
                    530,
                    "['abc','abc','abc']",
                    False,
                ),
                ("2019-10-01", "2036794456", "Sie", 934, [], True),
                ("2019-10-01", "7524080697", "Tribute", 200, [], False),
                ("2019-10-07", "1599677226", None, None, None, None, None, None),
                ("2019-10-07", "7524080697", "Continental", 634, [], None),
                ("2019-10-14", "1599677226", None, 553, "['abc','abc','abc']", None),
                (
                    "2019-10-14",
                    "2036794456",
                    "RX",
                    256,
                    ["abcedsadsds"],
                    "2019-12-22",
                    None,
                ),
                (
                    "2019-10-14",
                    "7524080697",
                    "XK Series",
                    158,
                    ["abc", "abc", "abc", "abc"],
                    True,
                ),
                (
                    "2019-10-14",
                    "2036794455",
                    "Grand Marquis",
                    -434,
                    ["abc", "abc", "abc", "abc", "abc"],
                    False,
                ),
                ("2019-10-21", "2036794456", "GT500", 447, [], None),
                (
                    "2019-10-21",
                    "2036794455",
                    "62",
                    759,
                    ["abc", "abc", "abc", "abc", "abc"],
                    True,
                ),
            ]
        )

        df_new = spark_session.createDataFrame(
            df_new.map(
                lambda x: Row(
                    weekstart=x[0],
                    msisdn=x[1],
                    feature_a=x[2],
                    feature_b=x[3],
                    feature_c=x[4],
                    feature_d=x[5],
                )
            )
        )

        df_old = sc.parallelize(
            [
                (
                    "2019-10-01",
                    "1599677226",
                    "Astor",
                    530,
                    "['abc','abc','abc']",
                    False,
                ),
                ("2019-10-01", "2036794456", "Sienna", 934, [], True),
                ("2019-10-01", "7524080697", "Tribute", 200, [], False),
                ("2019-10-07", "1599677226", None, None, None, None, None, None),
                ("2019-10-07", "7524080697", "Continental", 634, None, None),
                ("2019-10-14", "1599677226", None, 553, "['abc','abc','abc']", None),
                ("2019-10-14", "2036794456", "RX", 256, ["abc"], "2019-12-22", None),
                (
                    "2019-10-14",
                    "7524080697",
                    "XK Series",
                    158,
                    ["abc", "abc", "abc", "abc"],
                    True,
                ),
                (
                    "2019-10-14",
                    "2036794455",
                    "Grand Marquis",
                    438,
                    ["abc", "abc", "abc", "abc", "abc"],
                    False,
                ),
                ("2019-10-21", "2036794456", "GT500", 447, [], None),
                (
                    "2019-10-21",
                    "2036794455",
                    "62",
                    759,
                    ["abc", "abc", "abc", "abc", "abc"],
                    None,
                ),
            ]
        )

        df_old = spark_session.createDataFrame(
            df_old.map(
                lambda x: Row(
                    weekstart=x[0],
                    msisdn=x[1],
                    feature_a=x[2],
                    feature_b=x[3],
                    feature_c=x[4],
                    feature_d=x[5],
                )
            )
        )

        actual_same_percentage_df = get_consistency(df_new, df_old)

        actual_same_percentage_list = [
            [row.weekstart, row.columns, row.same_percent,]
            for row in actual_same_percentage_df.collect()
        ]

        expected_same_percentage_list = [
            ["2019-10-01", "feature_a", 0.6666666666666666],
            ["2019-10-01", "feature_b", 1.0],
            ["2019-10-01", "feature_c", 1.0],
            ["2019-10-01", "feature_d", 1.0],
            ["2019-10-07", "feature_a", 1.0],
            ["2019-10-07", "feature_b", 1.0],
            ["2019-10-07", "feature_c", 0.5],
            ["2019-10-07", "feature_d", 1.0],
            ["2019-10-14", "feature_a", 1.0],
            ["2019-10-14", "feature_b", 0.75],
            ["2019-10-14", "feature_c", 0.75],
            ["2019-10-14", "feature_d", 1.0],
            ["2019-10-21", "feature_a", 1.0],
            ["2019-10-21", "feature_b", 1.0],
            ["2019-10-21", "feature_c", 1.0],
            ["2019-10-21", "feature_d", 0.5],
        ]

        assert sorted(actual_same_percentage_df.columns) == sorted(
            ["weekstart", "columns", "same_percent",]
        )

        assert sorted(actual_same_percentage_list) == sorted(
            expected_same_percentage_list
        )
