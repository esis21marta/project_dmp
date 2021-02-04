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

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._06_qa.metrics.accuracy_completeness import (
    get_accuracy_completeness_metrics,
    get_outlier,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestAccuracyCompleteness:
    def test_accuracy_completeness(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        df_new = sc.parallelize(
            [
                (
                    "2019-10-01",
                    "1599677226",
                    "Astor",
                    530,
                    ["abc", "abc", "abc"],
                    False,
                ),
                ("2019-10-01", "2036794456", "Sie", 934, [], True),
                ("2019-10-01", "7524080697", "Tribute", 200, [], False),
                ("2019-10-07", "1599677226", None, None, None, None, None, None),
                ("2019-10-07", "7524080697", "Continental", 634, [], None),
                ("2019-10-14", "1599677226", None, 553, ["abc", "abc", "abc"], None),
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

        actual_qa_metrics = get_accuracy_completeness_metrics(
            df=df_new,
            percentiles=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.25, 0.75],
            percentiles_accuracy=80,
        )

        actual_qa_metrics_list = [
            [
                row.weekstart,
                row.columns,
                row.min,
                row.max,
                row.null_percentage,
                row.mean,
                row.negative_value_percentage,
                row["row_count"],
                row["percentile_0_1"],
                row["percentile_0_2"],
                row["percentile_0_3"],
                row["percentile_0_4"],
                row["percentile_0_5"],
                row["percentile_0_6"],
                row["percentile_0_7"],
                row["percentile_0_8"],
                row["percentile_0_9"],
                row["percentile_1_0"],
                row["percentile_0_25"],
                row["percentile_0_75"],
            ]
            for row in actual_qa_metrics.collect()
        ]

        expected_qa_metrics_list = [
            [
                "2019-10-01",
                "feature_a",
                None,
                None,
                0.0,
                None,
                None,
                3,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [
                "2019-10-01",
                "feature_b",
                200,
                934,
                0.0,
                554.6667,
                0.0,
                3,
                200,
                200,
                200,
                530,
                530,
                530,
                934,
                934,
                934,
                934,
                200,
                934,
            ],
            [
                "2019-10-01",
                "feature_c",
                0,
                3,
                0.0,
                1.0,
                0.0,
                3,
                0,
                0,
                0,
                0,
                0,
                0,
                3,
                3,
                3,
                3,
                0,
                3,
            ],
            [
                "2019-10-01",
                "feature_d",
                None,
                None,
                0.0,
                None,
                None,
                3,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [
                "2019-10-07",
                "feature_a",
                None,
                None,
                0.5,
                None,
                None,
                2,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [
                "2019-10-07",
                "feature_b",
                634,
                634,
                0.5,
                634.0,
                0.0,
                2,
                634,
                634,
                634,
                634,
                634,
                634,
                634,
                634,
                634,
                634,
                634,
                634,
            ],
            [
                "2019-10-07",
                "feature_c",
                -1,
                0,
                0.0,
                -0.5,
                0.5,
                2,
                -1,
                -1,
                -1,
                -1,
                -1,
                0,
                0,
                0,
                0,
                0,
                -1,
                0,
            ],
            [
                "2019-10-07",
                "feature_d",
                None,
                None,
                1.0,
                None,
                None,
                2,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [
                "2019-10-14",
                "feature_a",
                None,
                None,
                0.25,
                None,
                None,
                4,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [
                "2019-10-14",
                "feature_b",
                -434,
                553,
                0.0,
                133.25,
                0.25,
                4,
                -434,
                -434,
                158,
                158,
                158,
                256,
                256,
                553,
                553,
                553,
                -434,
                256,
            ],
            [
                "2019-10-14",
                "feature_c",
                1,
                5,
                0.0,
                3.25,
                0.0,
                4,
                1,
                1,
                3,
                3,
                3,
                4,
                4,
                5,
                5,
                5,
                1,
                4,
            ],
            [
                "2019-10-14",
                "feature_d",
                None,
                None,
                0.5,
                None,
                None,
                4,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [
                "2019-10-21",
                "feature_a",
                None,
                None,
                0.0,
                None,
                None,
                2,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [
                "2019-10-21",
                "feature_b",
                447,
                759,
                0.0,
                603.0,
                0.0,
                2,
                447,
                447,
                447,
                447,
                447,
                759,
                759,
                759,
                759,
                759,
                447,
                759,
            ],
            [
                "2019-10-21",
                "feature_c",
                0,
                5,
                0.0,
                2.5,
                0.0,
                2,
                0,
                0,
                0,
                0,
                0,
                5,
                5,
                5,
                5,
                5,
                0,
                5,
            ],
            [
                "2019-10-21",
                "feature_d",
                None,
                None,
                0.5,
                None,
                None,
                2,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
        ]

        assert sorted(actual_qa_metrics.columns) == sorted(
            [
                "columns",
                "max",
                "mean",
                "min",
                "negative_value_percentage",
                "null_percentage",
                "percentile_0_1",
                "percentile_0_2",
                "percentile_0_25",
                "percentile_0_3",
                "percentile_0_4",
                "percentile_0_5",
                "percentile_0_6",
                "percentile_0_7",
                "percentile_0_75",
                "percentile_0_8",
                "percentile_0_9",
                "percentile_1_0",
                "row_count",
                # 'same_percent',
                "weekstart",
            ]
        )

        assert sorted(actual_qa_metrics_list) == sorted(expected_qa_metrics_list)

    def test_get_outlier(self, spark_session: SparkSession):

        input_df = spark_session.createDataFrame(
            data=[
                # All in range
                ["123", datetime.date(2020, 6, 1), 10, "nokia", 0.1, ["a", "b"]],
                ["456", datetime.date(2020, 6, 1), 20, "apple", 0.2, ["a", "b", "c"]],
                ["789", datetime.date(2020, 6, 1), 15, None, 0.3, ["a"]],
                # Outlier Lower Bound
                ["123", datetime.date(2020, 6, 8), 2, "samsung", 0.4, []],
                ["456", datetime.date(2020, 6, 8), 3, None, 0.5, None],
                ["789", datetime.date(2020, 6, 8), 15, None, 0.6, ["a"]],
                # Outlier Upper Bound
                ["123", datetime.date(2020, 6, 15), 10, None, 0.7, ["a", "b"]],
                ["456", datetime.date(2020, 6, 15), 20, "oneplus", 0.8, ["a", "b"]],
                ["789", datetime.date(2020, 6, 15), 15, None, 0.9, ["a"]],
                # Only one data point
                ["123", datetime.date(2020, 6, 22), 10, None, 1.0, []],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("feature_a", LongType(), True),
                    StructField("feature_b", StringType(), True),
                    StructField("feature_c", FloatType(), True),
                    StructField("feature_d", ArrayType(StringType(), True), True),
                ]
            ),
        )

        df_metrics = spark_session.createDataFrame(
            data=[
                # All in range
                [datetime.date(2020, 6, 1), "feature_a", 10.0, 20.0],
                [datetime.date(2020, 6, 1), "feature_b", None, None],
                [datetime.date(2020, 6, 1), "feature_c", 0.0, 0.4],
                [datetime.date(2020, 6, 1), "feature_d", 1.0, 3.0],
                # Outlier Lower Bound
                [datetime.date(2020, 6, 8), "feature_a", 12.0, 18.0],
                [datetime.date(2020, 6, 8), "feature_b", None, None],
                [datetime.date(2020, 6, 8), "feature_c", 0.5, 0.7],
                [datetime.date(2020, 6, 8), "feature_d", 1.0, 5.0],
                # Outlier Upper Bound
                [datetime.date(2020, 6, 15), "feature_a", 10.0, 15.0],
                [datetime.date(2020, 6, 15), "feature_b", None, None],
                [datetime.date(2020, 6, 15), "feature_c", 0.7, 0.8],
                [datetime.date(2020, 6, 15), "feature_d", 1.0, 1.0],
                # Percentile 25 and 75 are same
                # Only one data point
                [datetime.date(2020, 6, 22), "feature_a", 10.0, 10.0],
                [datetime.date(2020, 6, 22), "feature_b", None, None],
                [datetime.date(2020, 6, 22), "feature_c", 2.3, 2.3],
                [datetime.date(2020, 6, 22), "feature_d", 0.0, 0.0],
            ],
            schema=StructType(
                [
                    StructField("weekstart", DateType(), False),
                    StructField("columns", StringType(), False),
                    StructField("percentile_0_25", DoubleType(), True),
                    StructField("percentile_0_75", DoubleType(), True),
                ]
            ),
        )

        actual_output = get_outlier(input_df, df_metrics)

        expected_output = spark_session.createDataFrame(
            data=[
                # All in range
                [datetime.date(2020, 6, 1), "feature_a", 0, 0],
                [datetime.date(2020, 6, 1), "feature_c", 0, 0],
                [datetime.date(2020, 6, 1), "feature_d", 0, 0],
                # Outlier Lower Bound
                [datetime.date(2020, 6, 8), "feature_a", 0, 1],
                [datetime.date(2020, 6, 8), "feature_c", 0, 0],
                [datetime.date(2020, 6, 8), "feature_d", 0, 0],
                # Outlier Upper Bound
                [datetime.date(2020, 6, 15), "feature_a", 0, 0],
                [datetime.date(2020, 6, 15), "feature_c", 0, 0],
                [datetime.date(2020, 6, 15), "feature_d", 2, 0],
                # Percentile 25 and 75 are same
                # Only one data point
                [datetime.date(2020, 6, 22), "feature_a", 0, 0],
                [datetime.date(2020, 6, 22), "feature_c", 0, 1],
                [datetime.date(2020, 6, 22), "feature_d", 0, 0],
            ],
            schema=StructType(
                [
                    StructField("weekstart", DateType(), False),
                    StructField("columns", StringType(), False),
                    StructField("count_higher_outlier", LongType(), True),
                    StructField("count_lower_outlier", LongType(), True),
                ]
            ),
        )

        assert_df_frame_equal(actual_output, expected_output)
