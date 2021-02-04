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

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from src.dmp.pipelines.cvm.shared.creating_target_variable.target_variable import (
    create_dls_payu_revenue_table,
    create_total_revenue_table,
    creating_grid,
    creating_window_functions,
    filling_grid_with_revenue_values,
)

conf = (
    SparkConf()
    .setMaster("local")
    .setAppName("dmp-unit-test")
    .set("spark.default.parallelism", "1")
    .set("spark.sql.shuffle.partitions", "1")
    .set("spark.shuffle.service.enabled", "false")
    .set("spark.sql.catalogImplementation", "hive")
)
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)

# test_create_total_revenue_table
expected_total_rev_list = [
    [1234567, datetime.date(2019, 12, 30), 80012],
    [2345678, datetime.date(2019, 12, 30), 100015],
    [2345678, datetime.date(2020, 1, 6), 120018],
    [2345678, datetime.date(2020, 1, 13), 40006],
]
# test_create_dls_payu_revenue_table
expected_dls_rev_list = [
    [1234567, datetime.date(2019, 12, 30), 4],
    [2345678, datetime.date(2019, 12, 30), 5],
    [2345678, datetime.date(2020, 1, 6), 6],
    [2345678, datetime.date(2020, 1, 13), 2],
]
# test_creating_grid
expected_grid_list = [
    [1234567, datetime.date(2019, 12, 30)],
    [1234567, datetime.date(2020, 1, 6)],
    [1234567, datetime.date(2020, 1, 13)],
    [2345678, datetime.date(2019, 12, 30)],
    [2345678, datetime.date(2020, 1, 6)],
    [2345678, datetime.date(2020, 1, 13)],
]
# test_filling_grid_with_revenue_values
expected_filled_out_grid_list = [
    [datetime.date(2019, 12, 30), 1234567, 80012, 4],
    [datetime.date(2019, 12, 30), 2345678, 100015, 5],
    [datetime.date(2020, 1, 6), 1234567, None, None],
    [datetime.date(2020, 1, 6), 2345678, 120018, 6],
    [datetime.date(2020, 1, 13), 1234567, None, None],
    [datetime.date(2020, 1, 13), 2345678, 40006, 2],
]

expected_abt_list = [
    ("2020-01-01", 2345678, 10001, 10002, 1),
    ("2020-01-02", 2345678, 10001, 10002, 1),
    ("2020-01-03", 2345678, 10001, 10002, 1),
    ("2020-01-04", 2345678, 10001, 10002, 1),
    ("2020-01-05", 2345678, 10001, 10002, 1),
    ("2020-01-07", 2345678, 10001, 10002, 1),
    ("2020-01-08", 2345678, 10001, 10002, 1),
    ("2020-01-09", 2345678, 10001, 10002, 1),
    ("2020-01-10", 2345678, 10001, 10002, 1),
    ("2020-01-11", 2345678, 10001, 10002, 1),
    ("2020-01-12", 2345678, 10001, 10002, 1),
    ("2020-01-13", 2345678, 10001, 10002, 1),
    ("2020-01-14", 2345678, 10001, 10002, 1),
    ("2020-01-01", 1234567, 10001, 10002, 1),
    ("2020-01-02", 1234567, 10001, 10002, 1),
    ("2020-01-03", 1234567, 10001, 10002, 1),
    ("2020-01-04", 1234567, 10001, 10002, 1),
]

expected_abt = spark.createDataFrame(
    expected_abt_list, ["trx_date", "msisdn", "rev_total", "rev_pkg_prchse", "rev_dls"]
)
expected_total_rev = spark.createDataFrame(
    expected_total_rev_list, ["msisdn", "week_start", "rev_total"]
)
expected_dls_rev = spark.createDataFrame(
    expected_dls_rev_list, ["msisdn", "week_start", "rev_dls"]
)
expected_grid = spark.createDataFrame(expected_grid_list, ["msisdn", "week_start"])
expected_filled_out_grid = spark.createDataFrame(
    expected_filled_out_grid_list, ["week_start", "msisdn", "rev_total", "rev_dls"]
)


def test_create_total_revenue_table():
    res = create_total_revenue_table(expected_abt, expected_abt)
    assert res.columns == ["msisdn", "week_start", "rev_total"]
    obtained = sorted(list(el) for el in res.collect())
    assert expected_total_rev_list == obtained


def test_create_dls_payu_revenue_table():
    res = create_dls_payu_revenue_table(expected_abt)
    assert res.columns == ["msisdn", "week_start", "rev_dls"]
    obtained = sorted([list(el) for el in res.collect()])
    assert expected_dls_rev_list == obtained


def test_creating_grid():
    out = creating_grid(expected_total_rev)
    assert out.columns == ["msisdn", "week_start"]
    obtained = sorted([list(el) for el in out.collect()])
    assert expected_grid_list == obtained


def test_filling_grid_with_revenue_values():
    out = filling_grid_with_revenue_values(
        expected_total_rev, expected_dls_rev, expected_grid
    )
    assert out.columns == ["week_start", "msisdn", "rev_total", "rev_dls"]
    obtained = sorted([list(el) for el in out.collect()])
    assert expected_filled_out_grid_list == obtained


# def test__window_sum_better_name_will_be_found():
#     out = _window_sum_better_name_will_be_found(expected_filled_out_grid, 7, 13, 'rev_dls', 'sum', 'temp')
#     out.show()
#     pass


def test_creating_window_functions():
    out = creating_window_functions(expected_filled_out_grid)
    res = sorted([list(el) for el in out.collect()])
    assert res == [
        [
            1234567,
            datetime.date(2019, 12, 30),
            80012,
            4,
            1,
            None,
            None,
            0,
            80012,
            4,
            1,
            None,
            None,
            0,
        ],
        [
            1234567,
            datetime.date(2020, 1, 6),
            80012,
            4,
            1,
            None,
            None,
            0,
            80012,
            4,
            1,
            None,
            None,
            0,
        ],
        [
            1234567,
            datetime.date(2020, 1, 13),
            80012,
            4,
            1,
            None,
            None,
            0,
            80012,
            4,
            1,
            None,
            None,
            0,
        ],
        [
            2345678,
            datetime.date(2019, 12, 30),
            100015,
            5,
            1,
            160024,
            8,
            2,
            100015,
            5,
            1,
            160024,
            8,
            2,
        ],
        [
            2345678,
            datetime.date(2020, 1, 6),
            220033,
            11,
            2,
            40006,
            2,
            1,
            220033,
            11,
            2,
            40006,
            2,
            1,
        ],
        [
            2345678,
            datetime.date(2020, 1, 13),
            260039,
            13,
            3,
            None,
            None,
            0,
            260039,
            13,
            3,
            None,
            None,
            0,
        ],
    ]
