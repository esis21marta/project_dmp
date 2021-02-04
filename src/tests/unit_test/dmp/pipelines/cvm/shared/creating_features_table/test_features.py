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


from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from src.dmp.pipelines.cvm.shared.creating_features_table.features import (
    get_mondays_multidim,
    prepare_multidim,
    union_tables,
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

expected_multidim_list = [
    ["2019-06-03", "2019-06-03", 2345678, 10001, 10002, 1],
    ["2019-06-03", "2019-06-03", 2345678, 10001, 10002, 1],
    ["2019-06-03", "2019-06-03", 2345678, 10001, 10002, 1],
    ["2019-06-03", "2019-06-03", 2345678, 10001, 10002, 1],
    ["2020-01-05", "2020-01-05", 2345678, 10001, 10002, 1],
    ["2020-01-06", "2020-01-06", 2345678, 10001, 10002, 1],
    ["2020-01-07", "2020-01-07", 2345678, 10001, 10002, 1],
    ["2020-01-08", "2020-01-08", 2345678, 10001, 10002, 1],
    ["2020-01-09", "2020-01-09", 2345678, 10001, 10002, 1],
    ["2020-01-10", "2020-01-10", 2345678, 10001, 10002, 1],
    ["2020-01-11", "2020-01-11", 2345678, 10001, 10002, 1],
    ["2020-01-12", "2020-01-12", 2345678, 10001, 10002, 1],
    ["2020-01-13", "2020-01-13", 2345678, 10001, 10002, 1],
    ["2020-01-14", "2020-01-14", 2345678, 10001, 10002, 1],
    ["2019-06-10", "2019-06-10", 1234567, 10001, 10002, 1],
    ["2019-06-10", "2019-06-10", 1234567, 10001, 10002, 1],
    ["2019-06-10", "2019-06-10", 1234567, 10001, 10002, 1],
    ["2019-06-10", "2019-06-10", 1234567, 10001, 10002, 1],
]

expected_mck_int_features_multidim_mondays_list = [
    ["2019-06-03", "2019-06-03", 2345678, 10001, 10002, 1],
    ["2019-06-03", "2019-06-03", 2345678, 10001, 10002, 1],
    ["2019-06-03", "2019-06-03", 2345678, 10001, 10002, 1],
    ["2019-06-03", "2019-06-03", 2345678, 10001, 10002, 1],
    ["2019-06-10", "2019-06-10", 1234567, 10001, 10002, 1],
    ["2019-06-10", "2019-06-10", 1234567, 10001, 10002, 1],
    ["2019-06-10", "2019-06-10", 1234567, 10001, 10002, 1],
    ["2019-06-10", "2019-06-10", 1234567, 10001, 10002, 1],
]


expected_mck_int_features_multidim_mondays_filtered_list = [
    [2345678, "2019-06-03", 10001, 10002, "2019-06-03", "201906"],
    [2345678, "2019-06-03", 10001, 10002, "2019-06-03", "201906"],
    [2345678, "2019-06-03", 10001, 10002, "2019-06-03", "201906"],
    [2345678, "2019-06-03", 10001, 10002, "2019-06-03", "201906"],
    [1234567, "2019-06-10", 10001, 10002, "2019-06-10", "201906"],
    [1234567, "2019-06-10", 10001, 10002, "2019-06-10", "201906"],
    [1234567, "2019-06-10", 10001, 10002, "2019-06-10", "201906"],
    [1234567, "2019-06-10", 10001, 10002, "2019-06-10", "201906"],
]


expected_table_to_union1_list = [
    ["2020-01-01", "2020-01-01", 2345678, 10001, 10002, 1],
    ["2020-01-02", "2020-01-01", 2345678, 10001, 10002, 1],
    ["2020-01-03", "2020-01-01", 2345678, 10001, 10002, 1],
    ["2020-01-04", "2020-01-01", 2345678, 10001, 10002, 1],
    ["2020-01-05", "2020-01-01", 2345678, 10001, 10002, 1],
    ["2020-01-07", "2020-01-01", 2345678, 10001, 10002, 1],
    ["2020-01-08", "2020-01-01", 2345678, 10001, 10002, 1],
    ["2020-01-09", "2020-01-01", 2345678, 10001, 10002, 1],
]

expected_table_to_union2_list = [
    ["2020-02-01", "2020-01-01", 2345678, 10001, 10002, 1],
    ["2020-02-02", "2020-01-01", 2345678, 10001, 10002, 1],
    ["2020-02-03", "2020-01-01", 2345678, 10001, 10002, 1],
    ["2020-02-04", "2020-01-01", 2345678, 10001, 10002, 1],
    ["2020-02-05", "2020-01-01", 2345678, 10001, 10002, 1],
    ["2020-02-07", "2020-01-01", 2345678, 10001, 10002, 1],
    ["2020-02-08", "2020-01-01", 2345678, 10001, 10002, 1],
    ["2020-02-09", "2020-01-01", 2345678, 10001, 10002, 1],
]

expected_table_unioned_list = [
    [2345678, 10001, 10002, "202002"],
    [2345678, 10001, 10002, "202002"],
    [2345678, 10001, 10002, "202002"],
    [2345678, 10001, 10002, "202002"],
    [2345678, 10001, 10002, "202002"],
    [2345678, 10001, 10002, "202002"],
    [2345678, 10001, 10002, "202002"],
    [2345678, 10001, 10002, "202002"],
    [2345678, 10001, 10002, "202003"],
    [2345678, 10001, 10002, "202003"],
    [2345678, 10001, 10002, "202003"],
    [2345678, 10001, 10002, "202003"],
    [2345678, 10001, 10002, "202003"],
    [2345678, 10001, 10002, "202003"],
    [2345678, 10001, 10002, "202003"],
    [2345678, 10001, 10002, "202003"],
]


expected_multidim = spark.createDataFrame(
    expected_multidim_list, ["trx_date", "event_date", "msisdn", "a", "b", "c"]
)

expected_mck_int_features_multidim_mondays = spark.createDataFrame(
    expected_mck_int_features_multidim_mondays_list,
    ["trx_date", "event_date", "msisdn", "a", "b", "c"],
)

expected_mck_int_features_multidim_mondays_filtered = spark.createDataFrame(
    expected_mck_int_features_multidim_mondays_filtered_list,
    ["msisdn", "trx_date", "a", "b", "event_date", "yearmonth"],
)

expected_params_prepare_multidim = {"cols_to_select": ["a", "b"]}


expected_table_to_union1 = spark.createDataFrame(
    expected_table_to_union1_list, ["trx_date", "event_date", "msisdn", "a", "b", "c"]
)

expected_table_to_union2 = spark.createDataFrame(
    expected_table_to_union2_list, ["trx_date", "event_date", "msisdn", "a", "b", "c"]
)

expected_table_unioned = spark.createDataFrame(
    expected_table_unioned_list, ["msisdn", "a", "b", "yearmonth"]
)

expected_params_union_tables = {
    "table_names": ["table_202001", "table_202002"],
    "cols_to_select": ["a", "b"],
}


def test_get_mondays_multidim():
    res = get_mondays_multidim(expected_multidim, if_repartition=False)
    assert res.columns == expected_mck_int_features_multidim_mondays.columns
    obtained = [list(el) for el in res.collect()]
    assert expected_mck_int_features_multidim_mondays_list == obtained


def test_prepare_multidim():
    res = prepare_multidim(
        expected_mck_int_features_multidim_mondays, expected_params_prepare_multidim
    )
    assert res.columns == expected_mck_int_features_multidim_mondays_filtered.columns
    obtained = [list(el) for el in res.collect()]
    assert expected_mck_int_features_multidim_mondays_filtered_list == obtained


def test_union_tables():
    res = union_tables(
        expected_params_union_tables, expected_table_to_union1, expected_table_to_union2
    )
    assert res.columns == expected_table_unioned.columns
    obtained = [list(el) for el in res.collect()]
    assert expected_table_unioned_list == obtained
