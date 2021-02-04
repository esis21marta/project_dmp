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

from src.dmp.pipelines.cvm.shared.creating_master_table.master_table import (  # prepare_campaigns_table,
    prepare_ds_features_table,
    prepare_target_var_table,
    removing_duplicats,
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

expected_mck_int_mck_features_ds_list = [
    [2345678, "2020-01-01", 10001, 10002, "202001"],
    [2345678, "2020-01-01", 10001, 10002, "202001"],
    [2345678, "2020-01-01", 10001, 10002, "202001"],
    [2345678, "2020-01-01", 10001, 10002, "202001"],
    [2345678, "2020-01-01", 10001, 10002, "202001"],
    [2345678, "2020-01-01", 10001, 10002, "202001"],
    [2345678, "2020-01-01", 10001, 10002, "202001"],
    [2345678, "2020-01-01", 10001, 10002, "202001"],
    [2345678, "2020-01-01", 10001, 10002, "202001"],
    [2345678, "2020-01-08", 10001, 10002, "202001"],
    [2345678, "2020-01-08", 10001, 10002, "202001"],
    [2345678, "2020-01-08", 10001, 10002, "202001"],
    [2345678, "2020-01-08", 10001, 10002, "202001"],
    [2345678, "2020-01-08", 10001, 10002, "202001"],
    [1234567, "2020-01-08", 10001, 10002, "202001"],
    [1234567, "2020-01-08", 10001, 10002, "202001"],
    [1234567, "2020-01-08", 10001, 10002, "202001"],
    [1234567, "2020-01-08", 10001, 10002, "202001"],
]

expected_mck_int_mck_features_ds_processed_list = [
    [2345678, "2020-01-08", 10001, 10002],
    [2345678, "2020-01-08", 10001, 10002],
    [2345678, "2020-01-08", 10001, 10002],
    [2345678, "2020-01-08", 10001, 10002],
    [2345678, "2020-01-08", 10001, 10002],
    [2345678, "2020-01-08", 10001, 10002],
    [2345678, "2020-01-08", 10001, 10002],
    [2345678, "2020-01-08", 10001, 10002],
    [2345678, "2020-01-08", 10001, 10002],
    [2345678, "2020-01-15", 10001, 10002],
    [2345678, "2020-01-15", 10001, 10002],
    [2345678, "2020-01-15", 10001, 10002],
    [2345678, "2020-01-15", 10001, 10002],
    [2345678, "2020-01-15", 10001, 10002],
    [1234567, "2020-01-15", 10001, 10002],
    [1234567, "2020-01-15", 10001, 10002],
    [1234567, "2020-01-15", 10001, 10002],
    [1234567, "2020-01-15", 10001, 10002],
]


expected_mck_int_mck_features_ds = spark.createDataFrame(
    expected_mck_int_mck_features_ds_list, ["msisdn", "trx_date", "a", "b", "yearmonth"]
)

expected_mck_int_mck_features_ds_processed = spark.createDataFrame(
    expected_mck_int_mck_features_ds_processed_list, ["msisdn", "week_start", "a", "b"]
)

expected_params_ds_features = {"features": ["a", "b"]}


expected_l3_int_mck_payu_pkg_rev_grid_window_functions_list = [
    [2345678, "2020-01-08", 1, 1, 123],
    [2345678, "2020-01-08", 2, 2, 132],
    [2345678, "2020-01-08", 3, 3, 123],
    [2345678, "2020-01-08", 4, 4, 123],
    [2345678, "2020-01-08", 1, 5, 123],
    [2345678, "2020-01-08", 2, 5, 123],
    [2345678, "2020-01-08", 3, 7, 123],
    [2345678, "2020-01-08", 4, 8, 123],
    [1234567, "2020-01-15", 1, 1, 123],
    [1234567, "2020-01-15", 2, 2, 123],
    [1234567, "2020-01-15", 3, 3, 123],
    [1234567, "2020-01-15", 4, 4, 123],
    [1234567, "2020-01-15", 1, 5, 123],
    [1234567, "2020-01-15", 2, 6, 123],
    [1234567, "2020-01-15", 3, 7, 123],
    [1234567, "2020-01-15", 4, 8, 123],
]

expected_target_to_master_list = [
    [2345678, "2020-01-08", 4, 4, 123],
    [1234567, "2020-01-15", 4, 4, 123],
]


expected_l3_int_mck_payu_pkg_rev_grid_window_functions = spark.createDataFrame(
    expected_l3_int_mck_payu_pkg_rev_grid_window_functions_list,
    [
        "msisdn",
        "trx_date",
        "rev_window_pre_21_cnt",
        "rev_window_post_8_28_cnt",
        "yearmonth",
    ],
)

expected_target_to_master = spark.createDataFrame(
    expected_target_to_master_list,
    [
        "msisdn",
        "trx_date",
        "rev_window_pre_21_cnt",
        "rev_window_post_8_28_cnt",
        "yearmonth",
    ],
)


expected_campaign_aggregated_table_list = [
    ["HQ", 1],
    ["REG10", 2],
    ["DLS", 3],
    ["DLS", 5],
    ["REG11", 1],
    ["DLSA", 2],
]

expected_t_stg_cms_trackingstream_list = [
    ["ELIGIBLE", 1, 123, "20200122123"],
    ["ELIGIBLE-1", 2, 123, "20200120132"],
    ["ELIGIBLE", 4, 123, "20200108123"],
    ["ELIGIBLE-1", 5, 123, "20200126123"],
    ["DLS", 3, 123, "20200108123"],
    ["ELIGIBLEEC", 1, 123, "20200122123"],
    ["123", 2, 123, "20200108123"],
]

expected_cmps_to_master_list = [
    [1, 123, "ELIGIBLE", "20200122123", "HQ", "2020-01-13"],
    [2, 123, "ELIGIBLE-1", "20200120132", "REG10", "2020-01-13"],
    [5, 123, "ELIGIBLE-1", "20200126123", "DLS", "2020-01-13"],
]

expected_campaign_aggregated_table = spark.createDataFrame(
    expected_campaign_aggregated_table_list, ["prop_owner", "campaignid"]
)

expected_t_stg_cms_trackingstream = spark.createDataFrame(
    expected_t_stg_cms_trackingstream_list,
    ["streamtype", "campaignid", "identifier", "producedon"],
)

expected_cmps_to_master = spark.createDataFrame(
    expected_cmps_to_master_list,
    ["campaignid", "msisdn", "streamtype", "producedon", "prop_owner", "week_start"],
)

expected_quickwins_master_initial_list = [
    ["123", 1, "a"],
    ["123", 1, "b"],
    ["123", 1, "c"],
    ["123", 2, "a"],
    ["123", 2, "b"],
    ["124", 1, "a"],
    ["124", 1, "b"],
]

expected_quickwins_master_list = [
    ["123", 1, "a", 1],
    ["123", 2, "a", 1],
    ["124", 1, "a", 1],
]

expected_quickwins_master_initial = spark.createDataFrame(
    expected_quickwins_master_initial_list, ["msisdn", "week_start", "a"]
)

expected_quickwins_master = spark.createDataFrame(
    expected_quickwins_master_list, ["msisdn", "week_start", "a", "row_number"]
)


def test_prepare_ds_features_table():
    res = prepare_ds_features_table(
        expected_mck_int_mck_features_ds, expected_params_ds_features
    )
    assert res.columns == expected_mck_int_mck_features_ds_processed.columns
    obtained = [list(el) for el in res.collect()]
    assert expected_mck_int_mck_features_ds_processed_list == obtained


def test_prepare_target_var_table():
    res = prepare_target_var_table(
        expected_l3_int_mck_payu_pkg_rev_grid_window_functions
    )
    assert res.columns == expected_target_to_master.columns
    obtained = [list(el) for el in res.collect()]
    assert expected_target_to_master_list == obtained


# def test_prepare_campaigns_table():
#    res = prepare_campaigns_table(
#        expected_campaign_aggregated_table,
#        expected_t_stg_cms_trackingstream,
#        {
#            "campaigns": {
#                "ts": ["streamtype", "campaignid"],
#                "cmp": ["prop_owner", "campaignid"],
#            }
#        },
#    )
#    assert res.columns == expected_cmps_to_master.columns
#    obtained = sorted([list(el) for el in res.collect()])
#    assert expected_cmps_to_master_list == obtained


def test_removing_duplicats():
    res = removing_duplicats(expected_quickwins_master_initial)
    assert res.columns == expected_quickwins_master.columns
    obtained = sorted([list(el) for el in res.collect()])
    assert expected_quickwins_master_list == obtained
