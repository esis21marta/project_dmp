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

from src.dmp.pipelines.cvm.shared.creating_campaign_table.campaign_table import (
    aggregate_cms_ts,
    cmp_and_wording,
    deduplicate_cmp_names,
    joining_wordings_stages_and_trackingstream,
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


cmp = [
    ["sample_id_dsyf", "ACCEPTED-1", "DIGITAL", "LANG", "sample_sog", 8],
    ["sample_id_kdym", "ELIGIBLE-4", "SMS", "CONTENT_ID", "sample_scm", 4],
    ["sample_id_ovaf", "REMINDER-9", "DIGITAL", "TYPE", "sample_xxa", 7],
    ["sample_id_mvrj", "ACCEPTED-3", "SMS", "LANG", "sample_jcd", 7],
    ["sample_id_gqsl", "REMINDER-5", "DIGITAL", "LANG", "sample_cvg", 1],
    ["sample_id_bttj", "REMINDER-8", "OTA", "CONTENT_ID", "sample_oqu", 0],
    ["sample_id_zkxz", "REMINDER-9", "SMS", "TYPE", "sample_wvq", 3],
    ["sample_id_sift", "REMINDER-2", "SMS", "CONTENT_ID", "sample_ytn", 9],
    ["sample_id_ogkv", "REMINDER-3", "DIGITAL", "IS_FLASH", "sample_dfd", 6],
    ["sample_id_fetq", "REMINDER-5", "SMS", "LANG", "sample_gop", 0],
    ["sample_id_jprb", "ELIGIBLE-1", "OTA", "IS_FLASH", "sample_bpz", 8],
    ["sample_id_esaf", "FILTERED_FULFILLM...", "SMS", "IS_FLASH", "sample_lot", 2],
    ["sample_id_xxkm", "REMINDER-1", "OTA", "CONTENT_ID", "sample_njo", 5],
    ["sample_id_dgxw", "REMINDER-8", "DIGITAL", "LANG", "sample_uza", 2],
    ["sample_id_xdjv", "ELIGIBLE-4", "SMS", "IS_FLASH", "sample_uqp", 6],
    ["sample_id_egck", "REMINDER-5", "SMS", "LANG", "sample_uda", 5],
    ["sample_id_xkcp", "REMINDER-6", "SMS", "IS_FLASH", "sample_heq", 5],
    ["sample_id_rcpe", "FILTERED_ACCEPTAN...", "SMS", "IS_FLASH", "sample_zgl", 2],
    ["sample_id_pobu", "REMINDER-10", "OTA", "TYPE", "sample_jzq", 1],
    ["sample_id_bxfl", "REMINDER-11", "SMS", "IS_FLASH", "sample_ero", 3],
]

di = [
    ["sample_id_dsyf", "live", "najopiuged", "8"],
    ["sample_id_kdym", "live", "hselufkhnj", "4"],
    ["sample_id_ovaf", "fut", "kavexrigfw", "7"],
    ["sample_id_mvrj", "live", "mhalnvxnlj", "7"],
    ["sample_id_gqsl", "live", "hgtddbcrsl", "1"],
    ["sample_id_bttj", "live", "bqorowcsei", "0"],
    ["sample_id_zkxz", "fut", "ugfxzlhhbi", "3"],
    ["sample_id_sift", "live", "nwgqywfebc", "9"],
    ["sample_id_ogkv", "retired", "nuwlagzixj", "6"],
    ["sample_id_fetq", "live", "frwwoysqfi", "0"],
    ["sample_id_jprb", "fut", "rslremzyfe", "8"],
    ["sample_id_esaf", "live", "pfuzwtuzxc", "2"],
    ["sample_id_xxkm", "retired", "mqsnghiyeu", "5"],
    ["sample_id_dgxw", "retired", "hvezztiezv", "2"],
    ["sample_id_xdjv", "fut", "ncljwzdmmd", "6"],
    ["sample_id_egck", "live", "pvjuqrclcz", "5"],
    ["sample_id_xkcp", "retired", "jtdmpsswvf", "5"],
    ["sample_id_rcpe", "live", "ocwtwidadw", "2"],
    ["sample_id_pobu", "fut", "rdrrejgsri", "1"],
    ["sample_id_bxfl", "fut", "xodayfcsez", "3"],
]

track = [
    ["sample_id_dsyf", "REG33", "INFORMATION", "LOYALTY", "BATCH"],
    [
        "sample_id_kdym",
        "REG40",
        "INFORMATION",
        "REDUCE NUMBER OF INACTIVE SUBS / CHURN",
        "BATCH",
    ],
    ["sample_id_ovaf", "DLS", "INFORMATION", "INFORMATION", "REAL-TIME CONTEXTUAL"],
    ["sample_id_mvrj", "REG10", "LOYALTY", "LOYALTY", "REAL-TIME CONTEXTUAL"],
    ["sample_id_gqsl", "REG20", "LOYALTY", "DRIVE VOICE PACKAGE PENETRATION", "BATCH"],
    [
        "sample_id_bttj",
        "REG32",
        "RECHARGE",
        "INCREASE HOLDING OF DIGITAL PRODUCTS",
        "BATCH",
    ],
    ["sample_id_zkxz", "REG20", "RECHARGE", "STIMULATION", "REAL-TIME CONTEXTUAL"],
    [
        "sample_id_s",
        "REG31",
        "INFORMATION",
        "INCREASE PENETRATION OF DATA IN LEGACY BASE",
        "REAL-TIME CONTEXTUAL",
    ],
    [
        "sample_id_ogkv",
        "REG33",
        "VOICESMS",
        "INCREASE HOLDING OF DIGITAL PRODUCTS",
        "BATCH",
    ],
    [
        "sample_id_fetq",
        "REG42",
        "VOICESMS",
        "DRIVE UPTAKE OF COMBO PACKS",
        "REAL-TIME CONTEXTUAL",
    ],
    [
        "sample_id_jprb",
        "REG41",
        "RECHARGE",
        "REDUCE NUMBER OF INACTIVE SUBS / CHURN",
        "BATCH",
    ],
    ["sample_id_esaf", "REG23", "DIGITAL", "LOYALTY", "REAL-TIME CONTEXTUAL"],
    ["sample_id_xxkm", "REG24", "LOYALTY", "STIMULATION", "BATCH"],
    [
        "sample_id_dgxw",
        "REG10",
        "BROADBAND",
        "DRIVE VOICE PACKAGE PENETRATION",
        "BATCH",
    ],
    ["sample_id_xdjv", "REG21", "INFORMATION", "STIMULATION", "BROADCAST-ONLY"],
    ["sample_id_egck", "REG43", "RECHARGE", "INFORMATION", "BATCH"],
    ["sample_id_xkcp", "REG24", "RECHARGE", "STIMULATION", "BATCH"],
    [
        "sample_id_rcpe",
        "REG31",
        "LOYALTY",
        "UPSELL DATA PACKAGES TO CURRENT DATA BASE",
        "BATCH",
    ],
    [
        "sample_id_pobu",
        "REG22",
        "INFORMATION",
        "INCREASE HOLDING OF DIGITAL PRODUCTS",
        "BROADCAST-ONLY",
    ],
    [
        "sample_id_bxfl",
        "REG40",
        "VOICESMS",
        "INCREASE PENETRATION OF DATA IN LEGACY BASE",
        "BROADCAST-ONLY",
    ],
]

cmp_df = spark.createDataFrame(
    cmp, ["campid", "type", "channel", "name", "wording", "file_date"]
)
di_df = spark.createDataFrame(di, ["campid", "stage", "name", "file_date"])
track_df = spark.createDataFrame(
    track,
    ["campaignid", "prop_owner", "prop_productgroup", "prop_objective", "prop_type"],
)


def test_joining_wordings_stages_and_trackingstream():
    wrd = cmp_and_wording(cmp_df)
    di_ = deduplicate_cmp_names(di_df)
    filtered_trac = aggregate_cms_ts(track_df)

    joined = joining_wordings_stages_and_trackingstream(filtered_trac, wrd, di_)

    obtained = sorted([list(el) for el in joined.collect()])
    expected = [
        [
            "sample_id_bttj",
            "live",
            "bqorowcsei",
            None,
            "REG32",
            "RECHARGE",
            "INCREASE HOLDING OF DIGITAL PRODUCTS",
            "BATCH",
        ],
        [
            "sample_id_bxfl",
            "fut",
            "xodayfcsez",
            None,
            "REG40",
            "VOICESMS",
            "INCREASE PENETRATION OF DATA IN LEGACY BASE",
            "BROADCAST-ONLY",
        ],
        [
            "sample_id_dgxw",
            "retired",
            "hvezztiezv",
            None,
            "REG10",
            "BROADBAND",
            "DRIVE VOICE PACKAGE PENETRATION",
            "BATCH",
        ],
        [
            "sample_id_dsyf",
            "live",
            "najopiuged",
            "sample_sog",
            "REG33",
            "INFORMATION",
            "LOYALTY",
            "BATCH",
        ],
        [
            "sample_id_egck",
            "live",
            "pvjuqrclcz",
            None,
            "REG43",
            "RECHARGE",
            "INFORMATION",
            "BATCH",
        ],
        [
            "sample_id_esaf",
            "live",
            "pfuzwtuzxc",
            None,
            "REG23",
            "DIGITAL",
            "LOYALTY",
            "REAL-TIME CONTEXTUAL",
        ],
        [
            "sample_id_fetq",
            "live",
            "frwwoysqfi",
            None,
            "REG42",
            "VOICESMS",
            "DRIVE UPTAKE OF COMBO PACKS",
            "REAL-TIME CONTEXTUAL",
        ],
        [
            "sample_id_gqsl",
            "live",
            "hgtddbcrsl",
            None,
            "REG20",
            "LOYALTY",
            "DRIVE VOICE PACKAGE PENETRATION",
            "BATCH",
        ],
        [
            "sample_id_jprb",
            "fut",
            "rslremzyfe",
            "sample_bpz",
            "REG41",
            "RECHARGE",
            "REDUCE NUMBER OF INACTIVE SUBS / CHURN",
            "BATCH",
        ],
        [
            "sample_id_kdym",
            "live",
            "hselufkhnj",
            None,
            "REG40",
            "INFORMATION",
            "REDUCE NUMBER OF INACTIVE SUBS / CHURN",
            "BATCH",
        ],
        [
            "sample_id_mvrj",
            "live",
            "mhalnvxnlj",
            None,
            "REG10",
            "LOYALTY",
            "LOYALTY",
            "REAL-TIME CONTEXTUAL",
        ],
        [
            "sample_id_ogkv",
            "retired",
            "nuwlagzixj",
            None,
            "REG33",
            "VOICESMS",
            "INCREASE HOLDING OF DIGITAL PRODUCTS",
            "BATCH",
        ],
        [
            "sample_id_ovaf",
            "fut",
            "kavexrigfw",
            None,
            "DLS",
            "INFORMATION",
            "INFORMATION",
            "REAL-TIME CONTEXTUAL",
        ],
        [
            "sample_id_pobu",
            "fut",
            "rdrrejgsri",
            None,
            "REG22",
            "INFORMATION",
            "INCREASE HOLDING OF DIGITAL PRODUCTS",
            "BROADCAST-ONLY",
        ],
        [
            "sample_id_rcpe",
            "live",
            "ocwtwidadw",
            None,
            "REG31",
            "LOYALTY",
            "UPSELL DATA PACKAGES TO CURRENT DATA BASE",
            "BATCH",
        ],
        [
            "sample_id_s",
            None,
            None,
            None,
            "REG31",
            "INFORMATION",
            "INCREASE PENETRATION OF DATA IN LEGACY BASE",
            "REAL-TIME CONTEXTUAL",
        ],
        [
            "sample_id_xdjv",
            "fut",
            "ncljwzdmmd",
            None,
            "REG21",
            "INFORMATION",
            "STIMULATION",
            "BROADCAST-ONLY",
        ],
        [
            "sample_id_xkcp",
            "retired",
            "jtdmpsswvf",
            None,
            "REG24",
            "RECHARGE",
            "STIMULATION",
            "BATCH",
        ],
        [
            "sample_id_xxkm",
            "retired",
            "mqsnghiyeu",
            "sample_njo",
            "REG24",
            "LOYALTY",
            "STIMULATION",
            "BATCH",
        ],
        [
            "sample_id_zkxz",
            "fut",
            "ugfxzlhhbi",
            None,
            "REG20",
            "RECHARGE",
            "STIMULATION",
            "REAL-TIME CONTEXTUAL",
        ],
    ]

    assert joined.columns == [
        "campaignid",
        "stage",
        "name",
        "wording",
        "prop_owner",
        "prop_productgroup",
        "prop_objective",
        "prop_type",
    ]
    assert obtained == expected


if __name__ == "__main__":
    test_joining_wordings_stages_and_trackingstream()
