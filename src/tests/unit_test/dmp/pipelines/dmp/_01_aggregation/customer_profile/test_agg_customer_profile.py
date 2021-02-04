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

from src.dmp.pipelines.dmp._01_aggregation.customer_profile.create_weekly_multidim_table import (
    _weekly_aggregation,
)
from src.dmp.pipelines.dmp._02_primary.scaffold.fill_time_series import fill_time_series


class TestCustomerProfile:
    def test_agg_customer_profile_to_weekly(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        cb_multidim_df = sc.parallelize(
            [
                # Case A) Sanity test - Same MSISDN, 3 rows for 1 week - should pick last one
                (
                    "111",
                    "2019-10-01",
                    "lte_usim_user_flag-1",
                    "area_sales-1",
                    "region_sales-1",
                    "bill_responsibility_type-1",
                    "segment_data_user-1",
                    "status-1",
                    "brand-1",
                    "price_plan-1",
                    "cust_type_desc-1",
                    "cust_subtype_desc-1",
                    "segment_hvc_mtd-1",
                    "segment_hvc_m1-1",
                    "loaylty_tier-1",
                    "nik_gender-1",
                    "nik_age-1",
                    "bill_cycle-1",
                    "persona_los-1",
                    "persona_quadrant-1",
                    "arpu_segment_name-1",
                    "mytsel_user_flag-1",
                    "los-1",
                    "2019-10-01",
                    "MARGAHAYU",
                    "BANDU",
                    "20190100739446766",
                    10,
                ),
                (
                    "111",
                    "2019-10-03",
                    "lte_usim_user_flag-2",
                    "area_sales-2",
                    "region_sales-2",
                    "bill_responsibility_type-2",
                    "segment_data_user-2",
                    "status-2",
                    "brand-2",
                    "price_plan-2",
                    "cust_type_desc-2",
                    "cust_subtype_desc-2",
                    "segment_hvc_mtd-2",
                    "segment_hvc_m1-2",
                    "loaylty_tier-2",
                    "nik_gender-2",
                    "nik_age-2",
                    "bill_cycle-2",
                    "persona_los-2",
                    "persona_quadrant-2",
                    "arpu_segment_name-2",
                    "mytsel_user_flag-2",
                    "los-2",
                    "2019-10-03",
                    "MARGAHAYU",
                    "BANDU",
                    "20190100739446766",
                    80,
                ),
                (
                    "111",
                    "2019-10-06",
                    "lte_usim_user_flag-3",
                    "area_sales-3",
                    "region_sales-3",
                    "bill_responsibility_type-3",
                    "segment_data_user-3",
                    "status-3",
                    "brand-3",
                    "price_plan-3",
                    "cust_type_desc-3",
                    "cust_subtype_desc-3",
                    "segment_hvc_mtd-3",
                    "segment_hvc_m1-3",
                    "loaylty_tier-3",
                    "nik_gender-3",
                    "nik_age-3",
                    "bill_cycle-3",
                    "persona_los-3",
                    "persona_quadrant-3",
                    "arpu_segment_name-3",
                    "mytsel_user_flag-3",
                    "los-3",
                    "2019-10-06",
                    "MARGAHAYU",
                    "BANDU",
                    "20190100739446766",
                    20,
                ),
                (
                    "111",
                    "2019-10-06",
                    "xte_usim_user_flag-3",
                    "area_sales-3",
                    "region_sales-3",
                    "bill_responsibility_type-3",
                    "segment_data_user-3",
                    "status-3",
                    "brand-3",
                    "price_plan-3",
                    "cust_type_desc-3",
                    "cust_subtype_desc-3",
                    "segment_hvc_mtd-3",
                    "segment_hvc_m1-3",
                    "loaylty_tier-3",
                    "nik_gender-3",
                    "nik_age-3",
                    "bill_cycle-3",
                    "persona_los-3",
                    "persona_quadrant-3",
                    "arpu_segment_name-3",
                    "mytsel_user_flag-3",
                    "los-3",
                    "2019-10-06",
                    "MARGAHAYU",
                    "BANDU",
                    None,
                    100,
                ),
                # Case B) Edge case - Same MSISDN as Case A, but week with no rows
                # Case C) Edge case - Same MSISDN as Case A, 1 week with 1 row (but many NULLs)
                (
                    "111",
                    "2019-10-20",
                    "lte_usim_user_flag-4",
                    "area_sales-4",
                    "region_sales-4",
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
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    "2019-10-20",
                    "MARGAHAYU KIYO",
                    "BANDU",
                    "20190100739446766",
                    80,
                ),
            ]
        )

        cb_multidim_df = spark_session.createDataFrame(
            cb_multidim_df.map(
                lambda x: Row(
                    msisdn=x[0],
                    trx_date=x[1],
                    event_date=x[23],
                    lte_usim_user_flag=x[2],
                    area_sales=x[3],
                    region_sales=x[4],
                    bill_responsibility_type=x[5],
                    segment_data_user=x[6],
                    status=x[7],
                    brand=x[8],
                    price_plan=x[9],
                    cust_type_desc=x[10],
                    cust_subtype_desc=x[11],
                    segment_hvc_mtd=x[12],
                    segment_hvc_m1=x[13],
                    loyalty_tier=x[14],
                    nik_gender=x[15],
                    nik_age=x[16],
                    bill_cycle=x[17],
                    persona_los=x[18],
                    prsna_quadrant=x[19],
                    arpu_segment_name=x[20],
                    mytsel_user_flag=x[21],
                    los=x[22],
                    kecamatan=x[24],
                    kabupaten=x[25],
                    persona_id=x[26],
                    tsel_poin=x[27],
                )
            )
        )

        weekly_scaffold = sc.parallelize(
            [("111", "2019-10-07"), ("111", "2019-10-14"), ("111", "2019-10-21")]
        )

        weekly_scaffold = spark_session.createDataFrame(
            weekly_scaffold.map(lambda x: Row(msisdn=x[0], weekstart=x[1]))
        )

        out = _weekly_aggregation(cb_multidim_df)

        out_scaffold = fill_time_series("msisdn")(weekly_scaffold, out)
        out_list = [
            [
                i[0],
                i[1],
                i[2],
                i[3],
                i[4],
                i[5],
                i[6],
                i[7],
                i[8],
                i[9],
                i[10],
                i[11],
                i[12],
                i[13],
                i[14],
                i[15],
                i[16],
                i[17],
                i[18],
                i[19],
                i[20],
                i[21],
                i[22],
                i[23],
                i[24],
                i[25],
                i[26],
                i[27],
                i[28],
            ]
            for i in out_scaffold.collect()
        ]

        assert sorted(out_list) == [
            # Case A) Should select Oct 7 as it's closest to weekstart
            [
                "111",
                "2019-10-07",
                "xte_usim_user_flag-3",
                "area_sales-3",
                "region_sales-3",
                "bill_responsibility_type-3",
                "segment_data_user-3",
                "los-3",
                "status-3",
                "brand-3",
                "price_plan-3",
                "cust_type_desc-3",
                "cust_subtype_desc-3",
                "segment_hvc_mtd-3",
                "segment_hvc_m1-3",
                "loaylty_tier-3",
                "nik_gender-3",
                "nik_age-3",
                "bill_cycle-3",
                "persona_los-3",
                "persona_quadrant-3",
                "arpu_segment_name-3",
                "mytsel_user_flag-3",
                "MARGAHAYU",
                "BANDU",
                "20190100739446766",
                52.5,
                10,
                100,
            ],
            # Case B) Should all be null
            [
                "111",
                "2019-10-14",
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
                None,
                None,
                None,
            ],
            # Case C) Should have some data and null for others
            [
                "111",
                "2019-10-21",
                "lte_usim_user_flag-4",
                "area_sales-4",
                "region_sales-4",
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
                None,
                None,
                None,
                None,
                None,
                None,
                "MARGAHAYU KIYO",
                "BANDU",
                "20190100739446766",
                80.0,
                80,
                80,
            ],
        ]
