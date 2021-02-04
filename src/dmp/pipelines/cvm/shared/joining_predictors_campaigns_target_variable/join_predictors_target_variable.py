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

from pyspark.sql import functions as f
from pyspark.sql.functions import to_date

# TODO: finish, make sure it works, add a pipeline, add testcases


def creating_enriched_revenue_table(
    int_mck_payu_pkg_rev_window,
    int_mck_features_with_recharge,
    t_stg_cms_trackingstream,
    int_mck_campaigns,
):
    """
    we join:
        weekdays and msisdns
        campaign_info
        Window functions for spending (target variable)
        Some Predictors,

    :return:
    """
    # TODO: test function and refactor into othe nodes

    # TODO this should be done in the previous step instead
    reve = int_mck_payu_pkg_rev_window
    reve = reve.filter(
        (reve.rev_window_pre_21_cnt == 4) & (reve.rev_window_post_8_28_cnt == 4)
    )

    # selecting features:
    selected_feat = [
        "lte_usim_user_flag",
        "region_sales",
        "bill_responsibility_type",
        "segment_data_user",
        "los",
        "active_pack_data_flag",
        "active_pack_data",
        "brand",
        "price_plan",
        "cust_type_desc",
        "cust_subtype_desc",
        "segment_hvc_mtd",
        "segment_hvc_m1",
        "loyalty_tier",
        "nik_gender",
        "nik_age",
        "myTelkomsel_user",
        "first_rank_category",
        "second_rank_category",
        "third_rank_category",
        "first_rank_apps",
        "second_rank_apps",
        "third_rank_apps",
        "data_user_category",
        "manufacture",
        "device_type",
        "data_capable",
        "os",
        "multisim",
        "sdn",
        "device4g",
        "lteusim_prev",
        "sdn_prev",
        "usim_prev",
        "lte_prev",
        "lapsed",
        "tot_amt",
        "tot_trx",
        "tot_amt_01m",
        "tot_amt_07d",
        "tot_trx_01m",
        "tot_trx_07d",
        "avg_topup_01m",
        "avg_topup_07d",
    ]
    int_mck_features_with_recharge = int_mck_features_with_recharge.select(
        "msisdn",
        to_date("week_start").alias("week_start"),  # TODO should I add 1 week lag?
        selected_feat,
    )

    # campaign info
    campaign_cols = [
        "campaignid",
        "stage",
        "name",
        "wording",
        "prop_owner",
        "prop_productgroup",
        "prop_objective",
        "prop_type",
    ]
    # TODO this or (c=='HQ') or (c=='REG10') or...
    campaigns = int_mck_campaigns.select(campaign_cols).filter(
        int_mck_campaigns.prop_owner in ("HQ", "REG10", "DLS")
    )
    ts = t_stg_cms_trackingstream.filter(
        t_stg_cms_trackingstream.streamtype in ("ELIGIBLE", "ELIGIBLE-1")
    )
    tsc = ts.join(campaigns, ["campaignid"]).select(
        f.col("identifier").alias("msisdn"),
        f.date_sub(f.next_day(f.to_date("producedon", "Mon"), 7)).alias(
            "week_start"
        ),  # todo check if correct
        "campaignid",
        "iscontrolgroup",
        "stage",
        "name",
        "wording",
        "prop_owner",
        "prop_productgroup",
        "prop_objective",
        "prop_type",
    )

    # joining the tables
    int_mck_devset_enriched_5 = tsc.join(
        reve, ["msisdn", "week_start"], how="left"
    ).join(
        int_mck_features_with_recharge, ["msisdn", "week_start"], how="left"  # ?
    )
    return int_mck_devset_enriched_5


"""
    SELECT
        tsc.msisdn,
        tsc.week_start,
        tsc.campaignid,
        tsc.iscontrolgroup,
        tsc.stage,
        tsc.name,
        tsc.wording,
        tsc.prop_owner,
        tsc.prop_productgroup,
        tsc.prop_objective,
        tsc.prop_type,

        rev_window_pre_21,
        dls_window_pre_21,
        rev_window_pre_21_cnt,
        rev_window_post_8_28,
        dls_window_post_8_28,
        rev_window_post_8_28_cnt,

        rev_window_pre_77,
        dls_window_pre_77,
        rev_window_pre_77_cnt,
        rev_window_post_8_84,
        dls_window_post_8_84,
        rev_window_post_8_84_cnt,

        md.lte_usim_user_flag,
        md....
        md.avg_topup_07d


    FROM (
        SELECT
            ts.identifier as msisdn,
            next_day(date_sub(to_date(left(ts.producedon,8),'yyyyMMdd'),14),'Mon') as week_start,
            --14 days to get last monday + one week lag
            ts.campaignid,
            ts.iscontrolgroup,

            cmp.stage,
            cmp.name,
            cmp.wording,
            cmp.prop_owner,
            cmp.prop_productgroup,
            cmp.prop_objective,
            cmp.prop_type

        FROM (SELECT * FROM mck.t_stg_cms_trackingstream WHERE streamtype IN ('ELIGIBLE', 'ELIGIBLE-1')) AS ts
        INNER JOIN
            (
            SELECT
                campaignid,
                stage,
                name,
                wording,
                prop_owner,
                prop_productgroup,
                prop_objective,
                prop_type

            FROM mck.int_mck_campaigns
            WHERE
                prop_owner in ('HQ','REG10','DLS')
            ) AS cmp
        ON
            ts.campaignid = cmp.campaignid
        ) as tsc

    LEFT JOIN (
    --INNER JOIN (
        SELECT *
        FROM mck.int_mck_payu_pkg_rev_window_c1
        WHERE
            rev_window_pre_21_cnt = 4 AND
            rev_window_post_8_28_cnt = 4
        ) AS reve
    ON
        tsc.msisdn = reve.msisdn AND
        tsc.week_start = reve.week_start

    LEFT JOIN (
        SELECT
            msisdn,
            date_add(date(trx_date),7) as week_start, --adding one week lag
            ...

        FROM mck.int_mck_features_with_recharge
        ) AS md
    ON
        tsc.msisdn = md.msisdn AND
        tsc.week_start = md.week_start
"""

# df = df.repartition(1000)
# df.write.mode('overwrite').partitionBy('week_start').saveAsTable("mck.int_mck_devset_enriched_5")
