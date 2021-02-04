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

import pyspark
import pyspark.sql.functions as F
from pyspark.sql.functions import col


def rfm_step_1(
    table_a: pyspark.sql.DataFrame,
    table_b: pyspark.sql.DataFrame,
    cols=None,
    how: str = "left",
) -> pyspark.sql.DataFrame:

    table_a = (
        table_a.select(["msisdn", "device_type", "design_type"])
        .withColumn("design_type", F.lower(F.col("design_type")))
        .withColumn("device_type", F.lower(F.col("device_type")))
        .filter(col("dvc_rank") == 1)
    )
    table_b = (
        table_b.select(["category", "device_type"])
        .withColumn("design_type", F.lower(F.col("device_type")))
        .withColumn("category_device", F.lower(F.col("category")))
        .select(["category_device", "design_type"])
    )

    cols = ["design_type"] if cols is None else cols
    res = table_a.join(table_b, cols, how=how)
    return res


def rfm_step_2(table_a: pyspark.sql.DataFrame,) -> pyspark.sql.DataFrame:

    res = table_a.selectExpr(
        "*",
        "CASE WHEN category_device is NULL THEN device_type ELSE category_device END category_x",
    )
    res = res.selectExpr(
        "*",
        "CASE when category_x in ('high') and device_type in ('smartphone','tablet') then 4\
                        when category_x in ('mid low','mid mid','mid high','smartphone') and device_type in ('smartphone','tablet') then 3\
                        when category_x in ('entry level') and device_type in ('smartphone','tablet') then 2\
                        when category_x is null and device_type = 'smartphone' then 2\
                        when category_x is null and device_type = 'tablet' then 2\
                        when category_x is null and device_type = 'other device' then 1\
                        when category_x is null and device_type = 'modem' then 1\
                        when category_x is null and device_type = 'basicphone' then 1\
                        when category_x is null and device_type = 'unidentified' then 1\
                        when category_x in ('entry level') and device_type ='basicphone' then 1\
                        when category_x in ('entry level') and device_type ='basicphone' then 1\
                        when category_x in ('high') and device_type ='basicphone' then 1\
                        when category_x in ('high') and device_type ='featurephone' then 1\
                        when category_x in ('mid low') and device_type ='featurephone' then 1\
                        when category_x in ('mid mid') and device_type ='featurephone' then 1\
                        when category_x is null and device_type is null then 2\
                        when device_type in ('tablet') and category_device is null then 2 else 1 end as category_new",
    )

    return res


def rfm_step_3(
    table_a: pyspark.sql.DataFrame,
    table_b: pyspark.sql.DataFrame,
    table_c: pyspark.sql.DataFrame,
    cols=None,
    how: str = "left",
) -> pyspark.sql.DataFrame:

    table_a = (
        table_a.select(
            [
                "msisdn",
                "total_revenue",
                "dominan_recharge_channel",
                "total_recharge",
                "trx_recharge",
                "device_type",
                "brand",
            ]
        )
        .withColumn("brand", F.lower(F.col("brand")))
        .filter(col("brand").isin(["simpati", "kartuas", "loop"]))
        .select(
            [
                "msisdn",
                "total_revenue",
                "dominan_recharge_channel",
                "total_recharge",
                "trx_recharge",
                "device_type",
            ]
        )
    )

    table_b = table_b.withColumn("total_revenue_b", F.col("total_revenue")).select(
        ["msisdn", "total_revenue_b"]
    )
    table_c = table_c.withColumn("total_revenue_c", F.col("total_revenue")).select(
        ["msisdn", "total_revenue_c"]
    )

    cols = ["msisdn"] if cols is None else cols
    table_join = table_a.join(table_b, cols, how=how).join(table_c, cols, how=how)

    res = table_join.selectExpr(
        "msisdn",
        "CASE WHEN total_revenue is NULL THEN 0 ELSE total_revenue END arpu_l1m",
        "CASE WHEN total_revenue_b is NULL THEN 0 ELSE total_revenue_b END arpu_l2m",
        "CASE WHEN total_revenue_c is NULL THEN 0 ELSE total_revenue_c END arpu_l3m",
        "dominan_recharge_channel",
        "CASE WHEN lower(dominan_recharge_channel) in ('mytelkomsel','linkaja','e-commerce') then 3\
                                WHEN lower(dominan_recharge_channel) in ('retail nasional','banking') then 2\
                                ELSE 1 END dominan_recharge_channel_group",
        "CASE WHEN total_recharge is NULL THEN 0 ELSE total_recharge END total_recharge",
        "CASE WHEN trx_recharge is NULL THEN 0 ELSE trx_recharge END trx_recharge",
        "device_type",
        "CASE WHEN lower(device_type) = 'smartphone' then 'smartphone'\
                                ELSE 'non_smartphone' END device_type_group",
    )
    return res


def rfm_step_4(table_a: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:

    res = table_a.selectExpr(
        "*",
        "CASE WHEN (arpu_l1m > arpu_l2m) and (arpu_l1m > arpu_l3m) THEN arpu_l1m\
                                WHEN arpu_l2m > arpu_l3m THEN arpu_l2m\
                                ELSE arpu_l3m END arpu_l3m_max",
    )

    return res


def rfm_step_5(
    table_a: pyspark.sql.DataFrame,
    table_b: pyspark.sql.DataFrame,
    cols=None,
    how: str = "left",
) -> pyspark.sql.DataFrame:

    cols = ["msisdn"] if cols is None else cols
    table_join = table_a.join(table_b.select(["msisdn", "category_new"]), cols, how=how)

    res = table_join.selectExpr(
        "msisdn",
        "arpu_l3m_max",
        "dominan_recharge_channel",
        "dominan_recharge_channel_group",
        "total_recharge",
        "trx_recharge",
        "device_type",
        "device_type_group",
        "case when trx_recharge = 0 then 1 else trx_recharge end as trx_recharge_new",
        "case when category_new is null then 2 else category_new end as category_new",
    )
    res = res.selectExpr(
        "*",
        "category_new as device_category",
        "(total_recharge/trx_recharge_new) as recharge_amount",
    )

    return res


def rfm_step_6(table_a: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:

    res = table_a.selectExpr(
        "msisdn",
        "arpu_l3m_max",
        "dominan_recharge_channel as recharge_channel",
        "total_recharge",
        "trx_recharge",
        "device_type",
        "recharge_amount",
        "case when round(arpu_l3m_max) < 16040 then 1\
                            when round(arpu_l3m_max) between 16041 and 68001 then 2\
                            else 3 end as arpu_l3m_score",
        "case when round(trx_recharge) < 1 then 1\
                            when round(trx_recharge) between 1 and 3 then 2\
                            else 3 end as freq_recharge_score",
        "case when round(recharge_amount) < 10000 then 1\
                            when round(recharge_amount) between 10001 and  28333  then 2\
                            else 3 end as recharge_amount_score",
        "dominan_recharge_channel_group as recharge_channel_score",
        "case when device_category = 4 then 3\
                            when device_category is null then 2\
                            else device_category end as device_score",
    )

    res = res.selectExpr(
        "msisdn",
        "arpu_l3m_max",
        "recharge_channel",
        "total_recharge",
        "trx_recharge",
        "device_type",
        "recharge_amount",
        "(arpu_l3m_score+freq_recharge_score+recharge_amount_score+device_score+recharge_channel_score) as total_score",
    )

    return res


def rfm_step_7(table_a: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:

    res = table_a.selectExpr(
        "msisdn",
        "arpu_l3m_max as rev_l3m_max",
        "recharge_channel",
        "total_recharge",
        "trx_recharge as tot_freq_recharge",
        "recharge_amount as avg_recharge_trx",
        "device_type",
        "case when total_score between 5 and 7 then 'low'\
                            when total_score between 8 and 11 then 'medium'\
                            else 'high' end as ses_segmentation",
    )
    return res


def selected_columns(
    rfm_bde_ses_segmentation: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:

    res = rfm_bde_ses_segmentation.select(
        [
            "msisdn",
            "tot_freq_recharge",
            "avg_recharge_trx",
            "rev_l3m_max",
            "ses_segmentation",
        ]
    )

    return res


def join_rfm_dmp(
    rfm_table: pyspark.sql.DataFrame,
    dmp_table: pyspark.sql.DataFrame,
    agg_table: pyspark.sql.DataFrame,
    agg_cols_to_select,
    ses_cols_to_select,
    segment,
    weekstart,
    cols=None,
    how: str = "left",
) -> pyspark.sql.DataFrame:

    agg_table = agg_table.filter(agg_table.weekstart == weekstart).select(
        agg_cols_to_select
    )
    cols = ["msisdn"] if cols is None else cols
    dmp_table = dmp_table.dropDuplicates(cols)
    dmp = dmp_table.join(agg_table, cols, how=how)

    if segment == "low":
        rfm_table = rfm_table.filter(rfm_table.ses_segmentation == segment)
        rfm_table = rfm_table.select(
            ["msisdn", "rev_l3m_max", "avg_recharge_trx", "tot_freq_recharge"]
        )
    elif segment == "medium":
        rfm_table = rfm_table.filter(rfm_table.ses_segmentation == segment)
        rfm_table = rfm_table.select(
            ["msisdn", "rev_l3m_max", "avg_recharge_trx", "tot_freq_recharge"]
        )
    else:
        rfm_table = rfm_table.filter(rfm_table.ses_segmentation == segment)
        rfm_table = rfm_table.select(
            ["msisdn", "rev_l3m_max", "avg_recharge_trx", "tot_freq_recharge"]
        )

    cols = ["msisdn"] if cols is None else cols
    dmp = dmp.select(ses_cols_to_select).dropDuplicates(cols)
    res = rfm_table.join(dmp, cols, how=how)
    res = res.select(
        ses_cols_to_select + ["rev_l3m_max", "avg_recharge_trx", "tot_freq_recharge"]
    )

    return res


def downsampling_master_table(
    master_table_net_segments_: pyspark.sql.DataFrame, fraction: int = 1
) -> pyspark.sql.DataFrame:
    return master_table_net_segments_.sample(False, fraction, 42).sample(
        False, fraction, 42
    )
