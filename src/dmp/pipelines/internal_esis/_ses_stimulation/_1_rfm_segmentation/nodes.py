from typing import List, Tuple

import pyspark
import pyspark.sql.functions as f
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def rfm_step_1(
    table_a: pyspark.sql.DataFrame,
    table_b: pyspark.sql.DataFrame,
    cols=None,
    how: str = "left",
) -> pyspark.sql.DataFrame:

    table_a = (
        table_a.select(["msisdn", "device_type", "design_type", "dvc_rank"])
        .withColumn("design_type", F.lower(F.col("design_type")))
        .withColumn("device_type", F.lower(F.col("device_type")))
        .filter(col("dvc_rank") == 1)
        .drop("dvc_rank")
    )
    table_b = (
        table_b.select(["category", "device_type"])
        .withColumn("design_type", F.lower(F.col("device_type")))
        .withColumn("device_category", F.lower(F.col("category")))
        .select(["device_category", "design_type"])
    )

    cols = ["design_type"] if cols is None else cols
    res = table_a.join(table_b, cols, how=how)
    return res


def rfm_step_2(table_a: pyspark.sql.DataFrame,) -> pyspark.sql.DataFrame:

    res = table_a.selectExpr(
        "*",
        "CASE WHEN device_category is NULL THEN device_type ELSE device_category END device_category_new",
    )
    res = res.selectExpr(
        "*",
        "CASE when device_category_new in ('high') and device_type in ('smartphone','tablet') then 4\
                        when device_category_new in ('mid low','mid mid','mid high','smartphone') and device_type in ('smartphone','tablet') then 3\
                        when device_category_new in ('entry level') and device_type in ('smartphone','tablet') then 2\
                        when device_category_new is null and device_type = 'smartphone' then 2\
                        when device_category_new is null and device_type = 'tablet' then 2\
                        when device_category_new is null and device_type = 'other device' then 1\
                        when device_category_new is null and device_type = 'modem' then 1\
                        when device_category_new is null and device_type = 'basicphone' then 1\
                        when device_category_new is null and device_type = 'unidentified' then 1\
                        when device_category_new in ('entry level') and device_type ='basicphone' then 1\
                        when device_category_new in ('entry level') and device_type ='basicphone' then 1\
                        when device_category_new in ('high') and device_type ='basicphone' then 1\
                        when device_category_new in ('high') and device_type ='featurephone' then 1\
                        when device_category_new in ('mid low') and device_type ='featurephone' then 1\
                        when device_category_new in ('mid mid') and device_type ='featurephone' then 1\
                        when device_category_new is null and device_type is null then 2\
                        when device_type in ('tablet') and device_category is null then 2 else 1 end as device_category_number",
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
    table_join = table_a.join(
        table_b.select(["msisdn", "device_category_number"]), cols, how=how
    )

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
        "case when device_category_number is null then 2 else device_category_number end as device_category_number",
    )
    res = res.selectExpr(
        "*",
        "device_category_number as device_category",
        "(total_recharge/trx_recharge_new) as recharge_amount",
    )

    return res


def rfm_step_6(
    table_a: pyspark.sql.DataFrame, ses_train_score: str
) -> pyspark.sql.DataFrame:

    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)
    table_a.createOrReplaceTempView("bde_abt_rfm_temp")

    if ses_train_score == "score":
        res = spark.sql(
            """select
         x.msisdn
        ,x.dominan_recharge_channel recharge_channel
        ,x.device_type
        ,x.arpu_l3m_max
        ,x.total_recharge
        ,x.trx_recharge
        ,x.recharge_amount
        ,x.device_category
        ,x.arpu_l3m_score
        ,x.freq_recharge_score
        ,x.recharge_amount_score
        ,x.device_score
        ,x.recharge_channel_score
        ,(x.arpu_l3m_score + x.freq_recharge_score + x.recharge_amount_score + x.device_score + x.recharge_channel_score) as total_score
    from
        (
            select
                 msisdn
                ,dominan_recharge_channel
                ,device_type
                ,total_recharge
                ,arpu_l3m_max
                ,trx_recharge
                ,recharge_amount
                ,device_category
                ,case
                    when round(arpu_l3m_max) < 16040 then 1
                    when round(arpu_l3m_max) between 16040 and 68001 then 2
                    else 3
                 end as arpu_l3m_score
                ,case
                    when round(trx_recharge) < 1 then 1
                    when round(trx_recharge) between 1 and 3 then 2
                    else 3
                 end as freq_recharge_score
                ,case
                    when round(recharge_amount) < 10000 then 1
                    when round(recharge_amount) between 10000 and  28333  then 2
                 else 3
                 end as recharge_amount_score
                ,dominan_recharge_channel_group as recharge_channel_score
                ,case
                    when device_category = 4 then 3
                    when device_category is null then 2
                    else device_category
                end as device_score
            from
                bde_abt_rfm_temp
        )x"""
        )

    else:

        res = spark.sql(
            f"""with
        pctx as
        (
            select msisdn,
            arpu_l3m_max, ntile(100)over(order by arpu_l3m_max asc) as pct_arpu,
            trx_recharge, ntile(100)over(order by trx_recharge asc) as pct_recharge_trx,
            recharge_amount, ntile(100)over(order by recharge_amount asc) as pct_recharge_amount
            from bde_abt_rfm_temp
        ),
        limxarpu as
        (
            select
                case when pct_arpu between 1 and 32 then 1
                     when pct_arpu between 33 and 66 then 2
                end as flag,
                sum(arpu_l3m_max)/count(1) as lim
            from pctx
            group by
                case when pct_arpu between 1 and 32 then 1
                     when pct_arpu between 33 and 66 then 2
                end
        ),
        limxrchgtrx as
        (
            select
                case when pct_recharge_trx between 1 and 32 then 1
                     when pct_recharge_trx between 33 and 66 then 2
                end as flag,
                sum(trx_recharge)/count(1) as lim
            from pctx
            group by
                case when pct_recharge_trx between 1 and 32 then 1
                     when pct_recharge_trx between 33 and 66 then 2
                end
        ),
        limxrchgamt as
        (
            select
                case when pct_recharge_amount between 1 and 32 then 1
                     when pct_recharge_amount between 33 and 66 then 2
                end as flag,
                sum(recharge_amount)/count(1) as lim
            from pctx
            group by
                case when pct_recharge_amount between 1 and 32 then 1
                     when pct_recharge_amount between 33 and 66 then 2
                end
        ),
        scorex as
        (
        select
        a.*, b.arpu_l3m_max, b.arpu_l3m_score,
        c.trx_recharge,c.freq_recharge_score,
        d.recharge_amount,d.recharge_amount_score
        from
                (
                    select msisdn, dominan_recharge_channel as recharge_channel,device_type,total_recharge,device_category,
                    dominan_recharge_channel_group as recharge_channel_score,
                    case when device_category = 4 then 3
                         when device_category is null then 2
                         else device_category
                    end as device_score
                    from bde_abt_rfm_temp
                ) a
                left join
                (
                    select msisdn,arpu_l3m_max,min(arpu_l3m_score) arpu_l3m_score
                    from
                        (
                        select msisdn,arpu_l3m_max,case when arpu_l3m_max <= lim then flag else 3 end as arpu_l3m_score
                        from bde_abt_rfm_temp cross join limxarpu
                        )
                    group by msisdn,arpu_l3m_max
                ) b on a.msisdn = b.msisdn
                left join
                (
                    select msisdn,trx_recharge,min(freq_recharge_score) freq_recharge_score
                    from
                        (
                        select msisdn,trx_recharge,case when trx_recharge <= lim then flag else 3 end as freq_recharge_score
                        from bde_abt_rfm_temp cross join limxrchgtrx
                        )
                    group by msisdn,trx_recharge
                ) c on a.msisdn = c.msisdn
                left join
                (
                    select msisdn,recharge_amount,min(recharge_amount_score) recharge_amount_score
                    from
                        (
                        select msisdn,recharge_amount,case when recharge_amount <= lim then flag else 3 end as recharge_amount_score
                        from bde_abt_rfm_temp cross join limxrchgamt
                        )
                    group by msisdn,recharge_amount
                ) d on a.msisdn = d.msisdn
        )
        select
        x.msisdn,
        x.arpu_l3m_max,
        x.recharge_channel,
        x.total_recharge,
        x.trx_recharge,
        x.device_type,
        x.device_category,
        x.recharge_amount,
        x.arpu_l3m_score,
        x.freq_recharge_score,
        x.recharge_amount_score,
        x.device_score,
        x.recharge_channel_score,
        (x.arpu_l3m_score+x.freq_recharge_score+x.recharge_amount_score+x.device_score+x.recharge_channel_score) as total_score
        from scorex x"""
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
        "now() as created_at",
    )
    return res


def selected_columns(
    rfm_bde_ses_segmentation: pyspark.sql.DataFrame, ses_cols_rfm: List[str]
) -> pyspark.sql.DataFrame:

    res = rfm_bde_ses_segmentation.select(*ses_cols_rfm)

    return res


def kuncie_baseline_summary(
    df1: pyspark.sql.DataFrame,
    df2: pyspark.sql.DataFrame,
    df3: pyspark.sql.DataFrame,
    df4: pyspark.sql.DataFrame,
    ses_year_month: str,
) -> Tuple[pyspark.sql.DataFrame, pyspark.sql.DataFrame, pyspark.sql.DataFrame]:
    spark = SQLContext(SparkContext.getOrCreate())

    df1.createOrReplaceTempView("bde_ses_segment")
    df2.createOrReplaceTempView("cb_prepaid_postpaid")
    df3.createOrReplaceTempView("v_bcpusg_appscat_apps_comp")
    df4.createOrReplaceTempView("uln_bcpall")

    res = spark.sql(
        f"""
    with baseline as
    (
        select
        b.msisdn,
        b.ses_segmentation,
        a.kabupaten city,
        a.region_lacci as regional,
        case when a.age < 15 then '01.<15'
            when a.age between 15 and 17 then '02.15-17'
            when a.age between 18 and 24 then '03.18-24'
            when a.age between 25 and 34 then '04.25-34'
            when a.age between 35 and 44 then '05.35-44'
            when a.age between 45 and 55 then '06.45-55'
            when a.age > 55 then '07.>55'
        end as age_segment,
        case when a.los/30 < 3 then '01.0-3 Mo'
            when a.los/30 between 3 and 6 then '02.3-6 Mo'
            when a.los/30 between 6 and 12 then '03.6-12 Mo'
            when a.los/30 > 12 then '04.>12 Mo'
        end as LOS,
        case when upper(a.os_prio) in ('ANDROID','IOS') then os_prio
             else 'Other'
        end as os_type,
        case
             when coalesce(a.rev_broadband,0) < 27000 then '01.<27K'
             when 27000 <= coalesce(a.rev_broadband,0) and coalesce(a.rev_broadband,0) <80000 then '02.27K-80K'
             when 80000 <= coalesce(a.rev_broadband,0) and coalesce(a.rev_broadband,0) <117000 then '03.80K-117K'
             when 117000 <= coalesce(a.rev_broadband,0) and coalesce(a.rev_broadband,0) <230000 then '04.117K-230K'
             else '05.>230K'
        end as arpu_data_segment,
        case
            when (((coalesce(a.vol_broadband,0))/1024)/(1024*1024)) <2 then '01.2GB'
            when 2 <= (((coalesce(a.vol_broadband,0))/1024)/(1024*1024)) and (((coalesce(a.vol_broadband,0))/1024)/(1024*1024)) <10 then '02.2GB-10GB'
            when 10 <= (((coalesce(a.vol_broadband,0))/1024)/(1024*1024)) and (((coalesce(a.vol_broadband,0))/1024)/(1024*1024)) <20 then '03.10GB-20GB'
            when 20 <= (((coalesce(a.vol_broadband,0))/1024)/(1024*1024)) and (((coalesce(a.vol_broadband,0))/1024)/(1024*1024)) <47 then '04.20GB-47GB'
            else '05.>47GB'
        end as payload_segment,
        coalesce(a.total_revenue,0) as revenue
        from
        cb_prepaid_postpaid a inner join
        (select distinct msisdn,ses_segmentation
        from bde_ses_segment) b
        on a.msisdn = b.msisdn
        where lower(a.brand) in ('simpati','kartuas','loop')
    ),
    e_learning_baseline as
    (
    select msisdn,case when e_learning_for_business_user=1 then 'Yes' else 'No' end as e_learning_for_business_user
        from
        (
            select
            msisdn,
            sum(if((((coalesce(payload_extrapolated,0))/1024)/1024)>0,1,0)) e_learning_for_business_user
            from
            (
                select a.msisdn,a.apps,a.payload/b.extrapolated payload_extrapolated
                from
                (
                select msisdn, appscategory, apps, sum(coalesce(payload,0)) payload
                from v_bcpusg_appscat_apps_comp
                where lower(apps) in ('skillacademy',
                                    'udemy',
                                    'kelaskita',
                                    'arkademi',
                                    'coursera',
                                    'indonesiax',
                                    'maubelajarapa',
                                    'edx',
                                    'ted',
                                    'linkedinlearning')
                and month = {ses_year_month}
                group by 1,2,3
                ) a
                inner join
                (
                select msisdn, extrapolated
                from uln_bcpall
                group by 1,2
                ) b
                on a.msisdn = b.msisdn
            )
            group by msisdn
        )
    ),
    active_ecommerce_baseline as
    (
    select msisdn,case when active_e_commerce=1 then 'Yes' else 'No' end as active_e_commerce
        from
        (
            select
            msisdn,
            sum(if((((coalesce(payload_extrapolated,0))/1024)/1024)>100,1,0)) active_e_commerce
            from
            (
                select a.msisdn,a.apps,a.payload/b.extrapolated payload_extrapolated
                from
                (
                select msisdn, appscategory, apps, sum(coalesce(payload,0)) payload
                from v_bcpusg_appscat_apps_comp
                where lower(apps) in ('tokopedia',
                                    'shopee',
                                    'bukalapak',
                                    'lazada',
                                    'blibli',
                                    'jdid',
                                    'zalora',
                                    'ecommercesites',
                                    'alibaba',
                                    'olx',
                                    'aliexpress',
                                    'amazon',
                                    'ebay',
                                    'pomelofashion',
                                    'orami',
                                    'bhinneka',
                                    'shopback',
                                    'elevenia',
                                    'ilotte',
                                    'rakuten')
                and month = {ses_year_month}
                group by 1,2,3
                ) a
                inner join
                (
                select msisdn, extrapolated
                from uln_bcpall
                group by 1,2
                ) b
                on a.msisdn = b.msisdn
            )
            group by msisdn
        )
    ),
    e_commerce_baseline as
    (
    select msisdn,case when e_commerce_seller=1 then 'Yes' else 'No' end as e_commerce_seller
        from
        (
            select
            msisdn,
            sum(if((((coalesce(payload_extrapolated,0))/1024)/1024)>0,1,0)) e_commerce_seller
            from
            (
                select a.msisdn,a.apps,a.payload/b.extrapolated payload_extrapolated
                from
                (
                select msisdn, appscategory, apps, sum(coalesce(payload,0)) payload
                from v_bcpusg_appscat_apps_comp
                where lower(apps) in ('carousell',
                                    'grabkios',
                                    'mitrabukalapak',
                                    'zaloraseller',
                                    'mitratokopedia',
                                    'prelo',
                                    'instagrambusiness',
                                    'koinworks')
                and month = {ses_year_month}
                group by 1,2,3
                ) a
                inner join
                (
                select msisdn, extrapolated
                from uln_bcpall
                group by 1,2
                ) b
                on a.msisdn = b.msisdn
            )
            group by msisdn
        )
    ),
    inspiration_seeker_baseline as
    (
    select msisdn,case when count_apps>=2 then 'Yes' else 'No' end as inspiration_seeker
        from
        (
            select
            msisdn,
            count(distinct lower(apps)) count_apps
            from
            (
                select a.msisdn,a.apps,a.payload/b.extrapolated payload_extrapolated
                from
                (
                select msisdn, appscategory, apps, sum(coalesce(payload,0)) payload
                from v_bcpusg_appscat_apps_comp
                where lower(apps) in ('youtube',
                                    'pinterest',
                                    'instagram',
                                    'podcastsites',
                                    'inspigo',
                                    'podcastradiomusiccastbox',
                                    'podbean',
                                    'spotify',
                                    'soundcloud',
                                    'kindle',
                                    'dribbble')
                and month = {ses_year_month}
                group by 1,2,3
                ) a
                inner join
                (
                select msisdn, extrapolated
                from uln_bcpall
                group by 1,2
                ) b
                on a.msisdn = b.msisdn
            )
            group by msisdn
        )
    ),
    career_aspirant_baseline as
    (
    select msisdn,case when career_aspi=1 then 'Yes' else 'No' end as career_aspirant
        from
        (
            select
            msisdn,
            sum(if((((coalesce(payload_extrapolated,0))/1024)/1024)>0,1,0)) career_aspi
            from
            (
                select a.msisdn,a.apps,a.payload/b.extrapolated payload_extrapolated
                from
                (
                select msisdn, appscategory, apps, sum(coalesce(payload,0)) payload
                from v_bcpusg_appscat_apps_comp
                where lower(apps) in ('linkedin',
                                    'linkedinlearning',
                                    'udemy',
                                    'coursera',
                                    'edx',
                                    'skillacademy',
                                    'arkademi',
                                    'indonesiax',
                                    'ted',
                                    'evernote')
                and month = {ses_year_month}
                group by 1,2,3
                ) a
                inner join
                (
                select msisdn, extrapolated
                from uln_bcpall
                group by 1,2
                ) b
                on a.msisdn = b.msisdn
            )
            group by msisdn
        )
    ),
    tech_greek_baseline as
    (
    select msisdn,case when techgreek=1 then 'Yes' else 'No' end as tech_greek
        from
        (
            select
            msisdn,
            sum(if((((coalesce(payload_extrapolated,0))/1024)/1024)>0,1,0)) techgreek
            from
            (
                select a.msisdn,a.apps,a.payload/b.extrapolated payload_extrapolated
                from
                (
                select msisdn, appscategory, apps, sum(coalesce(payload,0)) payload
                from v_bcpusg_appscat_apps_comp
                where lower(apps) in ('technologysites',
                                    'github',
                                    'khanacademy',
                                    'sololearn',
                                    'createjs',
                                    'myscript',
                                    'gsmarena')
                and month = {ses_year_month}
                group by 1,2,3
                ) a
                inner join
                (
                select msisdn, extrapolated
                from uln_bcpall
                group by 1,2
                ) b
                on a.msisdn = b.msisdn
            )
            group by msisdn
        )
    ),
    creatively_hungry_baseline as
    (
    select msisdn,case when creatively_hu=1 then 'Yes' else 'No' end as creatively_hungry
        from
        (
            select
            msisdn,
            sum(if((((coalesce(payload_extrapolated,0))/1024)/1024)>0,1,0)) creatively_hu
            from
            (
                select a.msisdn,a.apps,a.payload/b.extrapolated payload_extrapolated
                from
                (
                select msisdn, appscategory, apps, sum(coalesce(payload,0)) payload
                from v_bcpusg_appscat_apps_comp
                where lower(apps) in ('fiverr',
                                    'etsy',
                                    'maubelajarapa',
                                    'kelaskita',
                                    'canva')
                and month = {ses_year_month}
                group by 1,2,3
                ) a
                inner join
                (
                select msisdn, extrapolated
                from uln_bcpall
                group by 1,2
                ) b
                on a.msisdn = b.msisdn
            )
            group by msisdn
        )
    )
    select *,
    case
        when e_commerce_seller = 'Yes' then 'e_commerce_seller'
        when (e_commerce_seller = 'No' and active_e_commerce = 'Yes') then 'active_e_commerce'
        when (e_commerce_seller = 'No' and active_e_commerce = 'No' and inspiration_seeker = 'Yes') then 'inspiration_seeker'
        else 'Others'
    end as persona_kuncie
    from
    (
        select
        a.msisdn,a.ses_segmentation,a.revenue,a.city,a.regional,a.age_segment,a.LOS,a.os_type,
        coalesce(b.e_learning_for_business_user,'No') e_learning_for_business_user,
        coalesce(c.active_e_commerce,'No') active_e_commerce,
        coalesce(d.e_commerce_seller,'No') e_commerce_seller,
        coalesce(e.inspiration_seeker,'No') inspiration_seeker,
        coalesce(f.career_aspirant,'No') career_aspirant,
        coalesce(g.tech_greek,'No') tech_greek,
        coalesce(h.creatively_hungry,'No') creatively_hungry,
        a.arpu_data_segment,a.payload_segment
        from baseline a
        left join
        e_learning_baseline b on a.msisdn = b.msisdn
        left join
        active_ecommerce_baseline c on a.msisdn = c.msisdn
        left join
        e_commerce_baseline d on a.msisdn = d.msisdn
        left join
        inspiration_seeker_baseline e on a.msisdn = e.msisdn
        left join
        career_aspirant_baseline f on a.msisdn = f.msisdn
        left join
        tech_greek_baseline g on a.msisdn = g.msisdn
        left join
        creatively_hungry_baseline h on a.msisdn = h.msisdn
    )
    """
    )
    res.createOrReplaceTempView("baseline_kuncie")

    dfs = spark.sql("""select * from baseline_kuncie""")

    dgroup = (
        dfs.cache()
        .groupBy(
            "ses_segmentation",
            "city",
            "regional",
            "age_segment",
            "LOS",
            "os_type",
            "e_learning_for_business_user",
            "active_e_commerce",
            "e_commerce_seller",
            "inspiration_seeker",
            "career_aspirant",
            "tech_greek",
            "creatively_hungry",
            "arpu_data_segment",
            "payload_segment",
        )
        .agg(
            f.count("msisdn").alias("subs"),
            f.countDistinct("msisdn").alias("uniq_subs"),
            f.min("revenue").alias("min_revenue"),
            f.avg("revenue").alias("avg_revenue"),
            f.max("revenue").alias("max_revenue"),
        )
    )

    dgroups = (
        dfs.cache()
        .groupBy(
            "ses_segmentation",
            "city",
            "regional",
            "age_segment",
            "LOS",
            "os_type",
            "persona_kuncie",
            "arpu_data_segment",
            "payload_segment",
        )
        .agg(
            f.count("msisdn").alias("subs"),
            f.countDistinct("msisdn").alias("uniq_subs"),
            f.min("revenue").alias("min_revenue"),
            f.avg("revenue").alias("avg_revenue"),
            f.max("revenue").alias("max_revenue"),
        )
    )

    return res, dgroup, dgroups


def fitae_baseline_summary(
    df1: pyspark.sql.DataFrame,
    df2: pyspark.sql.DataFrame,
    df3: pyspark.sql.DataFrame,
    df4: pyspark.sql.DataFrame,
    ses_year_month: str,
) -> Tuple[pyspark.sql.DataFrame, pyspark.sql.DataFrame, pyspark.sql.DataFrame]:
    spark = SQLContext(SparkContext.getOrCreate())

    df1.createOrReplaceTempView("bde_ses_segment")
    df2.createOrReplaceTempView("cb_prepaid_postpaid")
    df3.createOrReplaceTempView("v_bcpusg_appscat_apps_comp")
    df4.createOrReplaceTempView("uln_bcpall")

    res = spark.sql(
        f"""
    with baseline as
    (
        select
        b.msisdn,
        b.ses_segmentation,
        a.kabupaten city,
        a.region_lacci as regional,
        case when a.age < 15 then '01.<15'
            when a.age between 15 and 17 then '02.15-17'
            when a.age between 18 and 24 then '03.18-24'
            when a.age between 25 and 34 then '04.25-34'
            when a.age between 35 and 44 then '05.35-44'
            when a.age between 45 and 55 then '06.45-55'
            when a.age > 55 then '07.>55'
        end as age_segment,
        case when gender = 'Mdr9KE2SRTu3bNmEQn3H%2Bw%3D%3D%0A' then 'male'
             when gender = 'ov%2Bz%2FyW6b%2BfPd3lZMdMf7w%3D%3D%0A' then 'female'
        else 'unknown'
        end as gender,
        case when a.los/30 < 3 then '01.0-3 Mo'
            when a.los/30 between 3 and 6 then '02.3-6 Mo'
            when a.los/30 between 6 and 12 then '03.6-12 Mo'
            when a.los/30 > 12 then '04.>12 Mo'
        end as LOS,
        case when upper(a.os_prio) in ('ANDROID','IOS') then os_prio
             else 'Other'
        end as os_type,
        case
             when coalesce(a.rev_broadband,0) < 27000 then '01.<27K'
             when 27000 <= coalesce(a.rev_broadband,0) and coalesce(a.rev_broadband,0) <80000 then '02.27K-80K'
             when 80000 <= coalesce(a.rev_broadband,0) and coalesce(a.rev_broadband,0) <117000 then '03.80K-117K'
             when 117000 <= coalesce(a.rev_broadband,0) and coalesce(a.rev_broadband,0) <230000 then '04.117K-230K'
             else '05.>230K'
        end as arpu_data_segment,
        case
            when (((coalesce(a.vol_broadband,0))/1024)/(1024*1024)) <2 then '01.2GB'
            when 2 <= (((coalesce(a.vol_broadband,0))/1024)/(1024*1024)) and (((coalesce(a.vol_broadband,0))/1024)/(1024*1024)) <10 then '02.2GB-10GB'
            when 10 <= (((coalesce(a.vol_broadband,0))/1024)/(1024*1024)) and (((coalesce(a.vol_broadband,0))/1024)/(1024*1024)) <20 then '03.10GB-20GB'
            when 20 <= (((coalesce(a.vol_broadband,0))/1024)/(1024*1024)) and (((coalesce(a.vol_broadband,0))/1024)/(1024*1024)) <47 then '04.20GB-47GB'
            else '05.>47GB'
        end as payload_segment,
        coalesce(a.total_revenue,0) as revenue
        from
        cb_prepaid_postpaid a inner join
        (select distinct msisdn,ses_segmentation from bde_ses_segment) b
        on a.msisdn = b.msisdn
        where lower(a.brand) in ('simpati','kartuas','loop')
    ),
    cho_baseline as
    (
    select msisdn,case when cho=1 then 'Yes' else 'No' end as chief_home_officer from
        (
            select
            msisdn,
            sum(if((((coalesce(payload_extrapolated,0))/1024)/1024)>0,1,0)) cho
            from
            (
                select a.msisdn,a.apps,a.payload/b.extrapolated payload_extrapolated
                from
                (
                select msisdn, appscategory, apps, sum(coalesce(payload,0)) payload
                from v_bcpusg_appscat_apps_comp
                where lower(apps) in ('cookingsites','cookpad','yummy')
                and month = {ses_year_month}
                group by 1,2,3
                ) a
                inner join
                (
                select msisdn, extrapolated
                from uln_bcpall
                group by 1,2
                ) b
                on a.msisdn = b.msisdn
            )
            group by msisdn
        )
    ),
    wg_baseline as
    (
    select msisdn,case when wg=1 then 'Yes' else 'No' end as wellness_generation from
        (
            select
            msisdn,
            sum(if((((coalesce(payload_extrapolated,0))/1024)/1024)>0,1,0)) wg
            from
            (
                select a.msisdn,a.apps,a.payload/b.extrapolated payload_extrapolated
                from
                (
                select msisdn, appscategory, apps, sum(coalesce(payload,0)) payload
                from v_bcpusg_appscat_apps_comp
                where lower(apps) in ('sportssites')
                and month = {ses_year_month}
                group by 1,2,3
                ) a
                inner join
                (
                select msisdn, extrapolated
                from uln_bcpall
                group by 1,2
                ) b
                on a.msisdn = b.msisdn
            )
            group by msisdn
        )
    ),
    bs_baseline as
    (
    select msisdn,case when bs=1 then 'Yes' else 'No' end as baby_steps from
        (
            select
            msisdn,
            sum(if((((coalesce(payload_extrapolated,0))/1024)/1024)>0,1,0)) bs
            from
            (
                select a.msisdn,a.apps,a.payload/b.extrapolated payload_extrapolated
                from
                (
                select msisdn, appscategory, apps, sum(coalesce(payload,0)) payload
                from v_bcpusg_appscat_apps_comp
                where lower(apps) in ('bolasport','bolacom','bola.net','stravarun','nikerun','garmin')
                and month = {ses_year_month}
                group by 1,2,3
                ) a
                inner join
                (
                select msisdn, extrapolated
                from uln_bcpall
                group by 1,2
                ) b
                on a.msisdn = b.msisdn
            )
            group by msisdn
        )
    ),
    welloff_baseline as
    (
    select msisdn,case when wll=1 then 'Yes' else 'No' end as the_well_offs from
        (
            select
            msisdn,
            sum(if((((coalesce(payload_extrapolated,0))/1024)/1024)>0,1,0)) wll
            from
            (
                select a.msisdn,a.apps,a.payload/b.extrapolated payload_extrapolated
                from
                (
                select msisdn, appscategory, apps, sum(coalesce(payload,0)) payload
                from v_bcpusg_appscat_apps_comp
                where lower(apps) in ('healthsites','sehatq')
                and month = {ses_year_month}
                group by 1,2,3
                ) a
                inner join
                (
                select msisdn, extrapolated
                from uln_bcpall
                group by 1,2
                ) b
                on a.msisdn = b.msisdn
            )
            group by msisdn
        )
    ),
    diabeate_baseline as
    (
    select msisdn,case when diabeate=1 then 'Yes' else 'No' end as diabeaters from
        (
            select
            msisdn,
            sum(if((((coalesce(payload_extrapolated,0))/1024)/1024)>0,1,0)) diabeate
            from
            (
                select a.msisdn,a.apps,a.payload/b.extrapolated payload_extrapolated
                from
                (
                select msisdn, appscategory, apps, sum(coalesce(payload,0)) payload
                from v_bcpusg_appscat_apps_comp
                where lower(apps) in ('telkomselalodokter',
                                      'halodoc',
                                      'klikdokter',
                                      'doktersehat')
                and month = {ses_year_month}
                group by 1,2,3
                ) a
                inner join
                (
                select msisdn, extrapolated
                from uln_bcpall
                group by 1,2
                ) b
                on a.msisdn = b.msisdn
            )
            group by msisdn
        )
    ),
    od_baseline as
    (
    select msisdn,case when od=1 then 'Yes' else 'No' end as operation_downsize from
        (
            select
            msisdn,
            sum(if((((coalesce(payload_extrapolated,0))/1024)/1024)>0,1,0)) od
            from
            (
                select a.msisdn,a.apps,a.payload/b.extrapolated payload_extrapolated
                from
                (
                select msisdn, appscategory, apps, sum(coalesce(payload,0)) payload
                from v_bcpusg_appscat_apps_comp
                where lower(apps) in ('wellnesssites',
                                    'hellosehat',
                                    'caloriecounterbyfatsecret',
                                    'mobilejkninsurance',
                                    'bpjsesehataninsurance',
                                    'prudentialfinancialinsurance',
                                    'pasarpolisinsurance',
                                    'lifepalinsurance')
                and month = {ses_year_month}
                group by 1,2,3
                ) a
                inner join
                (
                select msisdn, extrapolated
                from uln_bcpall
                group by 1,2
                ) b
                on a.msisdn = b.msisdn
            )
            group by msisdn
        )
    )
    select *,
    case
        when baby_steps = 'Yes' then 'baby_steps'
        when (baby_steps = 'No' and wellness_generation = 'Yes') then 'wellness_generation'
        when (baby_steps = 'No' and wellness_generation = 'No' and chief_home_officer = 'Yes') then 'chief_home_officer'
        else 'Others'
    end as persona_fitae
    from
    (
        select
        a.msisdn,a.ses_segmentation,a.revenue,a.city,a.regional,a.age_segment,a.gender,a.LOS,a.os_type,
        coalesce(b.chief_home_officer,'No') chief_home_officer,
        coalesce(c.wellness_generation,'No') wellness_generation,
        coalesce(d.baby_steps,'No') baby_steps,
        coalesce(e.the_well_offs,'No') the_well_offs,
        coalesce(f.diabeaters,'No') diabeaters,
        coalesce(g.operation_downsize,'No') operation_downsize,
        a.arpu_data_segment,a.payload_segment
        from baseline a
        left join
        cho_baseline b on a.msisdn = b.msisdn
        left join
        wg_baseline c on a.msisdn = c.msisdn
        left join
        bs_baseline d on a.msisdn = d.msisdn
        left join
        welloff_baseline e on a.msisdn = e.msisdn
        left join
        diabeate_baseline f on a.msisdn = f.msisdn
        left join
        od_baseline g on a.msisdn = g.msisdn
    )
    """
    )
    res.createOrReplaceTempView("baseline_fitae")

    dfs = spark.sql("""select * from baseline_fitae""")
    dfgroup = (
        dfs.cache()
        .groupBy(
            "ses_segmentation",
            "city",
            "regional",
            "age_segment",
            "gender",
            "LOS",
            "os_type",
            "chief_home_officer",
            "wellness_generation",
            "baby_steps",
            "the_well_offs",
            "operation_downsize",
            "diabeaters",
            "arpu_data_segment",
            "payload_segment",
        )
        .agg(
            f.count("msisdn").alias("subs"),
            f.countDistinct("msisdn").alias("uniq_subs"),
            f.min("revenue").alias("min_revenue"),
            f.avg("revenue").alias("avg_revenue"),
            f.max("revenue").alias("max_revenue"),
        )
    )

    dfgroups = (
        dfs.cache()
        .groupBy(
            "ses_segmentation",
            "city",
            "regional",
            "age_segment",
            "gender",
            "LOS",
            "os_type",
            "persona_fitae",
            "arpu_data_segment",
            "payload_segment",
        )
        .agg(
            f.count("msisdn").alias("subs"),
            f.countDistinct("msisdn").alias("uniq_subs"),
            f.min("revenue").alias("min_revenue"),
            f.avg("revenue").alias("avg_revenue"),
            f.max("revenue").alias("max_revenue"),
        )
    )

    return res, dfgroup, dfgroups


def join_rfm_dmp(
    rfm_table: pyspark.sql.DataFrame,
    dmp_table: pyspark.sql.DataFrame,
    agg_table: pyspark.sql.DataFrame,
    agg_cols_to_select,
    ses_cols_to_select,
    segment,
    weekstart,
    ses_cols_rfm: List[str],
    ses_train_score: str,
    cols=None,
    how: str = "left",
    fraction: int = 0.1,
) -> pyspark.sql.DataFrame:

    assert ses_train_score in ("train", "score")
    if ses_train_score == "score":
        agg_table = agg_table.filter(agg_table.weekstart == weekstart).select(
            agg_cols_to_select
        )
        cols = ["msisdn"] if cols is None else cols
        dmp_table = dmp_table.dropDuplicates(cols)
        dmp = dmp_table.join(agg_table, cols, how=how)

        rfm_table = rfm_table.filter(rfm_table.ses_segmentation == segment)
        rfm_table = rfm_table.select(*ses_cols_rfm)
        cols = ["msisdn"] if cols is None else cols
        dmp = dmp.select(ses_cols_to_select).dropDuplicates(cols)
        res = rfm_table.join(dmp, cols, how=how)
        res = res.select(ses_cols_to_select + ses_cols_rfm[1:])
    else:
        dmp_table = dmp_table.sample(False, fraction, 42).sample(False, fraction, 42)
        agg_table = agg_table.filter(agg_table.weekstart == weekstart).select(
            agg_cols_to_select
        )
        cols = ["msisdn"] if cols is None else cols
        dmp_table = dmp_table.dropDuplicates(cols)
        dmp = dmp_table.join(agg_table, cols, how=how)

        rfm_table = rfm_table.filter(rfm_table.ses_segmentation == segment)
        rfm_table = rfm_table.select(*ses_cols_rfm)
        cols = ["msisdn"] if cols is None else cols
        dmp = dmp.select(ses_cols_to_select).dropDuplicates(cols)
        res = rfm_table.join(dmp, cols, how=how)
        res = res.select(ses_cols_to_select + ses_cols_rfm[1:])

    return res


def check_consistency(df: pyspark.sql.DataFrame) -> None:

    print(df.agg(f.count("msisdn"), f.countDistinct("msisdn")).toPandas())
    print(
        df.groupBy("arpu_l3m_score")
        .agg(
            f.min("arpu_l3m_max").alias("min_rev_l3m_max"),
            f.avg("arpu_l3m_max").alias("avg_rev_l3m_max"),
            f.stddev("arpu_l3m_max").alias("stdev_rev_l3m_max"),
            f.max("arpu_l3m_max").alias("max_rev_l3m_max"),
        )
        .toPandas()
    )
    print(
        df.groupBy("freq_recharge_score")
        .agg(
            f.min("trx_recharge").alias("min_tot_freq_recharge"),
            f.avg("trx_recharge").alias("avg_tot_freq_recharge"),
            f.stddev("trx_recharge").alias("stdev_tot_freq_recharge"),
            f.max("trx_recharge").alias("max_tot_freq_recharge"),
        )
        .toPandas()
    )
    print(
        df.groupBy("recharge_amount_score")
        .agg(
            f.min("recharge_amount").alias("min_recharge_trx"),
            f.avg("recharge_amount").alias("avg_recharge_trx"),
            f.stddev("recharge_amount").alias("stdev_recharge_trx"),
            f.max("recharge_amount").alias("max_recharge_trx"),
        )
        .toPandas()
    )
