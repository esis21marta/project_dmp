from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyspark
from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import lit
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score


##### nodes training ####
def select_cols_topandas(
    df: pyspark.sql.DataFrame, ses_cols_to_select_log: List[str]
) -> Tuple[pd.DataFrame, List[str]]:

    df = df.na.fill(0).limit(2000000).toPandas()
    df.loc[:, ses_cols_to_select_log] = np.log(1 + df.loc[:, ses_cols_to_select_log])
    num_col = df.select_dtypes(np.number).columns.tolist()
    res = df[num_col].fillna(0)

    return res, num_col


def silhoutte_score_result(
    df: pd.DataFrame, path_to_save_inertia: str, ses_segment: str
) -> None:

    df = df.values
    ssd = []
    range_n_clusters = [2, 3, 4, 5, 6, 7, 8, 9, 10]
    for num_clusters in range_n_clusters:
        kmeans = KMeans(n_clusters=num_clusters, max_iter=100, random_state=123)
        kmeans.fit(df)
        ssd.append(kmeans.inertia_)
        cluster_labels = kmeans.labels_
        # silhouette score
        silhouette_avg = silhouette_score(df, cluster_labels)
        print(
            "For n_clusters={0}, the silhouette score is {1}".format(
                num_clusters, silhouette_avg
            )
        )
    # plot the SSDs for each n_clusters
    plt.figure(1, figsize=(8, 5))
    plt.plot(range_n_clusters, ssd, "o")
    plt.plot(range_n_clusters, ssd, "-", alpha=0.5)
    plt.xlabel(f"Number of Clusters {ses_segment}"), plt.ylabel("Inertia")
    plt.savefig(path_to_save_inertia)


def kmeans_clustering_fit(
    df: pd.DataFrame,
    _ses_clustering_parameter: Dict[str, str],
    ses_cols_to_select_log: List[str],
    max_rows: int = int(1e6),
) -> KMeans:
    km = KMeans(
        n_clusters=_ses_clustering_parameter["n_clusters"],
        n_init=_ses_clustering_parameter["n_init_clusters"],
        max_iter=_ses_clustering_parameter["max_iter_clusters"],
        n_jobs=_ses_clustering_parameter["n_jobs_clusters"],
        random_state=_ses_clustering_parameter["random_state_clustering"],
    )

    print(df.shape)
    if len(df) > max_rows:
        print("-----")
        np.random.seed(42)
        random_rows = np.random.choice(len(df), max_rows, False)
        df = df.loc[random_rows, :]
    print(len(df))
    df.loc[:, ses_cols_to_select_log] = np.log(1 + df.loc[:, ses_cols_to_select_log])
    df.select_dtypes(np.number).columns.tolist()
    dfs = df.values
    km.fit(dfs)

    return km


##### nodes scoring ####
def to_pandas(spark_table: pyspark.sql.DataFrame) -> pd.DataFrame:
    res = pd.DataFrame(spark_table.collect(), columns=spark_table.columns)

    return res


def scoring_clus(
    ses_cols_to_select_log: List[str],
    ses_cols_to_drop: List[str],
    ses_segment: str,
    df: pd.DataFrame,
    kmeans: KMeans,
) -> pd.DataFrame:
    # please remove line 85 to 103 if using new pickle
    df = df.rename(
        columns={
            "fea_int_app_usage_onlinebanking_data_vol_01m": "fea_int_app_usage_banking_data_vol_01m",
            "fea_int_app_usage_onlinebanking_data_vol_02m": "fea_int_app_usage_banking_data_vol_02m",
            "fea_int_app_usage_onlinebanking_data_vol_03m": "fea_int_app_usage_banking_data_vol_03m",
            "fea_int_app_usage_onlinebanking_accessed_apps_01m": "fea_int_app_usage_banking_accessed_apps_01m",
            "fea_int_app_usage_onlinebanking_accessed_apps_02m": "fea_int_app_usage_banking_accessed_apps_02m",
            "fea_int_app_usage_onlinebanking_accessed_apps_03m": "fea_int_app_usage_banking_accessed_apps_03m",
            "fea_int_app_usage_social_media_sites_data_vol_01m": "fea_int_app_usage_social_media_data_vol_01m",
            "fea_int_app_usage_social_media_sites_data_vol_02m": "fea_int_app_usage_social_media_data_vol_02m",
            "fea_int_app_usage_social_media_sites_data_vol_03m": "fea_int_app_usage_social_media_data_vol_03m",
            "fea_int_app_usage_social_media_sites_accessed_apps_01m": "fea_int_app_usage_social_media_accessed_apps_01m",
            "fea_int_app_usage_social_media_sites_accessed_apps_02m": "fea_int_app_usage_social_media_accessed_apps_02m",
            "fea_int_app_usage_social_media_sites_accessed_apps_03m": "fea_int_app_usage_social_media_accessed_apps_03m",
            "fea_int_app_usage_payments_data_vol_01m": "fea_int_app_usage_wallets_and_payments_data_vol_01m",
            "fea_int_app_usage_payments_data_vol_02m": "fea_int_app_usage_wallets_and_payments_data_vol_02m",
            "fea_int_app_usage_payments_data_vol_03m": "fea_int_app_usage_wallets_and_payments_data_vol_03m",
            "fea_int_app_usage_payments_accessed_apps_01m": "fea_int_app_usage_wallets_and_payments_accessed_apps_01m",
            "fea_int_app_usage_payments_accessed_apps_02m": "fea_int_app_usage_wallets_and_payments_accessed_apps_02m",
            "fea_int_app_usage_payments_accessed_apps_03m": "fea_int_app_usage_wallets_and_payments_accessed_apps_03m",
        }
    )

    num_col = df.select_dtypes(np.number).columns.tolist()
    df.loc[:, ses_cols_to_select_log] = np.log(1 + df.loc[:, ses_cols_to_select_log])
    res = (
        df.drop("msisdn", axis=1)[num_col]
        .drop(ses_cols_to_drop, axis=1)
        .fillna(0)
        .values
    )
    cluster_pred = kmeans.predict(res)
    df["cluster"] = cluster_pred
    res = df[["msisdn", "cluster"]]

    return res


def to_hdfs(df_pandas: pd.DataFrame) -> pyspark.sql.DataFrame:

    spark = SparkContext.getOrCreate()
    df_spark = SQLContext(spark).createDataFrame(df_pandas)
    return df_spark


def concatenate_results_of_clustering(
    *X: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    if len(X) == 0:
        raise NotImplementedError()
    res = X[0]
    for el in X[1:]:
        res = res.union(el)
    return res


def concatenate_results_of_rfm_and_clustering(
    X_low: pyspark.sql.DataFrame,
    X_medium: pyspark.sql.DataFrame,
    X_high: pyspark.sql.DataFrame,
    cols=None,
    how: str = "left",
) -> pyspark.sql.DataFrame:

    df_low = X_low.withColumn("ses_segmentation", lit("low"))
    df_medium = X_medium.withColumn("ses_segmentation", lit("medium"))
    df_high = X_high.withColumn("ses_segmentation", lit("high"))
    df = df_low.union(df_medium).union(df_high)
    res = df.selectExpr(
        "*",
        "case when ses_segmentation  = 'low' and cluster = 0 then 'Low Budget'\
                                  when ses_segmentation  = 'low' and cluster = 1 then 'Basic Data Users'\
                                  when ses_segmentation  = 'low' and cluster = 2 then 'Casual Users'\
                                  when ses_segmentation  = 'low' and cluster = 3 then 'Entry Users'\
                                  when ses_segmentation  = 'medium' and cluster = 0 then 'Legacy Oriented'\
                                  when ses_segmentation  = 'medium' and cluster = 2 then 'Casual Gamers'\
                                  when ses_segmentation  = 'medium' and cluster = 3 then 'Browsing Addict'\
                                  when ses_segmentation  = 'high' and cluster = 0 then 'Primary Users'\
                                  when ses_segmentation  = 'high' and cluster = 1 then 'Social Explorer'\
                                  when ses_segmentation  = 'high' and cluster = 2 then 'Prestige Buyers'\
                                  when ses_segmentation  = 'high' and cluster = 3 then 'Entertainments Addicts'\
                              else 'exclude' end as sub_segments_named",
    )
    return res


def concatenate_results_of_clustering(
    *X: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    if len(X) == 0:
        raise NotImplementedError()
    res = X[0]
    for el in X[1:]:
        res = res.union(el)
    return res


#### summary ###
# def recharges_dd(table_a: pyspark.sql.DataFrame,table_b: pyspark.sql.DataFrame,
#                 ses_weekstart:str) -> pyspark.sql.DataFrame:

#     res_a = table_a.selectExpr("msisdn",
#                                "case when voucher_type <> 1 then 'rech_package' else 'rech_balance' end rech_type",
#                                "trx_rech","rech",
#                                f"case when event_date between concat(substring('{ses_weekstart}',1,7),'-','01') and '{ses_weekstart}' then 1 else 0 END flag")
#     res_a = res_a.filter("flag=1")

#     res_b = table_a.selectExpr("msisdn",
#                                "case when rech_type like 'Pulsa%' then 'rech_package' else 'rech_balance' end rech_type",
#                                "trx_rech","rech",
#                                f"case when event_date between concat(substring('{ses_weekstart}',1,7),'-','01') and '{ses_weekstart}' then 1 else 0 END flag")
#     res_b = res_b.filter("flag=1")

#     res = res_a.union(res_b).groupBy("msisdn","rech_type").agg(F.sum('trx_rech').alias('trx_rech'),
#                                                                               F.sum('rech').alias('rech'))

#     return res


# def summary_step1(table_a: pyspark.sql.DataFrame,table_c: pyspark.sql.DataFrame,
#               table_d: pyspark.sql.DataFrame,table_e: pyspark.sql.DataFrame,table_f: pyspark.sql.DataFrame,
#               ses_source_table: List[str],
#               ses_year:str,
#               cols=None,how='left') -> pyspark.sql.DataFrame:

#     sc = SparkContext.getOrCreate()
#     spark = SparkSession(sc)

#     table_a1 = table_a.selectExpr("msisdn")
#     table_b1 = spark.sql(f"""select msisdn,concat(lac,'-',ci) lac_ci,region_lacci as regional,kabupaten as city,
#                              device_type,status,data_sdn_flag,data_flag,cust_segment_subtype,
#                              total_revenue,rev_broadband,rev_voice,
#                              rev_digital_services,rev_sms,total_recharge,trx_recharge,
#                              trx_sms,trx_voice,trx_digital_services,vol_broadband,dur_voice,los
#                              from {ses_source_table[8]}""")
#     table_c1 = table_c.selectExpr("msisdn","msisdn as c_msisdn")
#     table_d1 = table_d.selectExpr("msisdn","coalesce(payload_extrapolated,0) games_payload")\
#     .groupBy("msisdn").agg(F.sum('games_payload').alias('games_payload'))
#     table_e1 = table_e.selectExpr("msisdn","coalesce(payload_extrapolated,0) video_payload")\
#     .groupBy("msisdn").agg(F.sum('video_payload').alias('video_payload'))
#     table_e2 = table_e.selectExpr("msisdn","coalesce(payload_extrapolated,0) youtube_payload","lower(apps) apps")\
#     .filter("apps='youtube'").groupBy("msisdn").agg(F.sum('youtube_payload').alias('youtube_payload'))
#     table_e3 = table_e.selectExpr("msisdn","coalesce(payload_extrapolated,0) tiktok_payload","lower(apps) apps")\
#     .filter("apps='tiktok'").groupBy("msisdn").agg(F.sum('tiktok_payload').alias('tiktok_payload'))
#     table_e4 = table_e.selectExpr("msisdn","lower(apps) apps").filter("apps='netflix'")\
#     .groupBy("msisdn").agg(F.count('msisdn').alias('count_msisdn_netflix'))
#     table_e5 = table_e.selectExpr("msisdn","coalesce(payload_extrapolated,0) maxstream_payload","lower(apps) apps")\
#     .filter("apps='maxstream'").groupBy("msisdn").agg(F.sum('maxstream_payload').alias('maxstream_payload'))
#     table_e6 = table_e.selectExpr("msisdn","lower(apps) apps").filter("apps='viu'")\
#     .groupBy("msisdn").agg(F.count('msisdn').alias('count_msisdn_viu'))

#     table_f1 = table_f.selectExpr("msisdn","month","lower(apps) apps","case when msisdn like '628%' then 1 else 0 end as flag")\
#                       .filter(f"month = '{ses_year}' and apps = 'indihome' and flag = 1")\
#                       .groupBy("msisdn").agg(F.count('msisdn').alias('count_msisdn_indihome'))
#     table_f2 = table_f.selectExpr("msisdn","month","lower(apps) apps","case when msisdn like '628%' then 1 else 0 end as flag")\
#                       .filter(f"month = '{ses_year}' and apps = 'firstmedia' and flag = 1")\
#                       .groupBy("msisdn").agg(F.count('msisdn').alias('count_msisdn_firstmedia'))
#     table_g1 = spark.sql(f"""select msisdn,device_category,dominan_recharge_channel as recharge_channel from {ses_source_table[2]}""")

#     cols = ['msisdn'] if cols is None else cols
#     table_join = table_a1.join(table_b1,cols,how=how).join(table_c1,cols,how=how).join(table_d1,cols,how=how)\
#             .join(table_e1,cols,how=how).join(table_e2,cols,how=how).join(table_e3,cols,how=how)\
#             .join(table_e4,cols,how=how).join(table_e5,cols,how=how).join(table_e6,cols,how=how)\
#             .join(table_f1,cols,how=how).join(table_f2,cols,how=how).join(table_g1,cols,how=how)

#     res = table_join.selectExpr("msisdn","lac_ci","regional","city","device_type","status","data_sdn_flag","data_flag",
#                                 "cust_segment_subtype",
#                                 "case when coalesce(total_revenue,0) > 0 then 1 else 0 end as RGB",
#                                 "case when coalesce(rev_broadband,0) > 0 then 1 else 0 end as RGB_Data",
#                                 "case when coalesce(rev_voice,0) > 0 then 1 else 0 end as RGB_Voice",
#                                 "case when coalesce(rev_digital_services,0) > 0 then 1 else 0 end as RGB_DLS",
#                                 "case when los is null then 240 else los/30 end as los_month",
#                                 "coalesce(total_revenue,0) as revenue",
#                                 "coalesce(rev_broadband,0) as rev_data",
#                                 "coalesce(rev_voice,0) rev_voice",
#                                 "coalesce(rev_sms,0) rev_sms",
#                                 "coalesce(total_recharge, 0) as recharge_amount",
#                                 "coalesce(trx_recharge, 0) as recharge_transaction",
#                                 "coalesce(trx_sms,0) trx_sms",
#                                 "coalesce(trx_voice,0) trx_voice",
#                                 "coalesce(trx_digital_services,0) trx_digital_services",
#                                 "(((coalesce(vol_broadband,0))/1024)/1024) as payload_mb",
#                                 "(((coalesce(vol_broadband,0))/1024)/(1024*1024)) as payload_gb",
#                                 "((coalesce(dur_voice,0))/60) as mou",
#                                 "(((coalesce(games_payload,0))/1024)/1024) as games_payload_mb",
#                                 "(((coalesce(video_payload,0))/1024)/1024) as video_payload_mb",
#                                 "(((coalesce(youtube_payload,0))/1024)/1024) as youtube_payload_mb",
#                                 "(((coalesce(tiktok_payload,0))/1024)/1024) as tiktok_payload_mb",
#                                 "(((coalesce(maxstream_payload,0))/1024)/1024) as maxstream_payload_mb",
#                                 "case when c_msisdn is null then 'N' else'Y' end as mytsel_apps",
#                                 "case when c_msisdn is null then 0 else 1 end as mytsel_user",
#                                 "case when games_payload is null then 0 else 1 end as games_user",
#                                 "case when video_payload is null then 0 else 1 end as video_user",
#                                 "case when youtube_payload is null then 0 else 1 end as youtube_user",
#                                 "case when tiktok_payload is null then 0 else 1 end as tiktok_user",
#                                 "case when count_msisdn_netflix is null then 0 else 1 end as netflix_user",
#                                 "case when maxstream_payload is null then 0 else 1 end as maxstream_user",
#                                 "case when count_msisdn_viu is null then 0 else 1 end as viu_user",
#                                 "case when count_msisdn_indihome is null then 0 else 1 end as indihome_user",
#                                 "case when count_msisdn_firstmedia is null then 0 else 1 end as firstmedia_user",
#                                 "device_category",
#                                 "recharge_channel")

#     return res

# def summary_step2(table_a: pyspark.sql.DataFrame,
#                   table_b: pyspark.sql.DataFrame,
#                   table_c: pyspark.sql.DataFrame,
# #                   table_d: pyspark.sql.DataFrame,
#                   count_table:int,
#                   cols=None,how='left') -> pyspark.sql.DataFrame:

#     res_a = table_a.selectExpr("msisdn")

#     res_b = table_b.selectExpr("msisdn",
#                                "case when lower(brand) <> 'kartuhalo' and los_segment in ('02.<=6months','03.>6 months') and lower(multisim_eks) = 'y' then 1 else 0 END flag_b").filter("flag_b=1")

#     res_d = table_c.selectExpr("served_mobile_number as msisdn","case when coalesce(clm_revenue,0) > 1 then 1 else 0 END flag_d").filter("flag_d=1")
#     res_d = res_d.groupBy("msisdn").agg(F.count('msisdn').alias('count_msisdn_d'))

#     if count_table == 4:
#         res_c = table_d.selectExpr("msisdn","case when lower(multisim_int) = 'y' then 1 else 0 END flag_c").filter("flag_c=1")
#         res_c = res_c.groupBy("msisdn").agg(F.count('msisdn').alias('count_msisdn_c'))

#     else:
#         res_c = table_b.selectExpr("msisdn","case when lower(multisim_int) = 'y' then 1 else 0 END flag_c").filter("flag_c=1")
#         res_c = res_c.groupBy("msisdn").agg(F.count('msisdn').alias('count_msisdn_c'))

#     cols = ["msisdn"] if cols is None else cols
#     res_join = res_a.join(res_b,cols,how=how).join(res_c,cols,how=how).join(res_d,cols,how=how)
#     res = res_join.selectExpr("msisdn",
#                               "case when flag_b is null then 'N' else 'Y' end as external_multisim",
#                               "case when count_msisdn_c is null then 'N' else 'Y' end as internal_multisim",
#                               "case when count_msisdn_d is null then 0 else 1 end as DLS_user")

#     return res

# def summary_step3(table_a: pyspark.sql.DataFrame,table_b: pyspark.sql.DataFrame,table_c: pyspark.sql.DataFrame,
#                  table_d: pyspark.sql.DataFrame,table_e: pyspark.sql.DataFrame,cols=None,how='left') -> pyspark.sql.DataFrame:

#     res_a = table_a.selectExpr("msisdn")

#     res_b = table_b.selectExpr("msisdn","coalesce(payload,0) ott_payload").groupBy("msisdn").agg(F.sum('ott_payload').alias('ott_payload'))

#     res_c1 = table_c.selectExpr("msisdn","coalesce(payload_extrapolated,0) as fb_payload",
#                                 "case when lower(apps) in ('facebook') then 1 else 0 end as flag_fb").filter("flag_fb=1")\
#                                     .groupBy("msisdn").agg(F.sum('fb_payload').alias('fb_payload'))

#     res_c2 = table_c.selectExpr("msisdn","coalesce(payload_extrapolated,0) as payload_socnet_byte")\
#                                     .groupBy("msisdn").agg(F.sum('payload_socnet_byte').alias('payload_socnet_byte'))

#     res_d = table_d.selectExpr("msisdn").groupBy("msisdn").agg(F.count('msisdn').alias('count_msisdn_bcpfintech'))

#     res_e = table_e.selectExpr("msisdn").groupBy("msisdn").agg(F.count('msisdn').alias('count_msisdn_bcpbanking'))

#     cols = ["msisdn"] if cols is None else cols
#     res_join = res_a.join(res_b,cols,how=how).join(res_c1,cols,how=how).join(res_c2,cols,how=how)\
#                                 .join(res_d,cols,how=how).join(res_e,cols,how=how)

#     res = res_join.selectExpr("msisdn",
#                               "case when ott_payload is null then 0 else 1 end as OTT_user",
#                               "(((coalesce(ott_payload,0))/1024)/1024) as OTT_payload_mb",
#                               "case when fb_payload is null then 0 else 1 end as FB_user",
#                               "(((coalesce(fb_payload,0))/1024)/1024) as FB_payload_mb",
#                               "case when payload_socnet_byte is null then 0 else 1 end as socnet_user",
#                               "(((coalesce(payload_socnet_byte,0))/1024)/1024) as socnet_payload_mb",
#                               "case when count_msisdn_bcpfintech is null then 0 else 1 end as fintech_user",
#                               "case when count_msisdn_bcpbanking is null then 0 else 1 end as banking_user",
#                              )

#     return res

# def summary_step4(table_a: pyspark.sql.DataFrame,table_b: pyspark.sql.DataFrame,cols=None,how='left') -> pyspark.sql.DataFrame:

#     res_a = table_a.selectExpr("msisdn")
#     res_b1 = table_b.selectExpr("msisdn","rech as rech_balance","trx_rech as trx_rech_balance").filter("rech_type = 'rech_balance'")
#     res_b2 = table_b.selectExpr("msisdn","rech as rech_package","trx_rech as trx_rech_package").filter("rech_type = 'rech_package'")

#     cols = ["msisdn"] if cols is None else cols
#     res_join = res_a.join(res_b1,cols,how=how).join(res_b2,cols,how=how)

#     res = res_join.selectExpr("msisdn",
#                               "coalesce(rech_balance,0) as rech_balance",
#                               "coalesce(trx_rech_balance,0) as trx_rech_balance",
#                               "coalesce(rech_package,0) rech_package",
#                               "coalesce(trx_rech_package,0) trx_rech_package"
#                              )

#     return res

# def summary_step5(table_a: pyspark.sql.DataFrame,table_b: pyspark.sql.DataFrame,table_c: pyspark.sql.DataFrame,
#                   table_d: pyspark.sql.DataFrame,table_e: pyspark.sql.DataFrame,cols=None,how='left') -> pyspark.sql.DataFrame:

#     cols = ["msisdn"] if cols is None else cols
#     res = table_a.join(table_b,cols,how=how).join(table_c,cols,how=how).join(table_d,cols,how=how).join(table_e,cols,how=how)

#     return res

# def summary_step6(table_a: pyspark.sql.DataFrame,table_b: pyspark.sql.DataFrame,
#                   ses_network_dates:List[str],
#                   cols=None,how='left') -> pyspark.sql.DataFrame:

#     table_a = table_a.selectExpr("*")
#     table_b = table_b.selectExpr("lac_ci","throughput_total_kbps","3g_throughput_kbps","total_throughput_kbps",
#     "resource_block_utilizing_rate",f"case when trx_week between '{ses_network_dates[0]}' and '{ses_network_dates[1]}' then 1 else 0 end as flag").filter("flag=1").groupBy('lac_ci')\
#     .agg(F.avg('throughput_total_kbps').alias('throughput_total_kbps'),
#          F.avg('3g_throughput_kbps').alias('3g_throughput_kbps'),
#          F.avg('total_throughput_kbps').alias('total_throughput_kbps'),
#          F.avg('resource_block_utilizing_rate').alias('resource_block_utilizing_rate'))

#     cols = ["lac_ci"] if cols is None else cols
#     res = table_a.join(table_b,cols,how=how)
#     res = res.selectExpr("ses_segmentation","sub_segments_named as sub_segments",
#                          "case\
#                              when los_month <=3 then '0-3'\
#                              when 3 < los_month and los_month <=6 then '3-6'\
#                              when 6 < los_month and los_month <=12 then '6-12'\
#                              when 12 < los_month and los_month <=24 then '12-24'\
#                              else '>24'\
#                           end as segment_los_month",
#                          "case\
#                              when revenue <= 0 then 'zero'\
#                              when 0 < revenue and revenue <=5000 then '0-5'\
#                              when 5000 < revenue and revenue <=10000 then '5-10'\
#                              when 10000 < revenue and revenue <=15000 then '10-15'\
#                              when 15000 < revenue and revenue <=20000 then '15-20'\
#                              when 20000 < revenue and revenue <=30000 then '20-30'\
#                              when 30000 < revenue and revenue <=40000 then '30-40'\
#                              when 40000 < revenue and revenue <=50000 then '40-50'\
#                              when 50000 < revenue and revenue <=75000 then '50-75'\
#                              when 75000 < revenue and revenue <=100000 then '75-100'\
#                              when 100000 < revenue and revenue <=150000 then '100-150'\
#                              when 150000 < revenue and revenue <=200000 then '150-200'\
#                              else '>200K'\
#                           end as segment_arpu_k",
#                          "device_category",
#                          "case\
#                              when payload_gb <=1 then '0-1'\
#                              when 1 < payload_gb and payload_gb <=2 then '1-2'\
#                              when 2 < payload_gb and payload_gb <=3 then '2-3'\
#                              when 3 < payload_gb and payload_gb <=4 then '3-4'\
#                              when 4 < payload_gb and payload_gb <=5 then '4-5'\
#                              when 5 < payload_gb and payload_gb <=10 then '5-10'\
#                              when 10 < payload_gb and payload_gb <=15 then '10-15'\
#                              when 15 < payload_gb and payload_gb <=20 then '15-20'\
#                              when 20 < payload_gb and payload_gb <=30 then '20-30'\
#                              when 30 < payload_gb and payload_gb <=40 then '30-40'\
#                              when 40 < payload_gb and payload_gb <=50 then '40-50'\
#                              else '>50GB'\
#                           end as segment_Payload_GB",
#                          "internal_multisim","external_multisim","recharge_channel","mytsel_apps","status",
#                          "regional","city","msisdn as subs","los_month","revenue",
#                          "rev_data as revenue_data_total","rev_voice as revenue_voice_total","rev_sms as revenue_sms_total",
#                          "recharge_amount","recharge_transaction","rech_balance as recharge_balance",
#                          "trx_rech_balance as recharge_transaction_balance","rech_package as recharge_package",
#                          "trx_rech_package as recharge_transaction_package","RGB as RGB_Users","RGB_Data as RGB_Data_Users",
#                          "RGB_Voice as RGB_Voice_Users","RGB_DLS as RGB_DLS_Users","mytsel_user as mytsel_apps_user",
#                          "case when lower(data_flag)='y' then 1 else 0 end as Data_Users",
#                          "case when mou>0 or trx_voice>0 then 1 else 0 end as Voice_Users",
#                          "DLS_user",
#                          "case when lower(data_flag)='y' and payload_mb > 0 then 1 else 0 end as payload_Users",
#                          "case when device_type = 'SMARTPHONE' then 1 else 0 end as smarthphone_Users",
#                          "case when lower(data_sdn_flag)='y' then 1 else 0 end as 4GSDn_Users",
#                          "case when cust_segment_subtype='HVC'  then 1 else 0 end as hvc_Users",
#                          "games_user","video_user","tiktok_user","netflix_user","indihome_user","firstmedia_user",
#                          "maxstream_user","youtube_user","OTT_user","FB_user","socnet_user","fintech_user",
#                          "banking_user","viu_user","games_payload_mb","video_payload_mb","youtube_payload_mb",
#                          "tiktok_payload_mb","maxstream_payload_mb","FB_payload_mb","OTT_payload_mb",
#                          "socnet_payload_mb","payload_mb","mou","trx_sms",
#                          "coalesce(throughput_total_kbps,0) as 2g_throughput",
#                          "coalesce(3g_throughput_kbps,0) as 3g_throughput",
#                          "coalesce(total_throughput_kbps,0) as 4g_throughput",
#                          "coalesce(resource_block_utilizing_rate,0) resource_block_utilizing_rate")
#     return res
