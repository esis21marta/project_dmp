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

from typing import Dict, List

import numpy as np
import pandas as pd
import pyspark
from pyspark import SparkContext, SQLContext
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import broadcast
from sklearn.cluster import KMeans


# kmeans clustering
def kmeans_clustering_fit(
    X_pca: pd.DataFrame,
    _4g_clustering_parameter: Dict[str, str],
    max_rows: int = int(1e5),
):
    km_pca = KMeans(
        n_clusters=_4g_clustering_parameter["n_clusters"],
        n_init=_4g_clustering_parameter["n_init_clusters"],
        max_iter=_4g_clustering_parameter["max_iter_clusters"],
        n_jobs=_4g_clustering_parameter["n_jobs_clusters"],
        random_state=_4g_clustering_parameter["random_state_clustering"],
    )
    # pca
    print(X_pca.shape)
    if len(X_pca) > max_rows:
        print("-----")
        np.random.seed(42)
        random_rows = np.random.choice(len(X_pca), max_rows, False)
        X_pca = X_pca.loc[random_rows, :]
        print(len(X_pca))
        km_pca.fit(X_pca)
    else:
        km_pca.fit(X_pca)

    return km_pca


def kmeans_clustering_predict(X: pd.DataFrame, km: KMeans) -> pd.DataFrame:
    pred = km.predict(X)
    X["segment_clustering"] = pred
    return X


def joining_kmeans_result_and_original_table(
    _4g_pca_table_and_clusters: pd.DataFrame,
    _4g_local_pandas_master_table: pd.DataFrame,
) -> pd.DataFrame:
    """
    joining the original pandas table (= unit of analysis + features used in the analysis),
    to the result of the clustering (PCs + segments)
    :param _4g_pca_table_and_clusters: result of analysis
    :param _4g_local_pandas_master_table: original master table in local (contains unit of analysis)
    :return:
    """
    assert len(_4g_pca_table_and_clusters) == len(_4g_local_pandas_master_table)
    assert (
        _4g_pca_table_and_clusters.index == _4g_local_pandas_master_table.index
    ).all()
    res = pd.concat([_4g_pca_table_and_clusters, _4g_local_pandas_master_table], axis=1)
    res.columns = [str(c) for c in res.columns]

    return res


def to_hdfs(
    df_pandas: pd.DataFrame,
    cols_to_select: List[str] = ("msisdn", "segment_clustering"),
) -> pyspark.sql.DataFrame:
    sc = SparkContext.getOrCreate()
    spark = SQLContext(sc)
    df_spark = spark.createDataFrame(df_pandas.loc[:, cols_to_select])
    df_spark.createOrReplaceTempView("baseline_temporer")
    res = spark.sql(
        """
    select msisdn, segment_clustering,
    case
        when segment_clustering = 0 then 'Really bad network, Call you'
        when segment_clustering = 1 then 'Grab & Go Multisimers'
        when segment_clustering = 2 then 'Forced to Lapse due to Network Quality'
        when segment_clustering = 3 then 'Price Sensitive (Churning)'
        when segment_clustering = 4 then 'Churning due to network issues'
        when segment_clustering = 5 then 'Loyal lower HVC'
        when segment_clustering = 6 then 'Telkomsel for Legacy Only, network with other carrier'
        when segment_clustering = 7 then 'Price Sensitive (Churning) (2)'
        when segment_clustering = 8 then 'Loyal HVC with bad 4G Network experience'
        when segment_clustering = 9 then 'HVC 1'
        when segment_clustering = 10 then 'Churning due to network issues (2)'
        when segment_clustering = 11 then '3G Student Boys'
        when segment_clustering = 12 then 'HVC 2'
        when segment_clustering = 13 then 'Path to Churn v2 Network Quality'
        when segment_clustering = 14 then 'Grab & Go Excellent Network'
        when segment_clustering = 15 then 'Network Quality Issues Lapers'
        when segment_clustering = 16 then 'Heavy Voice Users'
        when segment_clustering = 17 then 'HVC 3'
        when segment_clustering = 18 then 'HVC small'
        when segment_clustering = 19 then 'HVC comfortable with 3G'
        else 'unknown'
    end as map_cluster_new,
    case
        when segment_clustering = 0 then 'Voice'
        when segment_clustering = 1 then 'Churners'
        when segment_clustering = 2 then 'HVC'
        when segment_clustering = 3 then 'Churners'
        when segment_clustering = 4 then 'Churners'
        when segment_clustering = 5 then 'HVC'
        when segment_clustering = 6 then 'Voice'
        when segment_clustering = 7 then 'Churners'
        when segment_clustering = 8 then 'HVC'
        when segment_clustering = 9 then 'HVC'
        when segment_clustering = 10 then 'Churners'
        when segment_clustering = 11 then 'HVC'
        when segment_clustering = 12 then 'HVC'
        when segment_clustering = 13 then 'Churners'
        when segment_clustering = 14 then 'Churners'
        when segment_clustering = 15 then 'Churners'
        when segment_clustering = 16 then 'Voice'
        when segment_clustering = 17 then 'HVC'
        when segment_clustering = 18 then 'HVC'
        when segment_clustering = 19 then 'HVC'
        else 'unknown'
    end as type
    from baseline_temporer
    """
    )
    del df_spark

    return res


def joining_with_complete_master_table(
    df_kmeans_pca_features_and_msisdns: pyspark.sql.DataFrame,
    complete_master_table: pyspark.sql.DataFrame,
    cols_to_select: List[str] = ("msisdn", "segment_clustering"),
) -> pyspark.sql.DataFrame:
    """
    Joining the result of the clustering (table with PCs, segments used, unit of analysis,
    features used in the analysis) and main table (table with all features)
    :param df_kmeans_pca_features_and_msisdns:
    :param complete_master_table:
    :param cols_to_select:
    :return:
    """
    df = df_kmeans_pca_features_and_msisdns.select(list(cols_to_select))
    res = complete_master_table.join(broadcast(df), ["msisdn"])
    return res


def downsample(df: pyspark.sql.DataFrame, n: int) -> pyspark.sql.DataFrame:
    """
    :param df: pyspark dataframe
    :param n: number of rows in the end (not a fraction)
    :return:
    """
    count = df.count()
    if n < count:
        fraction = n / count
        res = df.sample(False, fraction, 42)
        return res
    else:
        return df


def run_statistics_profile(
    df: pd.DataFrame,
    path_to_save_summary,
    segment_name: str = "segment_clustering",
    imputation_method: str = "zero",
) -> pd.DataFrame:
    """
    Create statistics for each feature and cluster
    The statistics are :
        - mean
        - median
        - quantiles (20% and 80%)

    :param df:
    :param segment_name:
    :param inmputation_method:
    :return:
    """
    # imputation
    for cols in df.loc[
        :, [col for col, ty in df.dtypes.items() if ty == "O"]
    ].columns.tolist():
        df.loc[:, cols] = df.loc[:, cols].fillna("unknown").astype("str")
    # applying imputation method
    assert imputation_method in ("zero", "mean", "none")
    df_subset = {"zero": df.fillna(0), "mean": df.fillna(df.mean()), "none": df}[
        imputation_method
    ]

    means = df_subset.groupby([segment_name]).mean()
    sizes = (
        df_subset[[segment_name, "msisdn"]]
        .groupby([segment_name])
        .count()
        .reset_index()
    )
    summary = pd.merge(sizes, means, on=[segment_name])

    medians = df_subset.groupby([segment_name]).median().reset_index()
    summary = pd.merge(
        summary, medians, on=[segment_name], suffixes=["_mean", "_median"]
    ).reset_index()

    obj_cols = []
    for col in df_subset.columns.tolist():
        if df_subset[col].dtype in (
            ["float32", "float64", "uint8", "int8", "int32", "int64"]
        ):
            obj_cols.append(col)

    counts = (
        df_subset[
            [segment_name]
            + [
                "fea_custprof_nik_gender_MALE",
                "fea_custprof_nik_gender_FEMALE",
                "os_ANDROID",
                "os_BLACKBERRY",
                "os_BLACKBERRY_10",
                "os_IOS",
                "os_OTHER_OS",
                "os_SYMBIAN",
                "os_UNIDENTIFIED",
                "os_WINDOWS",
                "multisim_Y",
                "brand_KartuAS",
                "brand_LOOP",
                "brand_kartuHALO",
                "brand_simPATI",
            ]
        ]
        .groupby([segment_name])
        .sum()
        .reset_index()
    )
    summary = pd.merge(summary, counts, on=[segment_name])

    num_cols = []
    for col in df_subset.columns.tolist():
        if df_subset[col].dtype in (
            ["float32", "float64", "uint8", "int8", "int32", "int64"]
        ):
            num_cols.append(col)

    quantile80 = df_subset[num_cols].groupby([segment_name]).quantile(0.80)
    quan_cols = [col + "_80percentile" for col in quantile80.columns.tolist()]
    quantile80.columns = quan_cols
    quantile80 = quantile80.rename(
        columns={"cluster_80percentile": "cluster"}
    ).reset_index()

    quantile20 = df_subset[num_cols].groupby([segment_name]).quantile(0.20)
    quan_cols = [col + "_20percentile" for col in quantile20.columns.tolist()]
    quantile20.columns = quan_cols
    quantile20 = quantile20.rename(
        columns={"cluster_20percentile": "cluster"}
    ).reset_index()

    summary = pd.merge(summary, quantile80, on=[segment_name])
    summary = pd.merge(summary, quantile20, on=[segment_name])

    summary = summary.drop("index", axis=1)
    summary = summary.rename(columns={segment_name: "segment_clustering"})
    summary = summary[ref]
    summary.to_csv(path_to_save_summary)

    return summary


def concatenate_results_of_clustering(
    *X: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    if len(X) == 0:
        raise NotImplementedError()
    res = X[0]
    for el in X[1:]:
        res = res.union(el)
    return res
