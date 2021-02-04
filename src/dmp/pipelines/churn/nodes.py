import logging
from functools import reduce
from operator import itemgetter
from typing import Dict, List

import pai
import pandas as pd
import pyspark.sql
import pyspark.sql.functions as f
from category_encoders import CountEncoder
from pyspark.sql import SparkSession, Window
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import StandardScaler

from src.dmp.pipelines.churn.consts import (
    MSISDN_COL,
    TARGET_PROPOSITION_CAP,
    TARGET_PROPOSITION_COL,
    WEEKSTART_COL,
)
from src.dmp.pipelines.model_factory.propensity_model_nodes import ColumnChecker


def _create_target_proposition(
    df: pyspark.sql.DataFrame, source_columns: List[str]
) -> pyspark.sql.DataFrame:
    """
    Add target proposition to the table. Source columns are assumed to be 'weeks since last activity' type.
    The resulting column is a minimum of source columns. Values greater than TARGET_PROPOSITION_CAP are dropped.

    :param df: Input DataFrame
    :param source_columns: List of column to use
    :returns: DataFrame with TARGET_PROPOSITION column
    """
    if len(source_columns) > 1:
        target_proposition = f.least(
            *[f.col(source_column) for source_column in source_columns]
        )
    else:
        target_proposition = f.col(source_columns[0])
    return df.withColumn(TARGET_PROPOSITION_COL, target_proposition)


def _join_inactivity_period(
    df: pyspark.sql.DataFrame, weekstart_column: str, msisdn_column: str,
) -> pyspark.sql.DataFrame:
    """
    Uses the inactivity features to define inactivity period table used for statistics calculation.

    :param msisdn_column: Name of msisdn column
    :param weekstart_column: Name of column with start of the week
    :param df: Input DataFrame
    :returns: Table with length of inactivity period and flag deactivation flag
    """
    # Pick only weeks when user was active or the deactivation week itself
    deactivation_col = "deactivation_flag"
    inactivity_length_weeks = "inactivity_length_weeks"
    msisdn_partition = Window.partitionBy(msisdn_column).orderBy(weekstart_column)
    # Make sure every user has one deactivation date
    df = (
        df.withColumn(
            "deactivation_date", f.max("deactivation_date").over(msisdn_partition)
        )
        .filter("deactivation_date is not null")
        .select(
            "deactivation_date", weekstart_column, msisdn_column, TARGET_PROPOSITION_COL
        )
    )
    deactivation_date_week_later = f.date_add(f.col("deactivation_date"), 7)
    deactivated = (
        df.filter(f"{weekstart_column} >= deactivation_date")
        .filter(f.col(weekstart_column) < deactivation_date_week_later)
        .withColumn(deactivation_col, f.lit(1))
    )
    active = df.filter(f"deactivation_date < {weekstart_column}").withColumn(
        deactivation_col, f.lit(0)
    )
    df_filtered = active.unionByName(deactivated)

    # Create inactivity period id
    inactivity_col = f.col(TARGET_PROPOSITION_COL)
    same_inactivity_period = (
        inactivity_col == f.lag(inactivity_col, default=0).over(msisdn_partition) + 1
    ) | (inactivity_col > f.lit(TARGET_PROPOSITION_CAP))
    different_inactivity_period = f.when(same_inactivity_period, 0).otherwise(1)
    inactivity_period_id = f.sum(different_inactivity_period).over(msisdn_partition)
    user_inactivity_partition = Window.partitionBy(
        f.col(msisdn_column), inactivity_period_id
    )

    # Create inactivity length
    inactivity_length = f.when(inactivity_col == 0, 0).otherwise(
        f.count("weekstart").over(user_inactivity_partition)
    )

    # Create output table
    cross_table = (
        df_filtered.withColumn(inactivity_length_weeks, inactivity_length)
        .groupby([deactivation_col, inactivity_length_weeks])
        .agg(f.count("*").alias("cases_observed"))
    )
    this_and_following_inactivity_length = (
        Window.partitionBy(deactivation_col)
        .orderBy(inactivity_length_weeks)
        .rowsBetween(0, Window.unboundedFollowing)
    )
    cross_table = cross_table.withColumn(
        "cases_observed_for_this_and_longer_inactivity_lengths",
        f.sum(f.col("cases_observed")).over(this_and_following_inactivity_length),
    )
    return cross_table.toPandas()


def generate_target_calibration_cross_table(
    df: pyspark.sql.DataFrame,
    source_columns: List[List[str]],
    weekstart_column: str,
    msisdn_column: str,
) -> None:
    """
    Generates target calibration plots. Saves them to pai.

    :param msisdn_column: Name of msisdn column
    :param weekstart_column: Name of column with start of the week
    :param df: Input DataFrame
    :param source_columns: List of groups of columns to base target propositions on
    """
    for source_column_group in source_columns:
        source_column_group_name = "_".join(source_column_group)[0:160]
        logging.info(f"Building target table for {source_column_group_name}")

        df_extended = _create_target_proposition(df, source_column_group)
        inactivity_cross_table = _join_inactivity_period(
            df_extended, weekstart_column, msisdn_column
        )
        pai.log_artifacts(
            {
                f"target_calibration_table_{source_column_group_name}": inactivity_cross_table
            }
        )


def join_features_for_pilot_bid_assignment(
    msisdns_scores: pyspark.sql.DataFrame,
    df_master: pyspark.sql.DataFrame,
    msisdn_column: str,
    date_to_pick: str,
    weekstart_column: str,
) -> pyspark.sql.DataFrame:
    """
    Joins the tables that contain information needed for `nuclear pilot` bid assignment
    The output table include:
    - msisdn
    - score
    - arpu
    - data usage

    :param weekstart_column: Name of column with start of the week
    :param date_to_pick: Weekstart for which features would be taken
    :param msisdns_scores: Table with msisdn and propensity score
    :param df_master: Master DataFrame with features needed, preferably scoring table
    :param msisdn_column: Name of msisdn column
    :return: Table with features needed for bid assignment
    """
    revenue_col_name = "fea_rev_tot_sum_01m"
    data_col_name = "fea_rev_data_tot_sum_01m"
    features_to_pick = [
        revenue_col_name,
        data_col_name,
        msisdn_column,
        weekstart_column,
    ]

    revenue_coalesced = f.when(f.col(revenue_col_name).isNull(), 0).otherwise(
        f.col(revenue_col_name)
    )
    data_usage_coalesced = f.when(f.col(data_col_name).isNull(), 0).otherwise(
        f.col(data_col_name)
    )

    is_rgb = f.when(revenue_coalesced <= 0, 0).otherwise(1)
    is_du = f.when(data_usage_coalesced <= 0, 0).otherwise(1)

    in_du_rgb_partition = Window.partitionBy(is_du, is_rgb)
    score_percentile_in_du_rgb_group = f.percent_rank().over(
        in_du_rgb_partition.orderBy("score")
    )

    arpu_thresholds = [0, 25_000, 50_000, 100_000]
    arpu_bin_whens = [
        f.when(revenue_coalesced < arpu_threshold, f"below_{arpu_threshold}")
        for arpu_threshold in arpu_thresholds
    ]
    arpu_bin = f.coalesce(*arpu_bin_whens, f.lit("over_100000"))

    msisdns_scores_df = SparkSession.builder.getOrCreate().createDataFrame(
        msisdns_scores
    )
    df = (
        df_master.select(*features_to_pick)
        .filter(f"{weekstart_column} == '{date_to_pick}'")
        .join(msisdns_scores_df, on=msisdn_column)
        .withColumn("is_rgb", is_rgb)
        .withColumn("is_du", is_du)
        .withColumn(
            "score_percentile_in_du_rgb_group", score_percentile_in_du_rgb_group
        )
        .withColumn("arpu_bin", arpu_bin)
    ).toPandas()
    return df


def _assign_random_value_from_list(
    df: pyspark.sql.DataFrame, new_col_name: str, values: List
) -> pyspark.sql.DataFrame:
    """
    Assigns a random new column using values provided

    :param df: Table to augment with new column
    :param new_col_name: Name of newly created column
    :param values: Values to pick at random
    :returns: Table df with new column
    """
    n = len(values)
    temp_col_name = "temp"
    values_whens = [
        f.when(f.col(temp_col_name) == i, f.lit(values[i])) for i in range(0, n)
    ]
    return (
        df.withColumn(temp_col_name, f.floor(f.rand() * n))
        .withColumn(new_col_name, f.coalesce(*values_whens))
        .drop(temp_col_name)
    )


def assign_bids_for_nuclear_pilot(
    df: pd.DataFrame, nuclear_pilot_configuration: Dict, msisdn_column: str
) -> pd.DataFrame:
    """
    Assigns bids needed for nuclear pilot

    :param df: Input DataFrame with scores and needed features
    :param nuclear_pilot_configuration: Dictionary describing how to assing bids
    :param msisdn_column: Name of msisdn column
    :return: Table with msisdns and bids assigned
    """
    score_col = "score"
    score_order = Window.orderBy(f.col(score_col).desc())
    # Convert to pyspark DataFrame
    pdf = SparkSession.builder.getOrCreate().createDataFrame(df)

    # Assign users to bids
    bids_groups = nuclear_pilot_configuration["bids_groups"]
    users_groups = nuclear_pilot_configuration["users_groups"]
    users_bids = []
    for group_id in users_groups:
        # Unpack configuration
        (
            filter_str,
            bid_group_chosen,
            from_model_group_size,
            random_group_size,
        ) = itemgetter(
            "filter", "bid_group", "from_model_group_size", "random_group_size",
        )(
            users_groups[group_id]
        )
        applicable_users = pdf.filter(filter_str).withColumn(
            "score_rn", f.row_number().over(score_order)
        )
        # Assign high scorers
        top_scorers = (
            applicable_users.filter(f"score_rn <= {from_model_group_size}")
            .select(msisdn_column)
            .withColumn("from_model", f.lit(1))
        )
        # Assign randomized group
        random_group = (
            applicable_users.filter(f"score_rn > {from_model_group_size}")
            .withColumn("r", f.rand())
            .withColumn("random_rn", f.row_number().over(Window.orderBy("r")))
            .filter(f"random_rn < {random_group_size}")
            .select(msisdn_column)
            .withColumn("from_model", f.lit(0))
        )
        # Join the groups and pick bid
        bids = bids_groups[bid_group_chosen]
        users_combined = top_scorers.unionByName(random_group).withColumn(
            "group_id", f.lit(group_id)
        )
        users_with_bid = _assign_random_value_from_list(users_combined, "bid", bids)
        users_bids.append(users_with_bid)
    return reduce(lambda df1, df2: df1.unionByName(df2), users_bids).toPandas()


def format_pilot_assignment(
    pilot_bids: pd.DataFrame, bids_description: pd.DataFrame,
) -> pd.DataFrame:
    """
    Formats the msisdn and bids table to include all columns needed

    :param pilot_bids: Msisdn to bid mapping
    :param bids_description: Table with descriptions of each bid used
    """
    pilot_bids_cols_to_pick = ["msisdn", "from_model", "bid"]
    return pd.merge(pilot_bids[pilot_bids_cols_to_pick], bids_description, on="bid")


def prepare_data_for_clustering(
    features: pyspark.sql.DataFrame,
    msisdns_chosen: pd.DataFrame,
    clustering_params: Dict,
) -> pd.DataFrame:
    """
    Prepares data for clustering. It sources the features from `features` table
    and chooses the msisdns from scores table.

    :param features: Table with all features needed
    :param msisdns_chosen: Table with chosen msisdns, will use `msisdn` column
    :param clustering_params: Parameters describing the clustering process
    :return: DataFrame with chosen msisdns and their features
    """
    clustering_date, clustering_features, features_to_summarize = itemgetter(
        "clustering_date", "clustering_features", "features_to_summarize"
    )(clustering_params)
    cols_to_pick = list(set(clustering_features + features_to_summarize))
    msisdns_picked = (
        SparkSession.builder.getOrCreate()
        .createDataFrame(msisdns_chosen)
        .select(MSISDN_COL)
        .distinct()
    )
    return (
        features.filter(f"{WEEKSTART_COL} == '{clustering_date}'")
        .join(msisdns_picked, on="msisdn")
        .select(*cols_to_pick)
        .toPandas()
    )


def fit_clustering_pipeline(df: pd.DataFrame, clustering_params: Dict,) -> Pipeline:
    """
    Creates preprocessing / clustering pipeline, fits it to data provided and saves the Pipeline to PAI.

    :param df: Input DataFrame with features needed
    :param clustering_params: Parameters describing the clustering process
    :returns: Sklearn pipeline for preprocessing and clustering
    """
    columns_to_pick, pca_components, n_clusters = itemgetter(
        "clustering_features", "pca_components", "n_clusters"
    )(clustering_params)
    clustering_pipeline = make_pipeline(
        ColumnChecker(columns_to_pick),
        CountEncoder(),
        StandardScaler(),
        SimpleImputer(strategy="mean"),
        PCA(n_components=pca_components),
        KMeans(n_clusters=n_clusters),
    )
    pipeline_fitted = clustering_pipeline.fit(df)
    pai.log_artifacts({"clustering_pipeline": pipeline_fitted})
    return pipeline_fitted


def assign_cluster_to_features(
    features: pd.DataFrame, clustering_pipeline: Pipeline
) -> pd.DataFrame:
    """
    Uses fitted clustering pipeline to add `cluster_id` column to features.

    :param features: DataFrame to add `cluster_id` to
    :param clustering_pipeline: Fitted clustering pipeline
    :returns: `features` table with `cluster_id` column added
    """
    cluster_ids = clustering_pipeline.predict(features)
    features["cluster_id"] = cluster_ids
    return features


def summarize_clusters(
    features: pd.DataFrame, clustering_params: Dict,
) -> pd.DataFrame:
    """
    Summarize the clusters

    :param features: Table with features and `cluster_id` column
    :param clustering_params: Parameters describing the clustering process
    :returns: Table with clusters summarized
    """
    features_to_summarize = clustering_params["features_to_summarize"]
    summary = (
        features.groupby("cluster_id")[features_to_summarize]
        .agg(["mean", "count", "median"])
        .reset_index()
    )
    summary.columns = ["_".join(col).strip() for col in summary.columns]
    return summary
