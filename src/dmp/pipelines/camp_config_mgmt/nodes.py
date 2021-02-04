import logging
from datetime import datetime as dt
from typing import List, Tuple

import pandas as pd
import pyspark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from src.dmp.pipelines.aggregator.api import insert_file_record
from src.dmp.pipelines.camp_config_mgmt.api import (
    insert_temp_keywords_into_table,
    update_cm_generated_keyword_agg_job_id,
)
from src.dmp.pipelines.camp_config_mgmt.config_validation_rules import (
    identify_partial_missing_configuration,
)
from src.dmp.pipelines.core.db import db_connection, db_session
from src.dmp.pipelines.core.entity import CampaignKeyword, FileRecord

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def today_str() -> str:
    return datetime.today().strftime("%Y%m%d%H%M%S")


def extract_campaign_name_family_from_score_table(
    score_data: pyspark.sql.DataFrame,
) -> Tuple[pd.DataFrame, pyspark.sql.DataFrame]:
    """
    The function loads and extracts a dataframe of unique campaign name and family from the score table.
    It also checks if a single unique dest_sys is specified in the score table

    Args:
        score_data (DataFrame): A spark DataFrame Score table

    Returns:
        Tuple[pd.DataFrame, DataFrame]
    """

    campaign_name_family_df = (
        score_data.select("campaign_name", "campaign_family").distinct().toPandas()
    )

    assert (
        score_data.select("dest_sys").distinct().count() == 1
    ), "Score table consists more than 1 dest_sys"

    return campaign_name_family_df, score_data


def load_validate_config_tables(
    campaign_name_family_df: pd.DataFrame,
) -> Tuple[pyspark.sql.DataFrame, pd.DataFrame, pyspark.sql.DataFrame]:
    """
    The function loads and validates configuration tables with a dataframe of unique campaign name and family

    Args:
        campaign_name_family_df (DataFrame): A dataframe of unique campaign name and family

    Returns:
        Tuple[DataFrame, pd.DataFrame, DataFrame]
    """

    campaign_config = pd.read_sql(
        f"SELECT config_id, campaign_family, \
        campaign_name, push_channel, dest_sys \
        FROM cm_campaign_config where status = 'active'",
        db_connection,
    )

    campaign_config = pd.merge(
        campaign_config,
        campaign_name_family_df,
        on=["campaign_name", "campaign_family"],
        how="inner",
    )

    prioritization_rank = pd.read_sql(
        "SELECT p_rank_id, p_rank_batch_id, \
        campaign_family, campaign_rank \
        FROM cm_prioritization_rank where status = 'active'",
        db_connection,
    )

    prioritization_criteria = pd.read_sql(
        "SELECT p_criteria_id, p_rank_id, \
        criteria_rank, criteria_desc \
        FROM cm_prioritization_criteria where status = 'active'",
        db_connection,
    )

    if (len(campaign_config)) == 0:
        logger.error("No active campaign_names found in database")
        exit()

    if (len(prioritization_rank)) == 0:
        logger.error("No active rank config found in database")
        exit()

    if (len(prioritization_criteria)) == 0:
        logger.error("No active criteria config found in database")
        exit()

    try:
        identify_partial_missing_configuration(
            campaign_name_family_df, prioritization_rank, campaign_config
        )

        logger.info(f"Validation pass")

        spark = SparkSession.builder.getOrCreate()

        prioritization_rank = spark.createDataFrame(prioritization_rank)
        campaign_config = spark.createDataFrame(campaign_config)

        return (
            prioritization_rank,
            prioritization_criteria,
            campaign_config,
        )

    except Exception as e:
        logger.error(f"Validation fails: {e}")
        exit()


def prepare_agg_input(
    score_data: pyspark.sql.DataFrame,
    prioritization_rank: pyspark.sql.DataFrame,
    prioritization_criteria: pd.DataFrame,
    campaign_config: pyspark.sql.DataFrame,
    output_path: str,
) -> None:
    """
    The function prepares aggregator input dataset by combing score and configuration tables.
    It saves the aggregator files (for each campaign family present) to hdfs and
    insert the file paths to table

    Args:
        score_data (DataFrame): Score table,
        prioritization_rank (DataFrame): Prioritization rank table,
        prioritization_criteria (pd.DataFrame): Prioritization criteria table
        campaign_config (DataFrame): Table of active campaign names and config (e.g campaign family, channel, system)
        output_path : path to save aggregator input

    Returns:
        None
    """
    today = today_str()

    score_data = score_data.join(
        campaign_config, on=["campaign_name", "campaign_family", "dest_sys"], how="left"
    ).join(
        prioritization_rank.select(
            ["campaign_family", "campaign_rank", "p_rank_id", "p_rank_batch_id"]
        ),
        on=["campaign_family"],
        how="left",
    )

    dest_sys = [
        row.dest_sys for row in score_data.select("dest_sys").distinct().collect()
    ][0]

    logger.info(f"Processing for system: {dest_sys}")

    if dest_sys == "cms":
        score_data = score_data.withColumn(
            "keyword",
            f.concat(
                f.lit(today),
                f.lit("_"),
                f.col("campaign_family"),
                f.lit("_"),
                f.col("campaign_name"),
            ),
        )

    elif dest_sys == "pes":
        score_data = score_data.withColumn(
            "keyword",
            f.concat(
                f.lit(today),
                f.lit("_"),
                f.col("campaign_family"),
                f.lit("_"),
                f.col("campaign_name"),
                f.lit("_"),
                f.col("push_channel"),
            ),
        )
    else:
        raise RuntimeError(f"Unexpected dest_sys : {dest_sys}")

    unique_campaign_family_p_rank = [
        [row.campaign_family, row.p_rank_id]
        for row in score_data.select("campaign_family", "p_rank_id")
        .distinct()
        .collect()
    ]

    agg_input_columns = [
        "msisdn",
        "keyword",
        "push_channel",
        "criteria_1",
        "criteria_2",
        "criteria_3",
        "criteria_4",
        "criteria_5",
    ]

    for campaign_family, p_rank_id in unique_campaign_family_p_rank:
        logger.info(f"Processing for {campaign_family}, {p_rank_id}")

        score_df = score_data.filter(f.col("p_rank_id") == int(p_rank_id))

        campaign_criteria = prioritization_criteria[
            prioritization_criteria["p_rank_id"] == int(p_rank_id)
        ][["criteria_rank", "criteria_desc"]]
        criteria_desc_list = [
            row
            for _, row in campaign_criteria[
                ["criteria_rank", "criteria_desc"]
            ].iterrows()
        ]

        for i in range(0, len(criteria_desc_list)):
            if criteria_desc_list[i].criteria_desc is None:
                logger.info(f"lit(0) as criteria_{criteria_desc_list[i].criteria_rank}")
                score_df = score_df.withColumn(
                    f"criteria_{criteria_desc_list[i].criteria_rank}", f.lit(0)
                )
            else:
                logger.info(
                    f"{criteria_desc_list[i].criteria_desc} as criteria_{criteria_desc_list[i].criteria_rank}"
                )
                score_df = score_df.withColumn(
                    f"criteria_{criteria_desc_list[i].criteria_rank}",
                    f.col(f"{criteria_desc_list[i].criteria_desc}"),
                )

        unique_temp_keyword_with_config = (
            score_df.select(["keyword", "config_id", "p_rank_id", "p_rank_batch_id"])
            .distinct()
            .toPandas()
        )

        insert_temp_keywords_into_table(unique_temp_keyword_with_config, db_session)

        logger.info(
            f"Inserted unique keywords and config ids to cm_generated_keyword successfully for {campaign_family}"
        )

        temp_keywords = unique_temp_keyword_with_config["keyword"].unique()
        temp_keyword_ids = [
            row.temp_keyword_id
            for row in db_session.query(CampaignKeyword).filter(
                CampaignKeyword.temp_keyword.in_(temp_keywords)
            )
        ]
        logger.info(f"Extracted temp_keyword_ids created for {campaign_family}")

        output_data = score_df.select(*agg_input_columns)

        save_agg_input_insert_db(output_data, dest_sys, temp_keyword_ids, output_path)


def save_agg_input_insert_db(
    agg_input: pyspark.sql.DataFrame,
    dest_sys: str,
    temp_ids: List[str],
    output_path: str,
) -> None:
    """
    The function saves aggregator input dataset and insert file path to the table

    Args:
        agg_input: Aggregator input table
        dest_sys: System identified
        temp_ids: List of ids to identify temporary keyword generated in this run
        output_path: File path for saving and to be inserted into DB

    Returns:
        None
    """

    today = today_str()
    # TODO : decide file_name, file_path per use case
    agg_input.write.mode("overwrite").parquet(
        f"{output_path}/{today}/{today}_config_mgmt.parquet"
    )
    logger.info(f"Saved file to HDFS successfully")
    insert_file_record(
        db_session,
        FileRecord(
            file_name=f"{today}_config_mgmt.parquet",
            file_path=f"{output_path}/{today}/",
            status="active",
            engine=dest_sys,
            created_at=dt.utcnow(),
            updated_at=dt.utcnow(),
            temp_keyword_ids=temp_ids,
        ),
    )
    logger.info(f"Inserted file path to Aggregator db successfully")
    agg_job_id = [
        row.id
        for row in db_session.query(FileRecord)
        .filter(FileRecord.temp_keyword_ids == temp_ids)
        .all()
    ]
    agg_job_id = agg_job_id[0]
    logger.info(f"Extracted agg_job_id for this run: {agg_job_id}")

    update_cm_generated_keyword_agg_job_id(
        db_session, agg_job_id=agg_job_id, temp_keyword_ids=temp_ids
    )
    logger.info(
        f"Updated agg_job_id to cm_generated_keyword for this run with : {agg_job_id}"
    )
