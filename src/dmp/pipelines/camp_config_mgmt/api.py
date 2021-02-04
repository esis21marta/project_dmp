import logging
from datetime import datetime as dt
from typing import List

import pandas as pd
from sqlalchemy.orm.session import Session

from src.dmp.pipelines.core.entity import CampaignKeyword

logging.basicConfig(level=logging.INFO)
logger = logging.Logger(__name__)


def insert_temp_keywords_into_table(df: pd.DataFrame, session: Session):
    """
    The function is the helper function used for inserting a table of unique keywords
    and their configuration to the table
    Args:
        df (pd.DataFrame): A dataframe of unique keywords and their configuration used in the run
        session (Session): The SQLAlchemy db session.
        
    Returns:

    """
    dicts = [
        dict(
            created_at=dt.utcnow(),
            updated_at=dt.utcnow(),
            temp_keyword=row["keyword"],
            config_id=row["config_id"],
            p_rank_id=row["p_rank_id"],
            p_rank_batch_id=row["p_rank_batch_id"],
        )
        for _, row in df.iterrows()
    ]

    session.bulk_insert_mappings(CampaignKeyword, dicts)
    session.commit()
    logger.info(f"Updated cm_generated_keyword table successfully")


def update_cm_generated_keyword_agg_job_id(
    session: Session, agg_job_id: int, temp_keyword_ids: List[int] = None
):
    """
    The function is the helper function used for updating agg_job_id of temporary keywords
    present in table `cm_generated_keyword`
    Args:
        session (Session): The SQLAlchemy db session.
        temp_keyword_ids (List[int]): List of temp_keyword ids
        agg_job_id (int): The job id which represents the job file inserted to aggregator
        
    Returns:

    """
    session.query(CampaignKeyword).filter(
        CampaignKeyword.temp_keyword_id.in_(temp_keyword_ids)
    ).update(
        {CampaignKeyword.agg_job_id: agg_job_id, CampaignKeyword.updated_at: dt.now()},
        synchronize_session=False,
    )
    session.commit()
    logger.info(f"Updated agg_job_id to cm_generated_keyword table successfully")
