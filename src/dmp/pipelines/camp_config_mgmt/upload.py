import logging
from datetime import datetime as dt
from typing import Union

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm.session import Session

from src.dmp.pipelines.camp_config_mgmt.config_validation_rules import (
    check_cms_has_single_row,
    check_duplication_per_campaign_name_family_channel_system,
)
from src.dmp.pipelines.core.entity import CampaignConfig, CampaignCriteria, CampaignRank

logging.basicConfig(level=logging.INFO)
logger = logging.Logger(__name__)


def update_campaign_config_table(df: pd.DataFrame, session: Session, connection):
    """
    The function checks the dataframe of campaign configuration and updates it to the database table
    Args:
        df (pd.DataFrame): A dataframe of campaign configuration
        session (Session): The SQLAlchemy db session
        connection (connection): The SQLAlchemy db connection

    Returns:
    """
    try:

        check_duplication_per_campaign_name_family_channel_system(df)

        check_cms_has_single_row(df)

        logger.info("Validation of Campaign Config passed")

        active_campaign_name_family = pd.read_sql(
            "SELECT distinct campaign_name, campaign_family FROM cm_campaign_config where status = 'active'",
            connection,
        )
        active_campaign_name_family["indicator_active"] = 1

        df_campaign_name_family = df[
            ["campaign_name", "campaign_family"]
        ].drop_duplicates()

        existing_campaign_name_family = pd.merge(
            df_campaign_name_family,
            active_campaign_name_family,
            on=["campaign_name", "campaign_family"],
            how="inner",
        )

        existing_campaign_name_family = existing_campaign_name_family[
            ["campaign_name", "campaign_family"]
        ].drop_duplicates()

        new_campaign_name_family = pd.merge(
            df_campaign_name_family,
            active_campaign_name_family,
            on=["campaign_name", "campaign_family"],
            how="left",
        )

        new_campaign_name_family = new_campaign_name_family[
            new_campaign_name_family["indicator_active"].isna() == True
        ]

        new_campaign_name_family = new_campaign_name_family[
            ["campaign_name", "campaign_family"]
        ].drop_duplicates()

        if len(new_campaign_name_family) > 0:
            logger.info(
                f"Adding new campaign names : {new_campaign_name_family.campaign_name.unique()}"
            )

            df_new = pd.merge(
                df,
                new_campaign_name_family,
                on=["campaign_name", "campaign_family"],
                how="inner",
            )

            insert_campaign_config_into_table(df_new, session)

        if len(existing_campaign_name_family) > 0:
            logger.info(
                f"Replacing existing campaign names : {existing_campaign_name_family.campaign_name.unique()}"
            )

            deactivate_existing_campaign_name_family(
                session, existing_campaign_name_family, status="inactive"
            )

            df_existing = pd.merge(
                df,
                existing_campaign_name_family,
                on=["campaign_name", "campaign_family"],
                how="inner",
            )

            insert_campaign_config_into_table(df_existing, session)

    except Exception as e:
        logger.error(f"Validation of Campaign Config failed: {e}")


def insert_campaign_config_into_table(df: pd.DataFrame, session: Session):
    """
    The function inserts the dataframe of campaign configuration into the table
    Args:
        df (pd.DataFrame): A dataframe of campaign configuration
        session (Session): The SQLAlchemy db session

    Returns:
    """

    dicts = [
        dict(
            created_at=dt.utcnow(),
            status="active",
            campaign_family=row["campaign_family"],
            campaign_name=row["campaign_name"],
            push_channel=row["push_channel"],
            dest_sys=row["dest_sys"],
        )
        for _, row in df.iterrows()
    ]

    try:
        session.bulk_insert_mappings(CampaignConfig, dicts)
        session.commit()
        logger.info(f"Updated campaign config table successfully")
    except Exception as e:
        logger.error(f"Error occurred while updating campaign config table: {e}")


def deactivate_existing_campaign_name_family(
    session: Session, campaign_name_family: pd.DataFrame, status: str
):
    """
    The function deactivates the records of cm_campaign_config table given a dataframe of unique campaign name and family
    Args:
        session (Session): The SQLAlchemy db session
        campaign_name_family (pd.DataFrame): A dataframe of campaign name and family
        status(str): The new status to be updated (e.g. inactive)

    Returns:
    """
    try:
        unique_camp_name_family = [
            [row.campaign_name, row.campaign_family]
            for _, row in campaign_name_family.iterrows()
        ]

        for campaign_name, campaign_family in unique_camp_name_family:
            session.query(CampaignConfig).filter(
                CampaignConfig.status == "active"
            ).filter(CampaignConfig.campaign_name.in_([campaign_name])).filter(
                CampaignConfig.campaign_family.in_([campaign_family])
            ).update(
                {
                    CampaignConfig.status: status,
                    CampaignConfig.deactivated_at: dt.now(),
                },
                synchronize_session=False,
            )
            session.commit()

    except Exception as e:
        logger.error(
            f"Error occurred while deactivating campaign configuration table: {e}"
        )


def update_prioritization_tables(df: pd.DataFrame, session: Session, connection):
    """
    The function updates the prioritization strategy dataframe to the two tables
    cm_prioritization_rank and cm_prioritization_criteria
    Args:
        df (pd.DataFrame): A dataframe of prioritization strategy
        session (Session): The SQLAlchemy db session
        connection : The SQLAlchemy db connection

    Returns:
    """
    camp_rank_df = df[["campaign_family", "campaign_rank"]]
    insert_campaign_ranks_into_table(camp_rank_df, session)

    camp_rank_db = pd.read_sql(
        "SELECT p_rank_id, campaign_family FROM cm_prioritization_rank where status = 'active'",
        connection,
    )
    criteria_df = pd.merge(df, camp_rank_db, on="campaign_family", how="left")
    criteria_df = pd.melt(
        criteria_df,
        id_vars=["p_rank_id", "campaign_family"],
        value_vars=[
            "criteria_1",
            "criteria_2",
            "criteria_3",
            "criteria_4",
            "criteria_5",
        ],
        var_name="criteria_rank",
        value_name="criteria_desc",
    )

    criteria_df["criteria_rank"] = (
        criteria_df["criteria_rank"].str.extract("(\d+)").astype(int)
    )
    criteria_df = criteria_df[["p_rank_id", "criteria_rank", "criteria_desc"]]
    criteria_df = criteria_df.sort_values(["p_rank_id", "criteria_rank"])

    insert_criteria_into_table(criteria_df, session)

    logger.info(f"Updated prioritization rank and criteria tables successfully")


def insert_campaign_ranks_into_table(df: pd.DataFrame, session: Session):
    """
    The function inserts the campaign ranks dataframe to cm_prioritization_rank
    Args:
        df (pd.DataFrame): A dataframe of campaign ranks
        session (Session): The SQLAlchemy db session

    Returns:
    """
    max_batch_id = session.query(func.max(CampaignRank.p_rank_batch_id)).scalar()
    if max_batch_id is None:
        max_batch_id = int(0)

    deactivate_current_active_status(session, CampaignRank, status="inactive")
    logger.info(f"Number of rows to add: {len(df.index)}")

    dicts = [
        dict(
            p_rank_batch_id=max_batch_id + 1,
            created_at=dt.utcnow(),
            status="active",
            campaign_family=row["campaign_family"],
            campaign_rank=row["campaign_rank"],
        )
        for _, row in df.iterrows()
    ]

    try:
        session.bulk_insert_mappings(CampaignRank, dicts)
        session.commit()
        logger.info(f"Updated prioritization rank table successfully")

    except Exception as e:
        logger.error(f"Error occurred while updating prioritization rank table: {e}")


def insert_criteria_into_table(df: pd.DataFrame, session: Session):
    """
    The function inserts the prioritization criteria dataframe to cm_prioritization_criteria
    Args:
        df (pd.DataFrame): A dataframe of prioritization criteria
        session (Session): The SQLAlchemy db session

    Returns:
    """

    deactivate_current_active_status(session, CampaignCriteria, status="inactive")
    logger.info(f"Number of rows to add: {len(df.index)}")

    dicts = [
        dict(
            p_rank_id=row["p_rank_id"],
            created_at=dt.utcnow(),
            status="active",
            criteria_rank=row["criteria_rank"],
            criteria_desc=row["criteria_desc"],
        )
        for _, row in df.iterrows()
    ]
    try:
        session.bulk_insert_mappings(CampaignCriteria, dicts)
        session.commit()
        logger.info(f"Updated prioritization criteria table successfully")

    except Exception as e:
        logger.error(
            f"Error occurred while updating prioritization criteria table: {e}"
        )


def deactivate_current_active_status(
    session: Session, config: Union["CampaignCriteria", "CampaignRank"], status: str
):
    """
    The function deactivates the records with active status in the config table
    Args:
        session (Session): The SQLAlchemy db session
        config (Union["CampaignCriteria", "CampaignRank"]): The record class
        status (str) : The new status to be updated (e.g. inactive)

    Returns:
    """
    try:
        current_active_records = len(
            session.query(config).filter(config.status == "active").all()
        )

        logger.info(f"Number of rows to deactivate: {current_active_records}")

        session.query(config).filter(config.status == "active").update(
            {config.status: status, config.deactivated_at: dt.now()},
            synchronize_session=False,
        )
        session.commit()

    except Exception as e:
        logger.error(f"Error occurred while deactivating active status: {e}")
