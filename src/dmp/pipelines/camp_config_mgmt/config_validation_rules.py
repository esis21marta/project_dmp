import numpy as np
import pandas as pd


def identify_partial_missing_configuration(
    campaign_name_family_df: pd.DataFrame,
    prioritization_rank: pd.DataFrame,
    campaign_config: pd.DataFrame,
) -> None:
    """
    The function checks if any campaign name and family in the df
    has missing information in cm_campaign_config or cm_prioritization_rank
    Args:
        campaign_name_family_df (pd.DataFrame): A dataframe of unique campaign name and family
        campaign_config (pd.DataFrame): A dataframe of cm_campaign_config table
        prioritization_rank (pd.DataFrame): A dataframe of cm_prioritization_rank table

    Returns:
    """

    camp_channels = campaign_config[
        ["campaign_name", "campaign_family", "push_channel", "dest_sys"]
    ]

    camp_df = pd.merge(
        campaign_name_family_df,
        camp_channels,
        on=["campaign_name", "campaign_family"],
        how="left",
    )
    camp_df = pd.merge(
        camp_df,
        prioritization_rank[["campaign_family", "campaign_rank"]],
        on=["campaign_family"],
        how="left",
    )

    camp_df["valid"] = np.where(
        (camp_df["campaign_family"].str.strip() == "")
        | (camp_df["campaign_family"].isnull() == True)
        | (camp_df["campaign_rank"] == "")
        | (camp_df["campaign_rank"].isnull() == True)
        | (camp_df["push_channel"] == "")
        | (camp_df["push_channel"].isnull() == True)
        | (camp_df["dest_sys"] == "")
        | (camp_df["dest_sys"].isnull() == True),
        "No",
        "Yes",
    )

    rejected_camp_df = camp_df[camp_df["valid"] == "No"]
    assert (
        rejected_camp_df["valid"].count() == 0
    ), f"Partial configuration not available"


def check_duplication_per_campaign_name_family_channel_system(df: pd.DataFrame):
    """
    The function checks if each (campaign_name, campaign_family, push_channel, dest_sys)
    has single row of record
    Args:
        df (pd.DataFrame): A dataframe with campaign_name, campaign_family, push_channel, dest_sys
    Returns:
    """
    check_duplication = (
        df.groupby(["campaign_name", "campaign_family", "push_channel", "dest_sys"])
        .size()
        .reset_index(name="counts")
    )
    check_duplication = check_duplication[check_duplication["counts"] > 1][
        "campaign_name"
    ].unique()

    assert (
        len(check_duplication) == 0
    ), f"Campaign name has multiple records for a channel per system, per family : {check_duplication}"


def check_cms_has_single_row(df: pd.DataFrame):
    """
    The function checks for configurations with dest_system = 'cms' should have a single
    row of record per campaign_name and family
    Args:
        df (pd.DataFrame): A dataframe with campaign_name, dest_sys
    Returns:
    """
    cms_df = df[df["dest_sys"] == "cms"]
    if len(cms_df) > 0:
        check_cms_records = (
            cms_df.groupby(["campaign_name", "campaign_family"])
            .size()
            .reset_index(name="counts")
        )
        check_cms_records = check_cms_records[check_cms_records["counts"] > 1][
            "campaign_name"
        ].unique()

        assert (
            len(check_cms_records) == 0
        ), f"Campaign name has multiple records but its system is CMS : {check_cms_records}"
