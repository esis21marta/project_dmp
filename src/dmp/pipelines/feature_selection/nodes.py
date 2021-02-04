import logging
from typing import List

import pai
import pandas as pd

FEATURE_COL = "feature"
IMPORTANCE_COL = "importance"

logger = logging.getLogger(__name__)


def _get_feature_importance_from_pai(pai_run_id: str) -> pd.DataFrame:
    """
    Loads feature and importance from given PAI run_id

    Args:
        pai_run_id: PAI run_id to get feature importance information
        feature_col: name of feature column
        importance_col: name of importance column

    Returns:
        DataFrame with feature and importance column of the features logged in PAI
    """
    IMPORTANCE_PREFIX = "importance_"
    PREFIX_LEN = len(IMPORTANCE_PREFIX)

    pai_features = pai.load_features(run_id=pai_run_id)
    logger.info("features loaded from: %s", pai_run_id)
    # select importance cols and remove "importance_" prefix
    importance_cols = [
        col for col in pai_features.columns if col.startswith(IMPORTANCE_PREFIX)
    ]
    importance_df = pai_features[importance_cols].rename(
        columns=lambda col: col[PREFIX_LEN:]
    )
    # select first row as series, rename series and index
    importance_df = (
        importance_df.iloc[0]
        .rename(IMPORTANCE_COL)
        .rename_axis(index=FEATURE_COL)
        .reset_index()
    )
    return importance_df


def get_feature_importance(
    pai_run_ids: List[str], pai_runs_load: str
) -> List[pd.DataFrame]:
    """
    Get feature importance dataframes from given PAI run_ids

    Args:
        pai_run_ids: PAI run_ids to get feature importances

    Returns:
        list of dataframes containing feature importance
    """
    # Step1: Get current PAI_RUNS and run_id, then end current run
    current_pai_runs = pai.get_storage()
    if current_pai_runs.startswith("file://"):
        current_pai_runs = "/" + current_pai_runs[5:].lstrip("/")
    current_run_id = pai.current_run_uuid()
    pai.end_run()

    # step2: Shift to new PAI_RUNS to load artifacts
    pai.set_config(storage_runs=pai_runs_load)

    importance_dfs = []
    for ix, pai_run_id in enumerate(pai_run_ids):
        feature_importance = _get_feature_importance_from_pai(pai_run_id)
        importance_dfs.append(feature_importance)

    # step 3: Shift back to original PAI_RUNS, and resume run
    pai.set_config(storage_runs=current_pai_runs, experiment="feature_selection")
    pai.start_run(run_id=current_run_id)
    return importance_dfs


def aggregate_feature_importance(importance_dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Aggregates feature importance

    Args:
        importance_dfs: list of dataframes containing feature importance

    Returns:
        dataframe with feature and importance columns
    """
    # append dataframes
    importance_df = pd.concat(importance_dfs, ignore_index=True)
    # aggregate by sum
    agg_importance_df = importance_df.groupby(FEATURE_COL).sum().reset_index()
    return agg_importance_df


def top_n_features(importance_df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    """
    Selects top_n features by importance values

    Args:
        importance_df: dataframe containing feature
        top_n: number of top features to select

    Returns:
        dataframe with feature and importance columns of top n features
    """
    # select top n
    top_importance_df = importance_df.nlargest(top_n, IMPORTANCE_COL).reset_index(
        drop=True
    )
    # log to pai
    pai.log_features(
        features=top_importance_df[FEATURE_COL],
        importance=top_importance_df[IMPORTANCE_COL],
    )
    pai.log_artifacts({"top_important_features": top_importance_df})
    return top_importance_df
