import contextlib
from typing import List, Tuple, Union

import matplotlib as mpl
import matplotlib.pyplot as plt
import pai
import pandas as pd
import shap
from catboost import CatBoostClassifier, CatBoostRegressor

from src.dmp.pipelines.model_factory.consts import FEATURE_COL, IMPORTANCE_COL


@contextlib.contextmanager
def telkomsel_style():
    """
    This little context manager ensures you're using a tsel style format
    in your graphs.

    Usage example
    -------------

    ```
    @telkomsel_style
    def my_fn():
        plot([1,2,3,4])
    ```

    Or
    ```
    def my_fn():
        with telkomsel_style():
            plot([1,2,3,4])
    ```
    """
    custom_style = {
        "figure.figsize": (16, 9),
        "figure.autolayout": True,
        # TSel colors
        "axes.prop_cycle": mpl.cycler(
            color=["ea3323", "909090", "BB2A23", "CDCDCD", "EFD3CA"]
        ),
    }

    # seaborn-white: white background with no axes ticks
    # seaborn-poster: larger font sizes
    styles = ["seaborn-white", "seaborn-poster", custom_style]

    # This yield none should work with the contextmanager annotation and
    # turn this function into a decorator / context manager.
    with plt.style.context(styles, after_reset=True):
        yield


def create_and_log_shap_plot(
    x: pd.DataFrame,
    explainer_model: Union[CatBoostRegressor, CatBoostClassifier],
    artifact_name: str,
) -> None:
    """
    Creates a SHAP plot using pre-trained explainer model and logs it in PAI as artifact_name.

    :param x: DataFrame containing features
    :param explainer_model: Pre-trained CatBoost model used for SHAP explaination
    :param artifact_name: Name of plot artifact in PAI
    """
    shap_explainer = shap.TreeExplainer(
        explainer_model, feature_perturbation="tree_path_dependent"
    )
    shap_values = shap_explainer.shap_values(x)
    # Shorten the feature names so that they fit on SHAP plots
    feature_names = [col[4:40] for col in x.columns]
    fig = plt.figure()
    shap.summary_plot(
        shap_values,
        features=x,
        feature_names=feature_names,
        show=False,
        max_display=len(x.columns),
    )
    plt.tight_layout()
    pai.log_artifacts({artifact_name: fig})


def summarise_table(
    df: pd.DataFrame, key_col: str, other_cols: Tuple[str, ...],
) -> pd.DataFrame:
    """
    Computes min, median, max and std of columns with prefix in other_cols.

    :param df: Input DataFrame
    :param key_col: Name of the key column
    :param other_cols: Prefix in name of the columns to be summarised
    :return: dataframe containing the key and the summary columns
    """
    df = df.copy()
    for col_name in other_cols:
        df[f"min_{col_name}"] = df.iloc[
            :, [col.startswith(col_name) for col in df.columns]
        ].min(axis=1, skipna=False)
        df[f"median_{col_name}"] = df.iloc[
            :, [col.startswith(col_name) for col in df.columns]
        ].median(axis=1, skipna=True)
        df[f"max_{col_name}"] = df.iloc[
            :, [col.startswith(col_name) for col in df.columns]
        ].max(axis=1, skipna=False)
        df[f"std_{col_name}"] = df.iloc[
            :, [col.startswith(col_name) for col in df.columns]
        ].std(axis=1, skipna=True)

    columns_of_interest = [key_col] + [col for col in df if col.endswith(other_cols)]
    return df[columns_of_interest]


def summarise_and_log_feature_importance(
    importance_dfs: List[pd.DataFrame],
    feature_col: str = FEATURE_COL,
    importance_col: str = IMPORTANCE_COL,
    pai_log_suffix: str = "",
) -> pd.DataFrame:
    """
    Takes a list of feature importance dataframes from cross validation fits, summarise and log into PAI.

    :param importance_dfs: list of dataframes containing feature names and importance values from cross validation fits
    :param feature_col: column name containing feature names
    :param importance_col: column name containing feature importance values
    :param pai_log_suffix: suffix for PAI logging purposes
    :return: summary dataframe of feature importance containing mix, max, median, std
    """
    # rename columns and merge dfs
    renamed_dfs = []
    for ix, df in enumerate(importance_dfs):
        df = df.rename(
            columns={
                "Feature Id": feature_col,
                "Importances": f"{importance_col}_{ix}",
            }
        ).set_index(feature_col)
        renamed_dfs.append(df)
    importance_df = pd.concat(renamed_dfs, axis=1, sort=True)
    # rename index as feature_col to handle index being unnamed
    importance_df = importance_df.rename_axis(index=feature_col).reset_index()

    # summarise df and sort by median
    importance_summary_df = summarise_table(
        importance_df, feature_col, (importance_col,)
    )
    importance_summary_df = importance_summary_df.sort_values(
        f"median_{importance_col}", ascending=False
    ).reset_index(drop=True)

    pai.log_artifacts(
        {f"feature_importance_{pai_log_suffix}": importance_summary_df.round(6)}
    )

    return importance_summary_df
