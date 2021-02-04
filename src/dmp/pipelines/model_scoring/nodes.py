import os
import shutil
import tempfile
import uuid
from functools import reduce
from itertools import combinations
from pathlib import Path
from typing import Any, List

import attr
import matplotlib.pyplot as plt
import numpy as np
import pai
import pandas as pd
import pyspark
import seaborn as sns
from mlflow import pyfunc
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f

from mllib.model_classes.python_model import PythonModel
from src.dmp.pipelines.model_factory.common_functions import telkomsel_style
from src.dmp.pipelines.model_scoring.utils import (
    get_model_path_from_pai_run,
    get_param_value_from_pai_run,
    register_mllib,
)

SCORE_COL = "score"


@attr.s
class ModelData:
    """
    Data class representing a model.

    :param run_id: PAI run id containing the model
    :param model_uri: Path of the mlflow model
    :param features: List of features used to train the model
    :param campaign_name: Name of the campaign on which the model was trained
    """

    run_id = attr.ib()
    model_uri = attr.ib()
    features = attr.ib()
    campaign_name = attr.ib()


def _make_mlflow_model(run_id: str) -> str:
    """
    Builds mlflow model from a model pickle saved on PAI and saves it to a path
    in a temporary directory.

    :param run_id: PAI run id containing the model
    :return: Path of the mlflow model
    """
    MODEL_PATH = "model_pickle_path"

    model_pickle_path = get_model_path_from_pai_run(run_id)
    path = os.path.join(os.path.abspath(tempfile.mkdtemp()), f"model_{uuid.uuid4()}")
    pyfunc.save_model(
        path,
        python_model=PythonModel(MODEL_PATH),
        artifacts={MODEL_PATH: model_pickle_path},
    )
    return path


def _get_features(model_uri: str) -> List[str]:
    """
    Gets list of features used to train a mlflow model.

    :param model_uri: Path of the mlflow model
    :return: List of features used to train the model
    """
    model = pyfunc.load_model(model_uri)
    return model.python_model.features()


def make_mlflow_models_from_pai_runs(
    pai_run_ids: List[str], pai_campaign_name: str,
) -> List[ModelData]:
    """
    Builds mlflow models from model pickles saved on PAI.
    Saves information about each model into a ModelData class.

    :param pai_run_ids: List of PAI run ids
    :param pai_campaign_name: Name of the campaign name parameter in PAI
    :return: List of ModelData classes containing information about the models
    """
    models = []
    for run_id in pai_run_ids:
        campaign_name = get_param_value_from_pai_run(
            run_id=run_id, param=pai_campaign_name
        )
        model_uri = _make_mlflow_model(run_id)
        features = _get_features(model_uri)
        model_data = ModelData(
            run_id=run_id,
            model_uri=model_uri,
            campaign_name=campaign_name,
            features=features,
        )
        models.append(model_data)
    return models


def _score_udf(
    spark: SparkSession, model_uri: str, df: pyspark.sql.DataFrame, cols: List[str],
) -> pyspark.sql.DataFrame:
    """
    Applies a model's predict function using spark_udf.

    :param spark: SparkSession
    :param model_uri: Path of the mlflow model
    :param df: DataFrame to be scored
    :param cols: Columns corresponding to the model features
    :return: DataFrame with SCORE_COL containing the score from the model
    """
    udf = pyfunc.spark_udf(spark, model_uri)
    return df.withColumn(SCORE_COL, udf(*cols))


def score(
    to_score: pyspark.sql.DataFrame,
    models: List[ModelData],
    msisdn_col: str,
    campaign_name_col: str,
) -> pyspark.sql.DataFrame:
    """
    Scores the input DataFrame using multiple models.
    Each model corresponds to one campaign.

    :param to_score: Input DataFrame to be scored
    :param models: Models to be used for scoring, returning one score per model
    :param msisdn_col: Input DataFrame's MSISDN column name
    :param campaign_name_col: Output DataFrame's campaign_name column name
    :return: DataFrame containing one score per model per msisdn
    """
    mllib_path = str(Path(__file__).resolve().parents[4] / "projlib" / "mllib.zip")
    spark = register_mllib(mllib_path)

    to_score.cache()

    dfs_with_score = []
    for model in models:
        df = _score_udf(spark, model.model_uri, to_score, model.features)
        df = df.withColumn(campaign_name_col, f.lit(model.campaign_name)).select(
            msisdn_col, campaign_name_col, SCORE_COL
        )
        dfs_with_score.append(df)

    df = reduce(pyspark.sql.DataFrame.union, dfs_with_score)
    df.cache()

    return df


def pivot_predicted_scores(
    df: pyspark.sql.DataFrame, msisdn_col: str, campaign_name_col: str,
) -> pyspark.sql.DataFrame:
    """
    Pivots the input DataFrame to have one score column per campaign.

    :param df: Input DataFrame containing all scores
    :param msisdn_col: Input DataFrame's MSISDN column name
    :param campaign_name_col: Input DataFrame's campaign_name column name
    :return: Pivoted DataFrame
    """
    return (
        df.groupBy(msisdn_col)
        .pivot(campaign_name_col)
        .agg(f.first(SCORE_COL))
        .repartition(20)
    )


def compute_correlations(df: pyspark.sql.DataFrame, msisdn_col: str) -> pd.DataFrame:
    """
    Computes correlations between pairs of models, returns them as a matrix.

    :param df: DataFrame containing model scores
    :param msisdn_col: Input DataFrame's MSISDN column name
    :return: DataFrame containing a correlation matrix
    """

    model_cols = sorted([c for c in df.columns if c != msisdn_col])
    n = len(model_cols)

    correlation_matrix = np.ones((n, n))
    for i, j in combinations(range(n), 2):
        corr = df.corr(model_cols[i], model_cols[j])
        correlation_matrix[i, j] = correlation_matrix[j, i] = corr

    return pd.DataFrame(correlation_matrix, index=model_cols, columns=model_cols)


@telkomsel_style()
def plot_correlation_matrix(df: pd.DataFrame) -> None:
    """
    Plots a figure representing the correlation matrix and saves it to PAI.

    :param df: DataFrame containing correlation matrix
    """

    figure = plt.figure()
    mask = np.triu(np.ones_like(df.values, dtype=bool), k=1)
    cmap = sns.diverging_palette(240, 10, as_cmap=True)
    gr = sns.heatmap(
        df,
        mask=mask,
        cmap=cmap,
        vmax=None,
        center=0,
        square=True,
        linewidths=5,
        cbar_kws={"shrink": 0.5},
    )
    gr.set_xticklabels(gr.get_xticklabels(), rotation=45, ha="right")

    pai.log_artifacts({"Correlation matrix between models": figure})


def select_highest_score_campaign(
    df: pyspark.sql.DataFrame, msisdn_col: str, campaign_name_col: str,
) -> pyspark.sql.DataFrame:
    """
    For every MSISDN, selects the campaign with the highest score.

    :param df: DataFrame containing models' scores
    :param msisdn_col: Input DataFrame's MSISDN column name
    :param campaign_name_col: Input DataFrame's campaign_name column name
    :return: Campaign_names and scores for highest scores for every MSISDN
    """
    RANK_COL = "rank"
    window = Window.partitionBy(f.col(msisdn_col)).orderBy(f.col(SCORE_COL).desc())
    top_recommended_pred = df.withColumn(RANK_COL, f.row_number().over(window)).where(
        f.col(RANK_COL) == 1
    )
    return top_recommended_pred.select(msisdn_col, campaign_name_col, SCORE_COL)


def return_above_threshold(
    df: pyspark.sql.DataFrame, threshold: int,
) -> pyspark.sql.DataFrame:
    """
    Selects the rows of the df with SCORE_COL above threshold.

    :param df: Scored DataFrame with top recommendation selected
    :param threshold: Minimum accepted value
    :return: DataFrame with SCORE_COL above threshold
    """
    return df.where(f.col(SCORE_COL) >= threshold).repartition(20)


def clean_temp(models: List[ModelData], dummy_input: Any) -> None:
    """
    Deletes mlflow models saved in a temporary directory.

    :param models: List of models represented as ModelData
    :param dummy_input: Artificial input to control Kedro order of execution
    """
    for model in models:
        shutil.rmtree(Path(model.model_uri).parent)
