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

from typing import Union

import catboost
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pai
import pandas as pd
import pyspark
import seaborn as sns
from sklearn.model_selection import cross_validate

from src.dmp.pipelines.external.common.python.artifacts_loading import (
    load_df_from_artifact,
    load_json_artifact,
)
from src.dmp.pipelines.external.common.python.utils import filter_down


def sample(df: pyspark.sql.DataFrame, sample_size: int) -> pyspark.sql.DataFrame:
    """
    Subsamples the DataFrame to return sample_size rows. If sample_size is greater than the original DataFrame size, returns original DataFrame.

    :param df: Input DataFrame
    :param sample_size: Required sample size
    :return: DataFrame containing a subset of rows
    """
    rows = df.count()
    if rows < sample_size:
        return df
    else:
        sample_fraction = sample_size / rows
        return df.sample(withReplacement=False, fraction=sample_fraction)


def to_pandas(df: pyspark.sql.DataFrame) -> pd.DataFrame:
    """
    Converts PySpark DataFrame to Pandas DataFrame.

    :param df: Input PySpark DataFrame
    :return: Pandas DataFrame
    """
    return df.toPandas()


def check_covariate_shift(
    score_df: pd.DataFrame,
    score_filter_query: str,
    plot_feature_distributions: bool,
    pai_run: str,
) -> None:
    """
    Performs model based check for covariate shift.
    Logs the model AUC for each feature using PAI.
    Optionally filters down the score data and plots the training and scoring features distributions.

    :param score_df: Input DataFrame containing features from the dataset to be scored
    :param score_filter_query: Query used to filter down score_df
    :param plot_feature_distributions: To plot or not the distributions of the training and scoring features
    :param pai_run: Name of the PAI run contining the final_model_train_data artifact and kedro_params*categorical_features json
    """
    score_df = filter_down(score_df, score_filter_query)

    train_df = load_df_from_artifact(pai_run, "final_model_train_data")
    categorical_features = load_json_artifact(
        pai_run, "kedro_params*categorical_features"
    )

    columns_of_interest = (
        col for col in train_df.columns if col not in categorical_features
    )

    shift_aucs = []
    for col in columns_of_interest:
        auc = _get_covariate_shift_auc(train_df[[col]], score_df[[col]])
        shift_aucs.append(
            {"Feature": col, "Covariate shift model AUC": np.round(auc, decimals=2)}
        )

        if plot_feature_distributions:
            distributions_plot = _plot_feature_distributions(
                train_df[col], score_df[col], col
            )
            pai.log_artifacts({f"{col} distributions plot": distributions_plot})

    shift_auc_df = (
        pd.DataFrame(shift_aucs)
        .sort_values(by="Covariate shift model AUC", ascending=False)
        .reset_index(drop=True)
    )
    pai.log_artifacts({"Covariate shift AUCs": shift_auc_df})


def _get_covariate_shift_auc(df1: pd.DataFrame, df2: pd.DataFrame) -> float:
    """
    Trains a classifier to distinguish between two one column DataFrames.
    Returns the mean value of classifier's AUC as a proxy for dissimilarity between the two columns.

    :param df1: Input DataFrame 1 containing one column - values of a feature of interest
    :param df2: Input DataFrame 2 containing one column - values of a feature of interest
    :return: Mean value of classifier's AUC over cross validation folds
    """

    df1 = df1.fillna(np.nan)
    df2 = df2.fillna(np.nan)

    y1 = [0] * df1.shape[0]
    y2 = [1] * df2.shape[0]

    y = y1 + y2
    X = df1.append(df2)

    params = {
        "iterations": 10,
        "depth": 6,
        "loss_function": "Logloss",
        "verbose": False,
        "allow_writing_files": False,
    }
    estimator = catboost.CatBoostClassifier(**params)

    return np.mean(cross_validate(estimator, X, y, scoring="roc_auc")["test_score"])


def _plot_feature_distributions(
    train_feature: pd.Series,
    scoring_feature: pd.Series,
    feature_name: str,
    outlier_quantile: float = 0.99,
) -> mpl.figure.Figure:
    """
    Creates a plot containing distributions of train and scoring features.

    :param train_feature: Series containing the values of a training feature
    :param scoring_feature: Series containing the values of a scoring feature
    :param feature_name: Name of the feature
    :param outlier_quantile: Outlier quantile used for cropping the right hand side of the plot
    :return: Plot containing distributions of train and scoring features
    """

    fig = plt.figure(figsize=(16, 9), constrained_layout=True)
    ax = fig.add_subplot(111)
    fig.suptitle(f"Train and scoring distributions for {feature_name}", size=15)

    # Matplotlib does not like nans and Decimals
    train_feature = train_feature.dropna().astype(float)
    sns.distplot(
        train_feature,
        label="Train",
        bins=_get_best_bins(train_feature),
        kde=False,
        norm_hist=True,
    )
    # Matplotlib does not like nans and Decimals
    scoring_feature = scoring_feature.dropna().astype(float)
    sns.distplot(
        scoring_feature,
        label="Scoring",
        bins=_get_best_bins(scoring_feature),
        kde=False,
        norm_hist=True,
    )

    ax.set_xlabel(feature_name)
    ax.legend(loc="upper right")

    combined = train_feature.append(scoring_feature)
    ax.set_xlim(combined.min(), combined.quantile(outlier_quantile))
    return fig


def _get_best_bins(feature_values: pd.Series) -> Union[int, np.ndarray]:
    """
    Finds the best binning for the feature_values univariate distribution.
    Returns number of bins or edges of bins depending on count of unique feature_values.

    :param feature_values: Series containing the values of the feature
    :return: Count of unique feature_values if the count is not greater than 30, edges of 50 bins otherwise
    """
    unique_count = len(feature_values.unique())
    if unique_count <= 30:
        return unique_count
    else:
        # Matplotlib does not like non-unique bin edges
        return np.unique(np.quantile(feature_values, np.arange(0, 1.02, 0.02)))
