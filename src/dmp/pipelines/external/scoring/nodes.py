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

from datetime import date
from typing import Any, Dict

import numpy as np
import pai
import pandas as pd
import pyspark
from pyspark.sql.functions import coalesce, col, exp, lit, pandas_udf, when
from pyspark.sql.types import DoubleType

from src.dmp.pipelines.external.common.python.artifacts_loading import (
    load_json_artifact,
)


def categorical_none_to_nan_string(
    to_score: pyspark.sql.DataFrame, pai_run: str
) -> pyspark.sql.DataFrame:
    """
    Replaces all missing values in columns with categorical data by 'nan' strings.

    :param to_score: DataFrame containing superset of all features used in the training pai_run
    :param pai_run: Name of the PAI run contining the kedro_params*categorical_features json
    :return: DataFrame with missing values for categorical data replaced by 'nan' strings
    """
    categorical_features = load_json_artifact(
        pai_run, "kedro_params*categorical_features"
    )
    return to_score.fillna("nan", subset=categorical_features)


def score(
    to_score: pyspark.sql.DataFrame,
    pai_run: str,
    score_column_name: str,
    model_type: str = "",
    predict_probability: bool = True,
) -> pyspark.sql.DataFrame:
    """
    Scores all rows in to_score using the model and column names saved in the training pai_run, puts the results in score_column_name.

    :param to_score: DataFrame containing superset of all features used in the training pai_run
    :param pai_run: Name of the PAI run contining the model and feature_column_name_list json
    :param score_column_name: Column in which all scores will be put
    :param predict_probability: to whether predict the probability or the value
    :return: DataFrame with scores put in score_column_name
    """
    model = pai.load_model(pai_run)
    feature_column_name_list = load_json_artifact(pai_run, "feature_column_name_list")
    today = date.today()

    @pandas_udf(returnType=DoubleType())
    def pandas_predict_udf(*cols):
        X = pd.concat(cols, axis=1)
        X.columns = feature_column_name_list
        if predict_probability:
            return pd.Series(model.predict_proba(X)[:, 1])
        return pd.Series(model.predict(X))

    to_score = to_score.withColumn(
        score_column_name, pandas_predict_udf(*feature_column_name_list)
    )

    refresh_date = f"refresh_date_{model_type}" if model_type != "" else "refresh_date"
    model_id = f"model_id_{model_type}" if model_type != "" else "model_id"

    to_score = to_score.withColumn(refresh_date, lit(today)).withColumn(
        model_id, lit(pai_run)
    )

    return to_score


def map_to_bin(score_column_name: str, bin_thresholds: Dict[str, Any]) -> list:
    """
    Maps each score to a correspondent bin resulted from the binning on the training set.

    :param score_column_name: Column in which all scores will be put
    :param bin_thresholds: A dictionary containing the thresholds of each training score bin
    :return: list of conditions to be applied on each row on a dataframe
    """
    conds = []

    for _, v in bin_thresholds.items():
        maxv = v["max"] if v["max"] != "inf" else np.inf
        minv = v["min"] if v["min"] != "-inf" else -np.inf
        meanv = v["mean"]
        conds.append(when(col(score_column_name).between(minv, maxv), meanv))
    return conds


def post_process(
    scored: pyspark.sql.DataFrame,
    score_column_name: str,
    binned: bool,
    binned_score_column_name: str = "",
    bin_thresholds: Dict[str, Any] = {},
    post_processing: str = "identity",
) -> pyspark.sql.DataFrame:
    """
    Applies post_processing to the score_column_name.

    :param scored: DataFrame containing the score_column_name column
    :param score_column_name: Column containing the model score
    :param binned: A boolean value that represents whether the raw scores need to be binned
    :param binned_score_column_name: Column containing the mean probability of the bin where the score is located
    :param bin_thresholds: A dictionary containing the thresholds of each training score bin
    :param post_processing: Name of postprocessing to be applied
    :return: DataFrame with post_processing applied to the score_column_name
    """
    if binned:
        conds = map_to_bin(score_column_name, bin_thresholds)
        return scored.select("*", coalesce(*conds).alias(binned_score_column_name))

    post_processing_options = {
        "identity": scored,
        "exponential": scored.withColumn(
            score_column_name, exp(col(score_column_name))
        ),
    }

    try:
        return post_processing_options[post_processing]
    except KeyError:
        raise ValueError(
            f"{post_processing} is not one of the implemented post processings: {set(post_processing_options)}"
        )
