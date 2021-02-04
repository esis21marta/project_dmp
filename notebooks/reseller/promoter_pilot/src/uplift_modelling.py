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

"""
Functions for supporting modelling with causalml
"""

from typing import Tuple

import numpy as np
import pandas as pd
import shap
from causalml.inference.meta.utils import convert_pd_to_np
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import KFold, train_test_split

from .util import check_args


def run_cross_validate(
    model, metric_func, x_data, y_data, treatment, **kwargs
) -> Tuple[np.array, np.array]:
    """
    Performs out-of-sample validation on a specified number of folds.
    Args:
        model: causalml model object to be evaluated
        metric_func: Function for model evaluation; causalml evaluation
        functions can be used directly.
        x_data: Array with features values
        y_data: Vector of outcome values
        treatment: Vector of 0/1 indicating belonging to treatment or control group
        **kwargs: {`n_splits`: Number of folds to test, `rnd_seed`: Random seed value}
    Returns:
        Vectors of provided metrics' values for test and train data sets for all folds
    """

    check_args(kwargs.keys(), ["n_splits", "rnd_seed"])
    n_splits = kwargs.get("n_splits", 5)
    rnd_seed = kwargs.get("rnd_seed", 42)
    x_data, treatment, y_data = convert_pd_to_np(x_data, treatment, y_data)
    fold = KFold(n_splits=n_splits, shuffle=True, random_state=rnd_seed)

    metrics_train = []
    metrics_test = []

    for train_index, test_index in fold.split(x_data):
        model.fit(
            X=x_data[train_index],
            y=y_data[train_index],
            treatment=treatment[train_index],
        )

        metric_train = evaluate_model(
            model=model,
            metric_func=metric_func,
            x_data=x_data[train_index],
            y_data=y_data[train_index],
            treatment=treatment[train_index],
        )
        metric_test = evaluate_model(
            model=model,
            metric_func=metric_func,
            x_data=x_data[test_index],
            y_data=y_data[test_index],
            treatment=treatment[test_index],
        )

        metrics_train.append(metric_train)
        metrics_test.append(metric_test)

    return metrics_test, metrics_train


def evaluate_model(model, metric_func, x_data, y_data, treatment) -> float:
    """
    Evaluates model on a given dataset with provided evaluation function.
    Args:
        model: Fitted causalml model object to be evaluated
        metric_func: Function for model evaluation; causalml evaluation
        functions can be used directly.
        x_data: Input variables
        y_data: Outcome variable
        treatment: Vector of 0/1 indicating belonging to treatment or control group
    Returns:
        Value of defined evaluation metric.
    Raises:
         ValueError: Raised when metric func value is out from allowed list.
    """

    if metric_func.__name__ not in (
        "get_cumgain",
        "get_cumlift",
        "get_qini",
        "auuc_score",
        "qini_score",
    ):
        raise ValueError(
            "Metric function should be from list {}".format(
                ("get_cumgain", "get_cumlift", "get_qini", "auuc_score", "qini_score",)
            )
        )
    x_data, treatment, y_data = convert_pd_to_np(x_data, treatment, y_data)
    preds = model.predict(X=x_data, y=y_data, treatment=treatment)

    metric_df = pd.DataFrame({"y": y_data, "w": treatment, "pred": preds.reshape(-1)})

    return metric_func(metric_df)[0]


def evaluate_treatment_control_split(
    x_data,
    treatment,
    model=RandomForestClassifier(n_estimators=100),
    test_size: float = 0.2,
    rnd_seed: float = 42,
) -> float:
    """
    Evaluates if treatment/control group assigment can be predicted using provided features.
    Args:
        x_data:  Matrix with features values
        treatment: Vector of 0/1 indicating belonging to treatment or control group
        model: Model to be used for evaluation
        test_size: Proportion of observations to be used as test set
        rnd_seed: Random seed value
    Returns:
        AUC value of fitted model on a 20% test set.
    """

    x_data, treatment = convert_pd_to_np(x_data, treatment)
    x_train, x_test, treatment_train, treatment_test = train_test_split(
        x_data, treatment, test_size=test_size, random_state=rnd_seed
    )

    model.fit(x_train, treatment_train)

    preds = model.predict_proba(x_test)

    return roc_auc_score(treatment_test, preds[:, 1])


def evaluate_treatment_control_outcome(model, x_data, y_data, treatment, metric_func):
    """
    Calculates goodness of fit value for outcome for treatment and control
    groups using provided metric function
    Args:
        model: Fitted causalml meta-learner model object to be evaluated
        x_data: Input variables
        y_data: Outcome variable
        treatment: Vector of 0/1 indicating belonging to treatment or control group
        metric_func: function for model evaluation
    Returns:
        (metric_control, metric_treatment) Values of specified metric
        calculated on treatment and control groups.
    Raises:
        ModuleNotFoundError: Raised when module of model is not found
    """

    x_data, treatment, y_data = convert_pd_to_np(x_data, treatment, y_data)
    x_control, x_treatment = _get_treatment_control(x_data, treatment)
    y_control, y_treatment = _get_treatment_control(y_data, treatment)

    if "tlearner" in model.__module__:
        control_y_score = getattr(model, "models_c")[1].predict(x_control)
        treatment_y_score = getattr(model, "models_t")[1].predict(x_treatment)

    elif "slearner" in model.__module__:
        gmodel = getattr(model, "models")[1]
        x_control = np.insert(x_control, 0, 0, axis=1)
        x_treatment = np.insert(x_treatment, 0, 1, axis=1)
        control_y_score = gmodel.predict(x_control)
        treatment_y_score = gmodel.predict(x_treatment)

    elif "xlearner" in model.__module__:
        control_y_score = getattr(model, "models_mu_c")[1].predict(x_control)
        treatment_y_score = getattr(model, "models_mu_t")[1].predict(x_treatment)

    else:
        raise ModuleNotFoundError("Model module doesn't exits.")

    return (
        metric_func(y_control, control_y_score),
        metric_func(y_treatment, treatment_y_score),
    )


def _get_treatment_control(data, treatment):
    """
    Args:
        data: Input data
        treatment: treatment flag with value 0 or 1
    Returns:
         control_data, control_treatment
    """
    return data[treatment == 0], data[treatment == 1]


def get_shap_input(
    model, x_data, uplift_learner=RandomForestRegressor(n_estimators=100)
):
    """
    Args:
        model: Fitted causalml model object to be investigated
        x_data: Array with features values
        uplift_learner: Model to predict the uplift and to be directly explained
    Returns:
        SHAP explainer and shap_values objects to be used for plots
    """

    uplifts = model.predict(x_data)

    uplift_learner.fit(x_data, uplifts)
    explainer = shap.TreeExplainer(uplift_learner)
    shap_values = explainer.shap_values(x_data)

    return explainer, shap_values
