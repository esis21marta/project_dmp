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

from typing import Dict, List, Union

import numpy as np
import pandas as pd
import shap
from catboost import CatBoostClassifier, Pool
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier


class ClassifierFacade(BaseEstimator):
    def __init__(self, classifier_params: Dict[str, Union[str, float]]):
        """
        Initializes the classifier

        :param classifier_params: classifier parameters

            Examples for options implemented so far:

                name: "catboost"
                params:
                    iterations: 500
                    l2_leaf_reg: 100
                    depth: 6
                    rsm: 0.5
                    learning_rate: 0.05
                    loss_function: "Logloss"
                    verbose: False
                    allow_writing_files: False

                name: "logistic_regression"
                params:
                    penalty: "l2"
                    tol: 0.001
                    C: 10.0
                    fit_intercept: True
                    random_state: 123
                    max_iter: 1000
                    class_weight: "balanced"
                    verbose: 0
                    warm_start: False

                name: "random_forest"
                params:
                    n_estimators: 500
                    max_depth: 6
                    max_features: "auto"
                    random_state: 123
                    class_weight: "balanced"

                name: "xgboost"
                params:
                    n_estimators: 300
                    learning_rate: 0.1
                    max_depth: 6
                    objective: "binary:logistic"
                    booster: 'gbtree'
                    scale_pos_weight: 1
                    """
        self.KNOWN_ML_CLASSIFIERS = {
            "CATBOOST": CatBoostClassifier,
            "LOGISTIC_REGRESSION": LogisticRegression,
            "RANDOM_FOREST": RandomForestClassifier,
            "XGBOOST": XGBClassifier,
        }

        self._classifier_name = classifier_params["name"].upper().strip()

        if self._classifier_name not in self.KNOWN_ML_CLASSIFIERS:
            raise NotImplementedError(
                (
                    f"Unknown classifier '{self._classifier_name }'. "
                    f"List of implemented classifiers are: {self.KNOWN_ML_CLASSIFIERS.keys()}"
                )
            )

        # Assign a random seed to each classifier to guarantee consistency in the model output
        random_states = {
            "CATBOOST": "random_seed",
            "LOGISTIC_REGRESSION": "random_state",
            "RANDOM_FOREST": "random_state",
            "XGBOOST": "seed",
        }
        if random_states[self._classifier_name] not in classifier_params["params"]:
            classifier_params["params"][random_states[self._classifier_name]] = 0

        self._params = classifier_params["params"]
        self._imputer = (
            SimpleImputer()
        )  # TODO: This replace NA by 0 => Make sure that we want this
        self._data_scaler = StandardScaler(with_mean=True, with_std=True)
        self._model = self.KNOWN_ML_CLASSIFIERS[self._classifier_name](**self._params)
        self._feature_importance = None

    def fit(
        self, X: pd.DataFrame, y: pd.Series, categorical_features: List[str] = None
    ):
        """
        Fits a classifier to the input data (X, y)

        :param X: DataFrame to be used to fit the classifier
        :param y: Series to be used as target when fitting the model
        :param categorical_features: Columns containing the categorical features
        :return: Fit model
        """
        if self._classifier_name == "CATBOOST":
            training_pool = Pool(data=X, label=y, cat_features=categorical_features)
            self._model.fit(training_pool)
            self._feature_importance = self._model.get_feature_importance(
                training_pool, prettified=True
            )
        else:
            self._feature_importance = {"Feature Id": X.columns}
            if self._classifier_name == "LOGISTIC_REGRESSION":
                self._imputer.fit()
                X_transformed = self._imputer.transform(X)
                self._data_scaler.fit(X_transformed)
                X_scaled = self._data_scaler.transform(X_transformed)
                self._model.fit(X_scaled, y)
                importance = np.abs(self._model.coef_[0])
            else:
                self._model.fit(X, y)
                importance = self._model.feature_importances_

            self._feature_importance["Importances"] = importance
        return self

    def predict(self, X: pd.DataFrame):
        """
         Predicts the classes using the input DataFrame

        :param X: DataFrame to be used to perform the prediction of classes
        :return: Series of predicted classes
        """
        if self._classifier_name == "LOGISTIC_REGRESSION":
            X_transformed = self._imputer.transform(X)
            X_scaled = self._data_scaler.transform(X_transformed)
            return self._model.predict(X_scaled)

        return self._model.predict(X)

    def predict_proba(self, X: pd.DataFrame):
        """
        Predicts the probabilities of classes using the input DataFrame

        :param X: DataFrame to be used to perform the prediction of probabilities
        :return: Series of predicted probabilities
        """
        if self._classifier_name == "LOGISTIC_REGRESSION":
            X_transformed = self._imputer.transform(X)
            X_scaled = self._data_scaler.transform(X_transformed)
            return self._model.predict_proba(X_scaled)

        return self._model.predict_proba(X)

    def get_feature_importance(self):
        """
        Returns the feature importance of the fitted model

        :return: a dictionary of feature & importance
        """

        return self._feature_importance

    def get_shap(self, X: pd.DataFrame, y: pd.Series, categorical_features=None):
        """
        Estimates the Shap values for a given X and y

        :param X: DataFrame to be used to estimate the Shap values
        :param y: Series used to be used in conjonction with the Input Dataframe to estimate the Shap values
        :param categorical_features: Columns containing the categorical features
        :return: the estimated Shap values
        """
        if self._classifier_name in ["LOGISTIC_REGRESSION"]:
            # NOT supported by Shap
            return None

        explainer = shap.TreeExplainer(
            self._model, feature_perturbation="tree_path_dependent"
        )
        if self._classifier_name == "CATBOOST":
            pool = Pool(data=X, label=y, cat_features=categorical_features)
            shap_values = explainer.shap_values(pool)
        else:
            shap_values = explainer.shap_values(X, y)

        return shap_values


class SimpleImputer(TransformerMixin, BaseEstimator):
    def fit(self):
        """
        Fits the imputer
        :return: Fit imputer
        """
        return self

    def transform(self, X: pd.DataFrame):
        """
        transforms X

        :param X: DataFrame to be used to fit the classifier
        :return: X transformed
        """
        return X.fillna(0)

    def fit_transform(self, X: pd.DataFrame):
        """
        Fits the imputer to the input data (X, y) and transforms X

        :param X: DataFrame to be used to fit the classifier
        :return: X transformed
        """
        self.fit()
        return self.transform(X)
