from typing import Type, Union

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier, CatBoostRegressor


class UpliftBase:
    """
    This is the base class for T-Learner uplift model,
    with CatBoost as the underlying modeling approach.
    """

    def __init__(self, uplift_iterations: int):
        """
        Constructor for UpliftBase class.

        :param uplift_iterations: Parameter `iterations` for CatBoost model
        """
        self.uplift_iterations = uplift_iterations
        self.features = None

        self._t = self._make_estimator()
        self._c = self._make_estimator()

        self.explainer = self._make_explainer()

    def _make_estimator(self) -> Union[CatBoostRegressor, CatBoostClassifier]:
        """
        To be overwritten.
        """
        raise NotImplementedError()

    def _make_explainer(self) -> CatBoostRegressor:
        """
        Create an explainer model with CatBoostRegressor.

        :return: A CatBoostRegressor based explainer
        """
        return CatBoostRegressor(
            iterations=self.uplift_iterations, verbose=False, allow_writing_files=False,
        )

    def fit(
        self,
        x: pd.DataFrame,
        y: Union[pd.DataFrame, pd.Series],
        w: Union[pd.DataFrame, pd.Series],
        *args,
    ):
        """
        Fit the model and explainer with training data.

        :param x: Training features
        :param y: Training targets
        :param w: Treatment indicators
        """
        self.features = x.columns.tolist()
        self._t.fit(x[w == 1].values, y[w == 1].values)
        self._c.fit(x[w == 0].values, y[w == 0].values)
        self.explainer.fit(x, self.predict(x))
        return self

    def predict(self, x: pd.DataFrame) -> np.ndarray:
        """
        To be overwritten.
        """
        raise NotImplementedError()

    def get_feature_importance(self) -> pd.DataFrame:
        """
        Get feature importance from the explainer.

        :return: DataFrame of (feature_id, feature importance) pair sorted by feature importance
        """
        importance_table = self.explainer.get_feature_importance(prettified=True)
        return importance_table


class UpliftRegressor(UpliftBase):
    def _make_estimator(self) -> CatBoostRegressor:
        """
        Create a CatBoostRegressor.

        :return: Created CatBoostRegressor
        """
        return CatBoostRegressor(
            iterations=self.uplift_iterations, verbose=False, allow_writing_files=False
        )

    def predict(self, x: pd.DataFrame) -> np.ndarray:
        """
        Model prediction for the given features.

        :param x: Features of the observations for prediction
        :return: Predicted scores
        """
        return self._t.predict(x) - self._c.predict(x)


class UpliftClassifier(UpliftBase):
    def _make_estimator(self) -> CatBoostClassifier:
        """
        Create a CatBoostClassifier.

        :return: Created CatBoostClassifier
        """
        return CatBoostClassifier(
            iterations=self.uplift_iterations, verbose=False, allow_writing_files=False
        )

    def predict(self, x: pd.DataFrame) -> np.ndarray:
        """
        Model prediction for the given features.

        :param x: Features of the observations for prediction
        :return: Predicted scores
        """
        return self._t.predict_proba(x)[:, 1] - self._c.predict_proba(x)[:, 1]


class QuantileCVMapModelWrapper:
    """
    Model wrapper to map model predictions to uplifts observed for that prediction quantile.
    """

    def __init__(
        self,
        estimator,
        uplift_by_quantile: pd.DataFrame,
        q: float,
        expose_wrapped_model: bool,
    ):
        """
        Constructor for QuantileCVMapModelWrapper class.

        :param estimator: The trained model
        :param uplift_by_quantile: The calculated uplift for each quantile of different folds
        :param q: The quantile used for calibration mapping
        """
        self.estimator = estimator
        self.explainer = self.estimator.explainer
        self.features = self.estimator.features
        assert isinstance(uplift_by_quantile.index, pd.IntervalIndex)
        self.calibration_mapping = uplift_by_quantile.quantile(q, axis=1)
        self.expose_wrapped_model = expose_wrapped_model

    def predict(self, x: pd.DataFrame) -> np.ndarray:
        """
        Predict scores and perform calibration.

        :param x: Features of the observations to be scored
        :return: Calibrated prediction scores
        """
        raw_preds = pd.Series(self.estimator.predict(x))
        if self.expose_wrapped_model:
            return raw_preds
        else:
            return self.map_score(raw_preds)

    def map_score(self, raw_preds: pd.Series) -> np.ndarray:
        """
        Calibration mapping.

        :param raw_preds: Raw prediction from the trained model
        :return: Calibrated scores
        """
        if raw_preds.ndim != 1:
            raise ValueError(f"Expecting a vector, got {raw_preds.shape}")
        indexes = pd.cut(raw_preds, self.calibration_mapping.index)
        return self.calibration_mapping.loc[indexes].values


class AverageTakeEffectCalculator:
    """
    Calculator of the Average Take Effect in the treatment group.
    """

    def __init__(self, **kwargs):
        """
        Constructor for AverageTakeEffectCalculator class.
        """
        self._average_take_effect = None

    def fit(
        self,
        x: pd.DataFrame,
        y_regression: Union[pd.DataFrame, pd.Series],
        valid_y_flag: Union[pd.DataFrame, pd.Series],
        y_classification: Union[pd.DataFrame, pd.Series],
    ):
        """
        Calculate the Average Take Effect.

        :param x: Input features, this is actually not used in this function,
            just a placeholder for consistency of the format
        :param y_regression: Target variable
        :param valid_y_flag: Treatment indicator, it is named as a valid flag
            because only the treatment group will be used for calculation
        :param y_classification: Taker indicator
        """
        y_classification = y_classification[valid_y_flag == 1]
        y_regression = y_regression[valid_y_flag == 1]
        self._average_take_effect = (
            (y_regression * y_classification).sum() / y_classification.sum()
            - (y_regression * (1 - y_classification)).sum()
            / (1 - y_classification).sum()
        )

        return self

    def predict(self, *args) -> np.float64:
        """
        Returns the calculated Average Take Effect.

        :return: Calculated Average Take Effect
        """
        if self._average_take_effect is None:
            raise Exception("Please fit a model before calling the predict function !")
        return self._average_take_effect


class BinaryUpliftClassifier:
    """
    A variation of the Uplift Modeling approach using a single model.
    The target variables are adjusted so the model will optimise for
    the people with highest potential uplift.

    This is only applicable for classification based use-cases like 4G.
    """

    def __init__(self, uplift_iterations: int):
        """
        Constructor for BinaryUpliftClassifier class.

        :param uplift_iterations: Parameter `iterations` for CatBoost model
        """
        self.uplift_iterations = uplift_iterations
        self.features = None

        self._estimator = self._make_estimator()
        self.explainer = self._make_estimator()

    def _make_estimator(self) -> CatBoostClassifier:
        """
        Create a CatBoostClassifier.

        :return: Created CatBoostClassifier
        """
        return CatBoostClassifier(
            iterations=self.uplift_iterations, verbose=False, allow_writing_files=False
        )

    def fit(
        self,
        x: pd.DataFrame,
        y: Union[pd.DataFrame, pd.Series],
        w: Union[pd.DataFrame, pd.Series],
        *args,
    ):
        """
        Fit the model and explainer with training data.

        :param x: Training features
        :param y: Training targets
        :param w: Treatment indicators
        """
        self.features = x.columns.tolist()

        n_0, n_1 = (w == 0).sum(), (w == 1).sum()
        w = w.sample(n=n_0 + n_1, replace=True, weights=w.map({0: n_1 / n_0, 1: 1}))
        x, y = x.loc[w.index], y.loc[w.index]
        y = y * w + (y - 1) * (w - 1)

        self._estimator.fit(x.values, y.values)
        self.explainer.fit(x, y)
        return self

    def predict(self, x: pd.DataFrame) -> np.ndarray:
        """
        Model prediction for the given features.

        :param x: Features of the observations for prediction
        :return: Predicted scores
        """
        return 2 * self._estimator.predict_proba(x)[:, 1] - 1

    def get_feature_importance(self) -> pd.DataFrame:
        """
        Get feature importance from the explainer.

        :return: DataFrame of (feature_id, feature importance) pair sorted by feature importance
        """
        importance_table = self.explainer.get_feature_importance(prettified=True)
        return importance_table


class AttentiveWrapper:
    """
    A propensity wrapper that trains a `propensity to take` model, and multiply
    its prediction probability with the wrapped base model's prediction score.
    Note that the based model will be trained at the same time as the propensity
    model.

    Essentially, train Attentive(uplift_iterations, your_base_model) means
    1. Train propensity model
    2. Train wrapped base model
    3. Train explainer base on propensity model

    Then predict with Attentive(uplift_iterations, your_base_model) means
    1. Generate probability score with propensity model
    2. Generate uplift score with base model
    3. Multiply the two score as the final score
    """

    def __init__(
        self, uplift_iterations: int, base_model: Type,
    ):
        """
        Constructor for AttentiveWrapper class.

        :param uplift_iterations: Parameter `iterations` for CatBoost model
        :param base_model: Base model to be wrapped
        """
        self.uplift_iterations = uplift_iterations
        self.features = None

        self.model = base_model(uplift_iterations=uplift_iterations)
        self.attentive_filter = self._make_filter()
        self.explainer = self._make_explainer()

    def _make_filter(self) -> CatBoostClassifier:
        """
        Create a CatBoostClassifier based propensity model.

        :return: A CatBoostClassifier model
        """
        return CatBoostClassifier(
            iterations=self.uplift_iterations, verbose=False, allow_writing_files=False
        )

    def _make_explainer(self) -> CatBoostClassifier:
        """
        Create an explainer model with CatBoostClassifier.

        :return: A CatBoostClassifier based explainer
        """
        return CatBoostClassifier(
            iterations=self.uplift_iterations, verbose=False, allow_writing_files=False
        )

    def fit(
        self,
        x: pd.DataFrame,
        y_regression: Union[pd.DataFrame, pd.Series],
        valid_y_flag: Union[pd.DataFrame, pd.Series],
        y_classification: Union[pd.DataFrame, pd.Series],
    ):
        """
        Fit the wrapper and base model.

        :param x: Input features
        :param y_regression: Target variable
        :param valid_y_flag: Treatment indicator, it is named as a valid flag
            because only the treatment group will be used for propensity model
            training
        :param y_classification: Taker indicator
        """
        # Stage 1, train the attentive filter
        x_filter = x[valid_y_flag == 1]
        y_filter = y_classification[valid_y_flag == 1]
        self.features = x.columns.tolist()
        self.attentive_filter.fit(x_filter.values, y_filter.values)
        self.explainer.fit(x_filter, y_filter)

        # stage 2, fit the base model, y_classification is not used for uplift models
        self.model.fit(x, y_regression, valid_y_flag, y_classification)
        return self

    def predict(self, x) -> np.ndarray:
        """
        Model prediction for the given features.

        :param x: Features of the observations for prediction
        :return: Predicted scores
        """
        uplift = self.model.predict(x)
        weights = self.attentive_filter.predict_proba(x)[:, 1]
        return uplift * weights

    def get_feature_importance(self) -> pd.DataFrame:
        """
        Get feature importance from the explainer.

        :return: DataFrame of (feature_id, feature importance) pair sorted by feature importance
        """
        importance_table = self.explainer.get_feature_importance(prettified=True)
        return importance_table
