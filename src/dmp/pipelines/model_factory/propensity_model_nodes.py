import json
import logging
from typing import Iterable, List, Tuple, Union

import matplotlib.pyplot as plt
import pai
import pandas as pd
import shap
from catboost import CatBoostClassifier, Pool
from category_encoders.count import CountEncoder
from pyspark.sql import DataFrame
from sklearn import model_selection
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.ensemble import RandomForestClassifier
from sklearn.inspection import plot_partial_dependence
from sklearn.metrics import roc_auc_score, roc_curve
from sklearn.pipeline import Pipeline, make_pipeline

from src.dmp.pipelines.model_factory.common_functions import telkomsel_style
from src.dmp.pipelines.model_factory.consts import TARGET_COL

log = logging.getLogger(__name__)


def train_test_split(
    df: pd.DataFrame, train_ratio: float
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split to train and test dataset - node wrapper around `sklearn`'s train test split.

    :param df: Input DataFrame
    :param train_ratio: What portion of `df` should go to train dataset
    :returns: Train and test dataset with target
    """
    return model_selection.train_test_split(
        df, train_size=train_ratio, random_state=2137
    )


class ColumnDropper(BaseEstimator, TransformerMixin):
    """ Used to drop columns in sklearn pipeline"""

    def __init__(self, cols_to_drop: Iterable[str]):
        self.cols_to_drop = list(cols_to_drop)

    def fit(self, X=None, y=None):
        return self

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop(self.cols_to_drop, axis=1)


class ColumnChecker(BaseEstimator, TransformerMixin):
    """ Used to assert that column are the same and in the same order in both train and test"""

    def __init__(self, columns: List[str] = None, use_pai: bool = True):
        self.columns = columns
        self.use_pai = use_pai

    def fit(self, X: pd.DataFrame, y=None):
        if self.columns is None:
            self.columns = list(X.columns.values)
        else:
            missing_cols = [col for col in self.columns if col not in X.columns.values]
            assert not missing_cols, f"Following columns are missing: {missing_cols}"
        if self.use_pai:
            pai.log_artifacts({"features": self.columns})
        return self

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        missing_cols = [col for col in self.columns if col not in df.columns.values]
        assert not missing_cols, f"Following columns are missing: {missing_cols}"
        return df[self.columns]


def pick_features_from_pai(
    df: DataFrame, run_id: str, msisdn_column: str, weekstart_column: str
) -> DataFrame:
    """
    Selects columns using columns saved in pai

    :param df: Input DataFrame
    :param run_id: PAI run id to use
    :param msisdn_column: Column containing the msisdns
    :param weekstart_column: Column containing the weekstarts
    :returns: DataFrame with selected columns
    """
    features_used = pai.load_artifact(run_id, "features")
    if type(features_used) is str:
        with open(features_used) as f:
            features_used = json.load(f)
    cols_to_pick = [msisdn_column, weekstart_column] + features_used
    return df.select(*cols_to_pick)


def preprocess_training_data(
    df_train: pd.DataFrame, blacklisted_cols: Iterable[str],
) -> Tuple[pd.DataFrame, pd.Series, Pipeline, List[str]]:
    """
    Prepares the data for training and saves preprocessing pipeline to pai. Should be used
    for last - mile operations like dropping the columns or converting types.

    :param df_train: Training DataFrame
    :param blacklisted_cols: List of columns to drop from training
    :returns: Preprocessed training data, target, features used and fitted preprocessing pipeline
    """
    log.info("Preprocessing training data")
    X_train = df_train.drop(TARGET_COL, axis=1)
    y_train = df_train[TARGET_COL]
    preprocessing_pipeline = make_pipeline(
        ColumnDropper(blacklisted_cols), ColumnChecker(), CountEncoder(),
    )
    pipeline_fitted = preprocessing_pipeline.fit(X_train)
    pai.log_artifacts({"preprocessing_pipeline": pipeline_fitted})
    return (
        pipeline_fitted.transform(X_train),
        y_train,
        pipeline_fitted,
        preprocessing_pipeline[1].columns,
    )


def preprocess_scoring_data(
    df_score: pd.DataFrame, preprocessing_pipeline: Pipeline = None, run_id: str = None
) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Prepares data for scoring using fitted preprocessing pipeline.

    :param df_score: Data to be preprocessed
    :param preprocessing_pipeline: Fitted sklearn preprocessing pipeline
    :param run_id: PAI run id, if preprocessing_pipeline is None then this one is used
        to load pipeline from PAI
    :returns: Preprocessed scoring dataset and target
    """
    log.info("Preprocessing scoring data")
    if preprocessing_pipeline is None:
        preprocessing_pipeline = pai.load_artifact(run_id, "preprocessing_pipeline")
    if TARGET_COL in df_score.columns:
        X_score = df_score.drop(TARGET_COL, axis=1)
        y_score = df_score[TARGET_COL]
    else:
        X_score = df_score
        y_score = pd.Series()  # Cannot be none, hence the choice
    return preprocessing_pipeline.transform(X_score), y_score


def train_rf_model(
    X_train: pd.DataFrame, y_train: pd.Series
) -> Union[BaseEstimator, CatBoostClassifier]:
    """
    Trains RandomForestClassifier on given dataset.

    :param X_train: Preprocessed training DataFrame
    :param y_train: Target to fit to
    :returns: Fitted RandomForestClassifier model
    """

    log.info("Fitting the model")
    rfc = RandomForestClassifier()
    logging.info(f"Using model type: {type(rfc)}")
    rfc.fit(X_train, y_train)
    pai.log_model(rfc)
    pai.log_artifacts({"training_row_number": X_train.shape[0]})
    return rfc


def train_catboost_model(
    X_train: pd.DataFrame, y_train: pd.Series
) -> Union[BaseEstimator, CatBoostClassifier]:
    """
    Trains CatBoost model on given dataset.

    :param X_train: Preprocessed training DataFrame
    :param y_train: Target to fit to
    :returns: Fitted RandomForestClassifier model
    """

    log.info("Fitting the model")
    training_pool = Pool(data=X_train, label=y_train)
    cb = CatBoostClassifier(iterations=200, verbose=False, allow_writing_files=False)
    logging.info(f"Using model type: {type(cb)}")
    cb.fit(training_pool)
    pai.log_model(cb)
    pai.log_artifacts({"training_row_number": X_train.shape[0]})
    return cb


def score_rf_model(
    X_score: pd.DataFrame, rf_model: RandomForestClassifier
) -> pd.Series:
    """
    Scores saved model.

    :param rf_model: Trained model
    :param X_score: Dataset to score
    :returns: Series of predictions
    """
    log.info("Scoring model")
    return rf_model.predict_proba(X_score)[:, 1]


def score_catboost_model(
    X_score: pd.DataFrame, cb_model: CatBoostClassifier = None, run_id: str = None
) -> pd.Series:
    """
    Scores saved model.

    :param run_id: Run id, will be used to load model from PAI if cb_model is None
    :param cb_model: Trained model
    :param X_score: Dataset to score
    :returns: Series of predictions
    """
    log.info("Scoring model")
    if cb_model is None:
        cb_model = pai.load_model(run_id)
    return cb_model.predict_proba(X_score)[:, 1]


def join_scores_with_msisdns(
    df: pd.DataFrame, score: pd.Series, msisdn_column: str,
) -> pd.DataFrame:
    """
    Joins produced scores and msisdns

    :param df: Input DataFrame with msisdns
    :param score: Series with predictions, must have the same order as df
    :param msisdn_column: Column containing the msisdns
    :returns: DataFrame of predictions and scores
    """
    msisdns = df[msisdn_column]
    msisdns_scores = pd.DataFrame({"msisdn": msisdns, "score": score})
    pai.log_artifacts({"msisdns_scores": msisdns_scores})
    return msisdns_scores


@telkomsel_style()
def _save_roc(true_values: pd.Series, pred_scores: pd.Series) -> None:
    """
    Saves ROC to pai.

    :param true_values: Series with true 0-1 values
    :param pred_scores: Series with prediction scores
    :returns: None
    """
    fpr, tpr, _ = roc_curve(true_values, pred_scores)
    roc_plot = plt.figure(figsize=(16, 9))
    plt.plot(fpr, tpr, label="ROC curve")
    plt.plot([0, 1], [0, 1], linestyle="--")
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel("FPR", fontsize=15)
    plt.ylabel("TPR", fontsize=15)
    plt.title("Receiver operating characteristic")
    plt.tight_layout()
    pai.log_artifacts({"ROC_curve": roc_plot})


def _save_auc(true_values: pd.Series, pred_scores: pd.Series) -> None:
    """
    Saves AUC to pai.

    :param true_values: Series with true 0-1 values
    :param pred_scores: Series with prediction scores
    :returns: None
    """
    auc = roc_auc_score(true_values, pred_scores)
    pai.log_metrics({"AUC": auc})


def save_feature_importance_rf(
    model_fitted: RandomForestClassifier, feature_used: List[str]
) -> None:
    """
    Saves features and their importance to PAI

    :param model_fitted: Trained sklearn model
    :param feature_used: List of features used
    :returns: None
    """
    pai.log_features(
        features=feature_used, importance=model_fitted.feature_importances_
    )


def save_feature_importance_catboost(
    model_fitted: CatBoostClassifier, feature_used: List[str]
) -> None:
    """
    Saves features and their importance to PAI

    :param model_fitted: Trained CatBoost model
    :param feature_used: List of features used
    :returns: None
    """
    pai.log_features(
        features=feature_used, importance=model_fitted.get_feature_importance()
    )


def create_shap_summary_plot(
    model_fitted: CatBoostClassifier,
    X_train: pd.DataFrame,
    feature_names: str,
    n: int = 20,
) -> None:
    """
    Creates shap summary plot for top n features and saves it to pai

    :param feature_names: Feature names to put on a plot
    :param n: Number of features to visualize
    :param model_fitted: Trained model
    :param X_train: DataFrame used for training
    """

    explainer = shap.TreeExplainer(model_fitted)
    shap_values = explainer.shap_values(X_train)
    shap_plot = plt.figure(figsize=(16, 9))
    shorter_feature_names = [col[4:40] for col in feature_names]
    shap.summary_plot(
        shap_values,
        max_display=n,
        show=False,
        feature_names=shorter_feature_names,
        features=X_train,
    )
    plt.tight_layout(rect=[0, 0, 0.85, 1])
    pai.log_artifacts({"shap_summary": shap_plot})


def validate_model(y_score: pd.Series, pred_scores: pd.Series) -> None:
    """
    Saves validation metrics to pai.

    :param y_score: Target from scoring data
    :param pred_scores: Produced prediction scores.
    :returns: None
    """
    true_values = y_score
    _save_auc(true_values, pred_scores)
    _save_roc(true_values, pred_scores)


@telkomsel_style()
def create_partial_dependence_plots(
    model_fitted: BaseEstimator,
    df: pd.DataFrame,
    features_used: List[str],
    pdp_features: Iterable[Iterable[str]],
) -> None:
    """
    Creates partial dependence plots and saves them to pai

    :param features_used: List of features used in the model
    :param model_fitted: Trained sklearn model
    :param df: Training DataFrame
    :param pdp_features: Features to create pdp plots for
    """
    for features in pdp_features:
        log.info(f"Creating partial dependence plot for {features}")
        features_not_found = [
            feature for feature in features if feature not in features_used
        ]
        if features_not_found:
            log.info(f"Features {features_not_found} not found, PDP generation skipped")
            continue
        feature_inds = tuple([features_used.index(feature) for feature in features])
        pdp_plot = plt.figure(figsize=(16, 9))
        plot_partial_dependence(model_fitted, df, feature_inds, fig=pdp_plot)
        features_str = " and ".join(features)
        plt.title(f"PDP for {features_str}")
        pai.log_artifacts({f"pdp_{features_str}": pdp_plot})
