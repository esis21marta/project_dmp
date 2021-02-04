import logging
import pickle
from typing import List, Union

import numpy as np
import pandas as pd
from mlflow import pyfunc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PythonModel(pyfunc.PythonModel):
    """
    The class represents the interface class for scoring ml models.
    """

    def __init__(self, model_path: str):
        """
        Constructor of class Scorer.

        :param model_path: The location of the model saved
        """
        self.model_path = model_path
        self.py_model = None

    def load_context(self, context: pyfunc.model.PythonModelContext) -> None:
        """
        The function loads the model using context object.

        :param context: The object refers to `PythonModelContext` of mlflow.pyfunc.model module
        """
        model_path = context.artifacts[self.model_path]
        try:
            with open(model_path, "rb") as f:
                model = pickle.load(f)
        except pickle.UnpicklingError:
            logger.error(f"Error while unpickling the file {model_path}")

        self.py_model = model["model"]

    def predict(
        self,
        context: pyfunc.model.PythonModelContext,
        model_input: Union[pd.DataFrame, np.ndarray],
    ) -> Union[pd.DataFrame, np.ndarray]:
        """
        The model's predict function.

        :param context: The object refers to `PythonModelContext` of mlflow.pyfunc.model module
        :param model_input: The input pandas dataframe or numpy 2D array for ml model
        :return: Dataframe or numpy 2D array
        """
        return self.py_model.predict(model_input)

    def features(self) -> List[str]:
        """
        Get model's features.

        :return: List of features of the model
        """
        return self.py_model.features
