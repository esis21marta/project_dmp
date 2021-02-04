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

# in-built libraries
from typing import Any, Dict, List, Union

# third-party libraries
import pandas
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV


def instantiate_gridsearchcv(
    features: pandas.DataFrame,
    target: pandas.Series,
    grid_search_params: Dict[str, Any],
) -> List[Union[pandas.DataFrame, Dict[str, Any], pandas.DataFrame]]:
    """Perform hyperparameter search using cross-validated grid search

    Args:
        features: Features of full data set
        target: Target variable of full dataset
        grid_search_params: Parameters to pipe to GridSearchCV as arguments

    Returns:
        Hyperparameter search results, best parameters and feature importances
    """

    # wrap grid search cross-validation object over model algorithm
    clf = GridSearchCV(
        RandomForestClassifier(),
        param_grid=grid_search_params["param_grid"],
        cv=grid_search_params["cv"],
        scoring=grid_search_params["scoring"],
        refit=grid_search_params["refit"],
        verbose=grid_search_params["gridsearchcv_verbose"],
        n_jobs=grid_search_params["gridsearchcv_n_jobs"],
    )

    # fit features and target variable
    clf.fit(features, target)

    # save and output grid search hyperparameters and results to csv
    cv_results = pandas.DataFrame(clf.cv_results_)
    cv_best_params = clf.best_params_

    # obtain feature importances
    feature_importances = pandas.DataFrame(
        {
            "feature": features.columns,
            "importance": clf.best_estimator_.feature_importances_,
        }
    )
    feature_importances = feature_importances.sort_values(
        by="importance", axis=0, ascending=False
    )

    return cv_results, cv_best_params, feature_importances
