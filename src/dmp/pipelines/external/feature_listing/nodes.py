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

import re
from typing import List

import pandas as pd


def get_features_list(df: pd.DataFrame, feature_column_prefix: str,) -> List[str]:
    """
    Creates a list of columns that start with the feature_column_prefix.

    :param df: Input DataFrame
    :param feature_column_prefix: Prefix of all columns containing features
    :return: List of all features names
    """
    return [col for col in df.columns if col.startswith(feature_column_prefix)]


def format_features_list(features_list: List[str],) -> str:
    """
    Formats the feature list before writting it to a text file.

    :param features_list: List of all features names
    :return: Feature list in yml-like format
    """
    formatted_list = ""
    for feature in features_list:
        formatted_list += f'    - "{feature}"\n'
    return formatted_list


def get_longest_features(features_list: List[str]) -> List[str]:
    """
    Get the longest features for window-based features. And if there are non-window features, return them as well.

    :param features_list: List of all feature names
    :return: List of features with longest period and non-window features
    """
    period_expr = "^[0-9]{1,2}[w,m]"

    window_features = [
        fea for fea in features_list if re.search(period_expr, fea.split("_")[-1])
    ]
    non_window_features = [fea for fea in features_list if fea not in window_features]

    grouped_features = {}
    for fea in window_features:
        feature_name = "_".join(fea.split("_")[:-1])
        grouped_features.setdefault(feature_name, [])
        grouped_features[feature_name].append(fea)

    longest_features = []
    for key, value in grouped_features.items():
        longest_fea = ""
        for fea in value:
            period = {"w": 10, "m": 100}
            if longest_fea == "":
                longest_fea = fea
            else:
                curr_period = longest_fea.split("_")[-1]
                curr_num = int(curr_period[0:2])
                curr_unit = curr_period[-1]

                fea_period = fea.split("_")[-1]
                fea_num = int(fea_period[0:2])
                fea_unit = fea_period[-1]

                if fea_num * period[fea_unit] > curr_num * period[curr_unit]:
                    longest_fea = fea
        longest_features.append(longest_fea)
    return longest_features + non_window_features
