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

import json
from typing import Any

import pai
import pandas as pd


def load_json_artifact(pai_run: str, artifact_name: str) -> Any:
    """
    Loads Python object from PAI artifact in JSON format.

    :param pai_run: Name of the PAI run contining the required artifact
    :param artifact_name: Name of the required artifact
    :return: Artifact as Python object
    """
    artifact_json = pai.load_artifact(pai_run, artifact_name)
    with open(artifact_json) as json_file:
        artifact = json.load(json_file)
    return artifact


def load_df_from_artifact(pai_run: str, artifact_name: str) -> pd.DataFrame:
    """
    Loads Pandas DataFrame from PAI artifact.

    :param pai_run: Name of the PAI run contining the required artifact
    :param artifact_name: Name of the required artifact
    :return: Artifact as Pandas DataFrame
    """
    artifact_path = pai.load_artifact(pai_run, artifact_name)
    return pd.read_csv(artifact_path, index_col=0)
