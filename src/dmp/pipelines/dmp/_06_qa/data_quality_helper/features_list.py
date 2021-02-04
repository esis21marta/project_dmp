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

from typing import List

import pyspark
from kedro.config import ConfigLoader


def get_features_used_list(df: pyspark.sql.DataFrame, feature_mode: str) -> List[str]:
    """
    Returns a list of features to be used for QA metrics.

    Returns:
        List of feature columns.
    """

    if feature_mode == "all":
        return [i for i in df.columns if i.startswith("fea_")]

    elif feature_mode == "file_list":
        conf_loader = ConfigLoader("conf/dmp/scoring")
        conf_parameters = conf_loader.get("parameters*", "parameters*/**")
        return list(
            set(df.columns).intersection(set(conf_parameters["output_features"]))
        )

    else:
        raise Exception(
            "Please pass a valid `features_mode` parameter from ('file_list', 'all')."
        )
