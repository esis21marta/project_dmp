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

from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.external.generate_psi_baseline.nodes import (
    calculate_csi_baseline,
    calculate_psi_baseline,
)


def create_pipeline():
    return Pipeline(
        [
            node(
                calculate_psi_baseline,
                [
                    "master",
                    "params:score_column_name",
                    "params:score_num_of_bins",
                    "params:filter_dataset",
                    "params:filter_column",
                    "params:first_period",
                    "params:last_period",
                ],
                "psi_baseline",
                name="calculate_psi_baseline",
            ),
            node(
                calculate_csi_baseline,
                [
                    "master",
                    "params:columns_of_interest",
                    "params:class_column_name",
                    "params:max_bin",
                    "params:filter_dataset",
                    "params:filter_column",
                    "params:first_period",
                    "params:last_period",
                ],
                "csi_baseline",
                name="calculate_csi_baseline",
            ),
        ]
    )
