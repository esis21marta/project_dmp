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

from src.dmp.pipelines.external.scoring.nodes import (
    categorical_none_to_nan_string,
    post_process,
    score,
)


def create_pipeline():
    return Pipeline(
        [
            node(
                categorical_none_to_nan_string,
                ["to_score", "params:pai_run"],
                "with_cleaned_nans",
                tags=["pai:no_logging"],
            ),
            node(
                score,
                [
                    "with_cleaned_nans",
                    "params:pai_run",
                    "params:score_column_name",
                    "params:model_type",
                    "params:predict_probability",
                ],
                "with_raw_score",
                tags=["pai:no_logging"],
            ),
            node(
                post_process,
                [
                    "with_raw_score",
                    "params:score_column_name",
                    "params:binned",
                    "params:binned_score_column_name",
                    "params:bin_thresholds",
                    "params:post_processing",
                ],
                "with_score",
            ),
        ]
    )
