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

from src.dmp.pipelines.external.common.python.utils import rename_columns, to_datetime
from src.dmp.pipelines.external.hcid.dataset_generation.nodes import clean_msisdn


def create_pipeline():
    return Pipeline(
        [
            node(
                func=to_datetime,
                inputs=[
                    "hcid_training",
                    "params:date_column",
                    "params:date_format_training",
                ],
                outputs="hcid_training_date_corrected",
            ),
            node(
                func=rename_columns,
                inputs=[
                    "hcid_training_date_corrected",
                    "params:column_mapping_training",
                ],
                outputs="hcid_training_column_renamed",
            ),
            node(
                func=clean_msisdn,
                inputs=["hcid_training_column_renamed", "params:msisdn_column",],
                outputs="hcid_training_processed",
            ),
            node(
                func=to_datetime,
                inputs=["hcid_test", "params:date_column", "params:date_format_test",],
                outputs="hcid_test_date_corrected",
            ),
            node(
                func=rename_columns,
                inputs=["hcid_test_date_corrected", "params:column_mapping_test",],
                outputs="hcid_test_column_renamed",
            ),
            node(
                func=clean_msisdn,
                inputs=["hcid_test_column_renamed", "params:msisdn_column",],
                outputs="hcid_test_processed",
            ),
        ],
        tags=["process_dataset_hcid"],
    )
