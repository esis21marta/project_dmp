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

from src.dmp.pipelines.cvm.shared.downsampling_by_wording.downsample_by_wording import (
    create_downsampling_table,
    downsample_by_wording,
    prepare_campaigns_table,
)


def create_pipeline():
    """
    Creates a pipeline for the campaigns.

    Args:
        The output is a table with the following columns:
        - campaignid
        - stage
        - name
        - wording

    Returns:
    """
    return Pipeline(
        [
            node(
                func=prepare_campaigns_table,
                inputs=[
                    "sh_campaign_aggregated_table",
                    "t_stg_cms_trackingstream",
                    "params:master_table",
                ],
                outputs="sh_trackingstream_wording",
                name="join_trackingstream_wording",
            ),
            node(
                func=create_downsampling_table,
                inputs=["sh_trackingstream_wording", "params:downsmpl"],
                outputs="sh_downsampling_table",
                name="create_downsampling_table",
            ),
            node(
                func=downsample_by_wording,
                inputs=["sh_trackingstream_wording", "sh_downsampling_table"],
                outputs="sh_trackingstream_wording_downsmpl",
                name="downsample_by_wording",
            ),
        ]
    )
