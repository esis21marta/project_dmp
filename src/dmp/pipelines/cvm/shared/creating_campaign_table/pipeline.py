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

from src.dmp.pipelines.cvm.shared.creating_campaign_table.campaign_table import (
    aggregate_cms_ts,
    cmp_and_wording,
    deduplicate_cmp_names,
    joining_wordings_stages_and_trackingstream,
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
          return
     """
    return Pipeline(
        [
            node(
                func=cmp_and_wording,
                inputs="t_stg_cms_definition_contact",
                outputs="sh_campaign_table__wrd",
                name="cmp_and_wording",
            ),
            node(
                func=deduplicate_cmp_names,
                inputs="t_stg_cms_definition_identification",
                outputs="sh_unique_cmp_names",
                name="deduplicate_cmp_names",
            ),
            node(
                func=aggregate_cms_ts,
                inputs="t_stg_cms_trackingstream",
                outputs="sh_cms_ts_aggr",
                name="filter_table_cms_trackingstream",
            ),
            node(
                func=joining_wordings_stages_and_trackingstream,
                inputs=[
                    "sh_cms_ts_aggr",
                    "sh_campaign_table__wrd",
                    "sh_unique_cmp_names",
                ],
                outputs="sh_campaign_aggregated_table",
                name="joining_wordings_stages_and_trackingstream",
            ),
        ]
    )
