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

from src.dmp.pipelines.dmp._01_aggregation.handset.create_handset_lookup_data import (
    create_handset_lookup_data,
)
from src.dmp.pipelines.dmp._01_aggregation.handset.create_handset_weekly_data import (
    create_handset_weekly_data,
)


def create_pipeline() -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to aggregate `handset device` data into a [MSISDN, Weekstart]
    granularity.  This function will be executed as part of the `aggregation` layer. The pipeline takes a series of nodes; with tags to
    help uniquely identify it. More information on running can be found here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the `handset device` nodes to execute the `aggregation` layer.
    """
    return Pipeline(
        [
            node(
                func=create_handset_lookup_data,
                inputs="l1_handset_dim_lookup",
                outputs="l1_handset_lookup_data",
                name="create_handset_lookup_data",
            ),
            node(
                func=create_handset_weekly_data,
                inputs=["l1_handset_dd", "l1_handset_lookup_data"],
                outputs=None,  # l2_handset_weekly_aggregated
                name="create_handset_weekly_data",
            ),
        ],
        tags=[
            "agg_handset",
            # "handset_pipeline",
            "de_aggregation_pipeline",
            # "de_pipeline"
            "agg_scoring_pipeline",
            "scoring_pipeline",
        ],
    )
