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

from .create_mytsel_revenue_weekly import create_mytsel_pkg_revenue_weekly
from .create_revenue_alt_weekly import create_revenue_alt_weekly
from .create_revenue_weekly import create_revenue_weekly


def create_pipeline(**kwargs) -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to aggregate `Revenue` data into a [MSISDN, Weekstart]
    granularity.  This function will be executed as part of the `aggregation` layer. The pipeline takes a series of nodes; with tags to
    help uniquely identify it. More information on running can be found here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the `Revenue` nodes to execute the `aggregation` layer.
    """

    return Pipeline(
        [
            node(
                func=create_revenue_weekly,
                inputs=[
                    "l1_itdaml_dd_cdr_payu_prchse_all",
                    "l1_itdaml_dd_cdr_pkg_prchse_all",
                    "l1_usage_rcg_prchse_abt_dd",
                ],
                outputs=None,  # l2_revenue_weekly_data
                name="revenue_weekly_aggregation",
                tags=["agg_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=create_mytsel_pkg_revenue_weekly,
                inputs=["l1_mck_mytsel_pack",],
                outputs=None,  # l2_revenue_mytsel_weekly
                name="mytsel_revenue_weekly_aggregation",
            ),
            node(
                func=create_revenue_alt_weekly,
                inputs=["l1_merge_revenue_dd", "l1_airtime_loan_id"],
                outputs=None,  # l2_revenue_alt_weekly_data
                name="revenue_alt_weekly_aggregation",
                tags=["agg_scoring_pipeline", "scoring_pipeline"],
            ),
        ],
        tags=[
            "agg_revenue",
            # "revenue_pipeline",
            "de_aggregation_pipeline",
            # "de_pipeline",
        ],
    )
