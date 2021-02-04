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

from src.dmp.pipelines.dmp._01_aggregation.customer_profile.create_nsb_weekly import (
    agg_nsb_to_weekly,
)
from src.dmp.pipelines.dmp._01_aggregation.customer_profile.create_weekly_base_subs import (
    agg_customer_expiry_to_weekly,
)
from src.dmp.pipelines.dmp._01_aggregation.customer_profile.create_weekly_loyalty_redeemed import (
    agg_redeemed_points_to_weekly,
)
from src.dmp.pipelines.dmp._01_aggregation.customer_profile.create_weekly_multidim_table import (
    agg_customer_profile_to_weekly,
)
from src.dmp.pipelines.dmp._01_aggregation.customer_profile.create_weekly_outlet_dim import (
    agg_outlet_to_weekly,
)
from src.dmp.pipelines.dmp._01_aggregation.customer_profile.create_weekly_sellthru import (
    agg_customer_profile_sellthru_to_weekly,
)


def create_pipeline() -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to aggregate `customer profile` data into a [MSISDN, Weekstart]
    granularity.  This function will be executed as part of the `aggregation` layer . The pipeline takes a series of nodes; with tags to
    help uniquely identify it. More information on running can be found here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the `customer profile` nodes to execute the `aggregation` layer.
    """
    return Pipeline(
        [
            node(
                func=agg_customer_profile_to_weekly,
                inputs="l1_cb_multidim",
                outputs=None,  # l2_customer_profile_weekly
                name="customer_profile_aggregation",
                tags=["agg_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=agg_customer_expiry_to_weekly,
                inputs="l1_subs_ocs_sse",
                outputs=None,  # l2_customer_expiry_weekly
                name="customer_expiry_aggregation",
            ),
            node(
                func=agg_nsb_to_weekly,
                inputs="l1_base_nsb",
                outputs=None,  # l2_customer_profile_nsb_weekly
                name="customer_nsb_aggregation",
            ),
            node(
                func=agg_redeemed_points_to_weekly,
                inputs="l1_dbi_redeem_poin",
                outputs=None,  # l2_customer_profile_points_redeemed_weekly
                name="customer_redeem_points_aggregation",
            ),
            node(
                func=agg_outlet_to_weekly,
                inputs="l1_digipos_outlet_reference_dd",
                outputs=None,  # l2_outlet_weekly
                name="outlet_aggregation",
            ),
            node(
                func=agg_customer_profile_sellthru_to_weekly,
                inputs="l1_digipos_sp_migration",
                outputs=None,  # l2_customer_profile_sellthru_weekly
                name="customer_profile_sellthru_aggregation",
            ),
        ],
        tags=[
            "agg_customer_profile",
            # "customer_profile_pipeline",
            "de_aggregation_pipeline",
            # "de_pipeline"
        ],
    )
