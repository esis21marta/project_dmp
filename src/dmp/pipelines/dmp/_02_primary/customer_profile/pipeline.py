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

from src.dmp.pipelines.dmp._02_primary.filter_partner_data.filter_partner_data import (
    filter_partner_data,
)
from src.dmp.pipelines.dmp._02_primary.scaffold.fill_time_series import fill_time_series


def create_pipeline(**kwargs) -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to create the primary layer for `customer_profile` after the data
    has been aggregated to weekly-level. This function will be executed as part of the `primary` layer. The pipeline takes a series of nodes; with tags to
    help uniquely identify it. More information on running can be found here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the `customer_profile` nodes to execute the `primary` layer.
    """
    return Pipeline(
        [
            node(
                func=filter_partner_data,
                inputs=[
                    "partner_msisdns",
                    "l2_customer_profile_weekly",
                    "params:filter_partner_data",
                ],
                outputs="l3_customer_profile_partner",
                name="customer_profile_filtered_data",
                tags=["prm_scoring_pipeline", "scoring_pipeline",],
            ),
            node(
                func=fill_time_series(),
                inputs=["l1_weekly_scaffold", "l3_customer_profile_partner"],
                outputs="l4_customer_profile_scaffold_weekly",
                name="fill_customer_profile_time_series",
            ),
            node(
                func=filter_partner_data,
                inputs=[
                    "partner_msisdns",
                    "l2_customer_expiry_weekly",
                    "params:filter_partner_data",
                ],
                outputs="l3_customer_expiry_partner",
                name="customer_expiry_filtered_data",
            ),
            node(
                func=filter_partner_data,
                inputs=[
                    "partner_msisdns",
                    "l2_customer_profile_nsb_weekly",
                    "params:filter_partner_data",
                ],
                outputs="l3_customer_profile_nsb_partner",
                name="customer_profile_nsb_filtered_data",
            ),
            node(
                func=fill_time_series(),
                inputs=["l1_weekly_scaffold_6m", "l3_customer_profile_nsb_partner",],
                outputs="l4_customer_profile_nsb_scaffold_weekly",
                name="fill_customer_profile_nsb_time_series",
            ),
            node(
                func=filter_partner_data,
                inputs=[
                    "partner_msisdns",
                    "l2_customer_profile_points_redeemed_weekly",
                    "params:filter_partner_data",
                ],
                outputs="l3_customer_profile_redeemed_points_partner",
                name="redeemed_points_filtered_data",
            ),
            node(
                func=fill_time_series(),
                inputs=[
                    "l1_weekly_scaffold_6m",
                    "l3_customer_profile_redeemed_points_partner",
                ],
                outputs="l4_customer_profile_redeemed_points_scaffold_weekly",
                name="fill_redeemed_points_time_series",
            ),
            node(
                func=filter_partner_data,
                inputs=[
                    "partner_msisdns",
                    "l2_customer_profile_sellthru_weekly",
                    "params:filter_partner_data",
                ],
                outputs="l3_customer_profile_sellthru_partner",
                name="customer_profile_sellthru_filtered_data",
            ),
            node(
                func=fill_time_series(),
                inputs=["l1_weekly_scaffold", "l3_customer_profile_sellthru_partner"],
                outputs="l4_customer_profile_sellthru_scaffold_weekly",
                name="fill_customer_profile_sellthru_time_series",
            ),
        ],
        tags=[
            "prm_customer_profile",
            "customer_profile_pipeline",
            "de_primary_pipeline",
            "de_pipeline",
        ],
    )
