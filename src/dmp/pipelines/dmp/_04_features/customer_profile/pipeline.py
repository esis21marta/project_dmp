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

from src.dmp.pipelines.dmp._04_features.customer_profile.customer_nsb import (
    customer_nsb,
)
from src.dmp.pipelines.dmp._04_features.customer_profile.customer_profile import (
    customer_profile,
)
from src.dmp.pipelines.dmp._04_features.customer_profile.fea_customer_activation_outlet import (
    fea_customer_activation_outlet,
)
from src.dmp.pipelines.dmp._04_features.customer_profile.fea_customer_expiry import (
    fea_customer_expiry,
)
from src.dmp.pipelines.dmp._04_features.customer_profile.fea_customer_points import (
    fea_customer_points,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to create the feature layer for `Customer Profile` after the data
    has been run through the primary layer. This function will be executed as part of the `feature` layer. The pipeline takes a series of nodes; with tags to
    help uniquely identify it. More information on running can be found here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the `Customer Profile` nodes to execute the `feature` layer.
    """
    return Pipeline(
        [
            node(
                func=customer_profile,
                inputs=[
                    "l3_customer_profile_partner",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_customer_profile",
                name="fea_customer_profile",
                tags=["fea_scoring_pipeline", "scoring_pipeline",],
            ),
            node(
                func=fea_customer_expiry,
                inputs=[
                    "l3_customer_expiry_partner",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_customer_expiry",
                name="fea_customer_expiry",
            ),
            node(
                func=fea_customer_points,
                inputs=[
                    "l4_customer_profile_scaffold_weekly",
                    "l4_customer_profile_redeemed_points_scaffold_weekly",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_customer_points",
                name="fea_customer_points",
            ),
            node(
                func=customer_nsb,
                inputs=[
                    "l4_customer_profile_nsb_scaffold_weekly",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_customer_nsb",
                name="fea_customer_nsb",
            ),
            node(
                func=fea_customer_activation_outlet,
                inputs=[
                    "l4_customer_profile_sellthru_scaffold_weekly",
                    "l2_outlet_weekly",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_customer_activation_outlet",
                name="fea_customer_activation_outlet",
            ),
        ],
        tags=[
            "fea_customer_profile",
            "customer_profile_pipeline",
            "de_feature_pipeline",
            "de_pipeline",
        ],
    )
