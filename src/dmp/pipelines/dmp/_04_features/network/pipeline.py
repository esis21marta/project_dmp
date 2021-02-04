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

from src.dmp.pipelines.dmp._04_features.network.fea_map_msisdn_to_tutela_network import (
    fea_map_msisdn_to_tutela_network,
)
from src.dmp.pipelines.dmp._04_features.network.fea_network import fea_network
from src.dmp.pipelines.dmp._04_features.network.fea_network_tutela import (
    fea_network_tutela,
)


def create_pipeline(**kwargs) -> Pipeline:
    """ Create the Network Feature Pipeline.

    :param kwargs: Ignore any additional arguments added in the future.

    :return: A Pipeline object containing all the Network Feature Nodes.
    """

    return Pipeline(
        [
            node(
                func=fea_network("msisdn"),
                inputs=[
                    "l2_network_msisdn_scaffold_weekly",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l4_fea_network_msisdn",
                name="fea_network_msisdn",
                tags=[
                    "fea_network_msisdn",
                    "network_msisdn_pipeline",
                    "fea_scoring_pipeline",
                    "scoring_pipeline",
                ],
            ),
            node(
                func=fea_network("lac_ci"),
                inputs=[
                    "l2_network_lacci_scaffold_weekly",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l4_fea_network_lacci",
                name="fea_network_lacci",
                tags=["fea_network_lacci", "network_lacci_pipeline"],
            ),
            node(
                func=fea_network("lac"),
                inputs=[
                    "l2_network_lac_scaffold_weekly",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l4_fea_network_lac",
                name="fea_network_lac",
                tags=["fea_network_lacci", "network_lacci_pipeline"],
            ),
            node(
                func=fea_network_tutela,
                inputs=[
                    "l2_network_tutela_location_weekly_aggregated",
                    # "l5_fea_customer_profile",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l4_fea_network_tutela",
                name="fea_network_tutela",
            ),
            node(
                func=fea_map_msisdn_to_tutela_network,
                inputs=[
                    "l4_customer_profile_scaffold_weekly",
                    "l4_fea_network_tutela",
                ],
                outputs="l5_fea_map_msisdn_to_tutela_network",
                name="fea_map_msisdn_to_tutela_network",
            ),
        ],
        tags=["fea_network", "network_pipeline", "de_feature_pipeline", "de_pipeline"],
    )
