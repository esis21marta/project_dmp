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

from src.dmp.pipelines.dmp._01_aggregation.network.create_network_lacci_weekly import (
    create_network_lacci_weekly,
)
from src.dmp.pipelines.dmp._01_aggregation.network.create_network_weekly import (
    create_network_weekly,
)
from src.dmp.pipelines.dmp._01_aggregation.network.network_tutela_aggregation import (
    create_tutela_aggregation,
)


def create_pipeline(**kwargs) -> Pipeline:
    """ Create the Network Pipeline.

    :param kwargs: Ignore any additional arguments added in the future.

    :return: A Pipeline object containing all the Network Primary Nodes.
    """

    return Pipeline(
        [
            node(
                func=create_network_weekly,
                inputs=[
                    "l1_mck_sales_dnkm_dd",
                    "l1_prepaid_customers_data",
                    "l1_postpaid_customers_data",
                ],
                outputs=None,  # l1_network_msisdn_weekly_aggregated
                name="create_network_weekly",
                tags=[
                    "agg_network_msisdn",
                    # "network_msisdn_pipeline",
                    "agg_scoring_pipeline",
                    "scoring_pipeline",
                ],
            ),
            node(
                func=create_network_lacci_weekly,
                inputs="l1_mck_sales_dnkm_dd",
                outputs=None,  # l1_network_lacci_weekly_aggregated, l1_network_lac_weekly_aggregated
                name="create_network_lacci_weekly",
                tags=[
                    "agg_network_lacci",
                    # "network_lacci_pipeline",
                ],
            ),
            node(
                func=create_tutela_aggregation,
                inputs="l1_network_tutela_weekly",
                outputs=None,  # l2_network_tutela_location_weekly_aggregated
                name="create_tutela_aggregation",
                tags=["agg_network_tutela",],
            ),
        ],
        tags=[
            "agg_network",
            # "network_pipeline",
            "de_aggregation_pipeline",
            # "de_pipeline",
        ],
    )
