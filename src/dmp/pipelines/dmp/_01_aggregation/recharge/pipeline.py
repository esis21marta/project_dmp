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

from src.dmp.pipelines.dmp._01_aggregation.recharge.create_account_balance_weekly import (
    create_account_balance_weekly,
)
from src.dmp.pipelines.dmp._01_aggregation.recharge.create_digital_recharge_weekly import (
    create_digital_recharge_weekly,
)
from src.dmp.pipelines.dmp._01_aggregation.recharge.create_mytsel_recharge_weekly import (
    create_mytsel_recharge_weekly,
)
from src.dmp.pipelines.dmp._01_aggregation.recharge.create_package_purchase_weekly import (
    create_package_purchase_weekly,
)
from src.dmp.pipelines.dmp._01_aggregation.recharge.create_recharge_weekly import (
    create_recharge_weekly,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to aggregate `recharge` data into a [MSISDN, Weekstart]
    granularity.  This function will be executed as part of the `aggregation` layer. The pipeline takes a series of nodes; with tags to
    help uniquely identify it. More information on running can be found here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the `recharge` nodes to execute the `aggregation` layer.
    """
    return Pipeline(
        [
            node(
                func=create_recharge_weekly,
                inputs="l1_abt_rech_daily_abt_dd",
                outputs=None,  # l1_rech_weekly
                name="recharge_union_tables",
                tags=["agg_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=create_digital_recharge_weekly,
                inputs="l1_smy_rech_urp_dd",
                outputs=None,  # l1_digi_rech_weekly
                name="digital_recharge_weekly",
                tags=["agg_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=create_account_balance_weekly,
                inputs=[
                    "l1_account_balance",
                    "l1_prepaid_customers_data",
                    "l1_abt_rech_daily_abt_dd",
                ],
                outputs=None,  # l2_account_balance_weekly
                name="recharge_weekly_balance",
                tags=["agg_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=create_package_purchase_weekly,
                inputs="l1_itdaml_dd_cdr_pkg_prchse_all",
                outputs=None,  # l1_chg_pkg_prchse_weekly
                name="recharge_weekly_chg_pkg",
                tags=["agg_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=create_mytsel_recharge_weekly,
                inputs="l1_mytsel_rech_dd",
                outputs=None,  # l1_mytsel_rech_weekly
                name="recharge_weekly_mytsel",
            ),
        ],
        tags=[
            "agg_recharge",
            # "recharge_pipeline",
            "de_aggregation_pipeline",
            # "de_pipeline",
        ],
    )
