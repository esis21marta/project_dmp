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

from src.dmp.pipelines.dmp._01_aggregation.customer_los.create_weekly_customer_los import (
    create_weekly_customer_los_table,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    Create the LOS Aggregation Pipeline.
    Args:
        kwargs: Ignore any additional arguments added in the future.
    Returns:
        A Pipeline object containing all the LOS Aggregation Nodes.
    """
    return Pipeline(
        [
            node(
                func=create_weekly_customer_los_table,
                inputs=[
                    "l1_t_profile_full_hist",
                    "l1_prepaid_customers_data",
                    "l1_postpaid_customers_data",
                    "params:los_cb_prior_start_date",
                    "params:los_cb_prior_end_date",
                    "params:los_churn_period",
                ],
                outputs=None,  # l1_customer_los_weekly
                name="create_weekly_customer_los_table",
            ),
        ],
        tags=[
            "agg_customer_los",
            # "customer_los_pipeline",
            "de_aggregation_pipeline",
            # "de_pipeline",
            "agg_scoring_pipeline",
            "scoring_pipeline",
        ],
    )
