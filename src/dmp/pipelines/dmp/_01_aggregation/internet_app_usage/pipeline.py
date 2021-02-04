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

from src.dmp.pipelines.dmp._01_aggregation.internet_app_usage.create_internet_app_usage_weekly import (
    create_internet_app_usage_weekly,
)
from src.dmp.pipelines.dmp._01_aggregation.internet_app_usage.create_internet_competitor_monthly import (
    create_internet_app_competitor_monthly,
)
from src.dmp.pipelines.dmp._01_aggregation.internet_app_usage.create_internet_connection_weekly import (
    create_internet_connection_weekly,
)
from src.dmp.pipelines.dmp._01_aggregation.internet_app_usage.feature_mapping import (
    feature_mapping,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to aggregate `internet app usage` data into a [MSISDN, Weekstart]
    granularity.  This function will be executed as part of the `aggregation` layer. The pipeline takes a series of nodes; with tags to
    help uniquely identify it. More information on running can be found here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the `internet app usage` nodes to execute the `aggregation` layer.
    """

    return Pipeline(
        nodes=[
            node(
                func=feature_mapping,
                inputs=[
                    "l1_content_mapping",
                    "l1_segment_mapping",
                    "l1_category_mapping",
                    "l1_fintech_mapping",
                    "l1_app_mapping",
                ],
                outputs="l1_internet_app_feature_mapping",
                name="bcp_feature_mapping",
                tags=["agg_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=create_internet_app_usage_weekly,
                inputs=["l1_bcp_usage_daily", "l1_internet_app_feature_mapping"],
                outputs=None,  # l2_internet_app_usage_weekly
                name="weekly_aggregation_internet_app_usage",
                tags=["agg_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=create_internet_connection_weekly,
                inputs="l1_bcp_usage_daily",
                outputs=None,  # l2_internet_connection_weekly
                name="internet_app_usage_weekly_aggregation",
                tags=["agg_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=create_internet_app_competitor_monthly,
                inputs=["l1_internet_app_competitor_monthly", "params:month_dt_dict"],
                outputs=None,  # l2_internet_app_usage_competitor_monthly
                name="internet_app_usage_competitor_monthly_aggregation",
            ),
        ],
        tags=[
            "agg_internet_app_usage",
            # "internet_app_usage_pipeline",
            "de_aggregation_pipeline",
            # "de_pipeline",
        ],
    )
