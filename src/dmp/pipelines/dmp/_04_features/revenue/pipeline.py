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

from .fea_revenue_base_payu_pkg import fea_revenue_base_payu_pkg
from .fea_revenue_monthly_spend import fea_revenue_monthly_spend
from .fea_revenue_mytsel import fea_revenue_mytsel
from .fea_revenue_salary import fea_revenue_salary
from .fea_revenue_spend_pattern import fea_revenue_spend_pattern
from .fea_revenue_threshold_sms_dls_roam import fea_revenue_threshold_sms_dls_roam
from .fea_revenue_threshold_voice_data import fea_revenue_threshold_voice_data
from .fea_revenue_weekend import fea_revenue_weekend


def create_pipeline(**kwargs) -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to create the feature layer for `revenue` after the data
    has been run through the primary layer. This function will be executed as part of the `feature` layer. The pipeline takes a series of nodes; with tags to
    help uniquely identify it. More information on running can be found here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the `revenue` nodes to execute the `feature` layer.
    """
    return Pipeline(
        [
            node(
                func=fea_revenue_weekend,
                inputs=[
                    "l4_revenue_weekly_data_final",
                    "l4_revenue_alt_weekly_data_final",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l4_fea_revenue_weekend",
                name="fea_revenue_weekend",
                tags=["fea_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=fea_revenue_base_payu_pkg,
                inputs=[
                    "l4_revenue_weekly_data_final",
                    "l4_revenue_alt_weekly_data_final",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l4_fea_revenue_base_payu_pkg",
                name="fea_revenue_base_payu_pkg",
                tags=["fea_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=fea_revenue_spend_pattern,
                inputs=[
                    "l4_revenue_weekly_data_final",
                    "l4_revenue_alt_weekly_data_final",
                    "l1_rech_weekly_final",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l4_fea_revenue_spend_pattern",
                name="fea_revenue_spend_pattern",
                tags=["fea_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=fea_revenue_monthly_spend,
                inputs=[
                    "l4_revenue_weekly_data_final",
                    "l4_revenue_alt_weekly_data_final",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l4_fea_revenue_monthly_spend",
                name="fea_revenue_monthly_spend",
                tags=["fea_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=fea_revenue_salary,
                inputs=[
                    "l4_revenue_weekly_data_final",
                    "l4_revenue_alt_weekly_data_final",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l4_fea_revenue_salary",
                name="fea_revenue_salary",
                tags=["fea_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=fea_revenue_threshold_voice_data,
                inputs=[
                    "l4_revenue_weekly_data_final",
                    "l4_revenue_alt_weekly_data_final",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l4_fea_revenue_threshold_voice_data",
                name="fea_revenue_threshold_voice_data",
                tags=["fea_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=fea_revenue_threshold_sms_dls_roam,
                inputs=[
                    "l4_revenue_weekly_data_final",
                    "l4_revenue_alt_weekly_data_final",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l4_fea_revenue_threshold_sms_dls_roam",
                name="fea_revenue_threshold_sms_dls_roam",
                tags=["fea_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=fea_revenue_mytsel,
                inputs=[
                    "l3_revenue_mytsel_weekly_final",
                    "params:config_feature.revenue",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l4_fea_revenue_mytsel",
                name="fea_revenue_mytsel",
            ),
        ],
        tags=["fea_revenue", "revenue_pipeline", "de_feature_pipeline", "de_pipeline",],
    )
