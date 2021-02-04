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

from .create_proxy_target_variable import create_proxy_target_variable
from .fea_airtime_loan import fea_airtime_loan
from .fea_airtime_loan_offer_n_recharge import fea_airtime_loan_offer_n_recharge
from .fea_airtime_loan_offer_n_revenue import fea_airtime_loan_offer_n_revenue


def create_pipeline(**kwargs) -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to create the feature layer for `Airtime loan` after the data
    has been run through the primary layer. This function will be executed as part of the `feature` layer. The pipeline takes a series of nodes; with tags to
    help uniquely identify it. More information on running can be found here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the `Airtime loan` nodes to execute the `feature` layer.
    """
    return Pipeline(
        [
            node(
                func=create_proxy_target_variable,
                inputs=[
                    "l4_airtime_loan_weekly_final",
                    "l4_revenue_alt_weekly_data_final",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_airtime_loan_target_variable",
                name="airtime_loan_target_variable",
            ),
            node(
                func=fea_airtime_loan,
                inputs=[
                    "l4_airtime_loan_weekly_final",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_airtime_loan",
                name="fea_airtime_loan",
            ),
            node(
                func=fea_airtime_loan_offer_n_recharge,
                inputs=[
                    "l4_airtime_loan_weekly_final",
                    "l1_acc_bal_weekly_final",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_airtime_loan_offer_n_recharge",
                name="fea_airtime_loan_offer_n_recharge",
            ),
            node(
                func=fea_airtime_loan_offer_n_revenue,
                inputs=[
                    "l4_airtime_loan_weekly_final",
                    "l4_revenue_alt_weekly_data_final",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_airtime_loan_offer_n_revenue",
                name="fea_airtime_loan_offer_n_revenue",
            ),
        ],
        tags=[
            "fea_airtime_loan",
            "airtime_loan_pipeline",
            "de_feature_pipeline",
            "de_pipeline",
        ],
    )
