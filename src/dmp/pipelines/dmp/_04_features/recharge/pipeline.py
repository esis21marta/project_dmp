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

from src.dmp.pipelines.dmp._04_features.recharge.fea_recharge_covid import (
    fea_recharge_covid,
    fea_recharge_covid_fixed,
    fea_recharge_covid_fixed_all,
)

from .fea_recharge import fea_recharge
from .fea_recharge_mytsel import fea_recharge_mytsel


def create_pipeline(**kwargs) -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to create the feature layer for `Recharge` after the data
    has been run through the primary layer. This function will be executed as part of the `feature` layer. The pipeline takes a series of nodes; with tags to
    help uniquely identify it. More information on running can be found here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the `Recharge` nodes to execute the `feature` layer.
    """
    return Pipeline(
        [
            node(
                func=fea_recharge,
                inputs=[
                    "l1_rech_weekly_final",
                    "l1_digi_rech_weekly_final",
                    "l1_acc_bal_weekly_final",
                    "l1_chg_pkg_prchse_weekly_final",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_recharge",
                name="fea_recharge",
                tags=["fea_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=fea_recharge_mytsel,
                inputs=[
                    "l1_mytsel_rech_weekly_final",
                    "params:config_feature.recharge",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_recharge_mytsel",
                name="fea_recharge_mytsel",
            ),
            node(
                func=fea_recharge_covid,
                inputs=[
                    "l1_rech_weekly_final",
                    "l1_digi_rech_weekly_final",
                    "l1_acc_bal_weekly_final",
                    "l1_chg_pkg_prchse_weekly_final",
                    "l5_fea_recharge_pre_covid_fixed",
                    "l5_fea_recharge_covid_fixed_all",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_recharge_covid_final",
                name="fea_recharge_covid_final",
            ),
        ],
        tags=[
            "fea_recharge",
            "recharge_pipeline",
            "de_feature_pipeline",
            "de_pipeline",
        ],
    )


def create_covid_pipeline(**kwargs) -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to create the feature layer for Covid-19 after the data
    has been run through the primary layer. This function will be executed as part of the `feature` layer. The pipeline takes a series of nodes; with tags to
    help uniquely identify it. More information on running can be found here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the Covid-19 nodes to execute the `feature` layer.
    """

    return Pipeline(
        [
            node(
                func=fea_recharge_covid_fixed("pre_covid"),
                inputs=[
                    "l4_rech_weekly_final_pre_covid",
                    "l4_digi_rech_weekly_final_pre_covid",
                    "l4_acc_bal_weekly_final_pre_covid",
                    "l4_chg_pkg_prchse_weekly_final_pre_covid",
                    "params:config_feature",
                ],
                outputs="l5_fea_recharge_pre_covid_fixed",
                name="fea_recharge_pre_covid_fixed",
            ),
            node(
                func=fea_recharge_covid_fixed("covid"),
                inputs=[
                    "l4_rech_weekly_final_covid",
                    "l4_digi_rech_weekly_final_covid",
                    "l4_acc_bal_weekly_final_covid",
                    "l4_chg_pkg_prchse_weekly_final_covid",
                    "params:config_feature",
                ],
                outputs="l5_fea_recharge_covid_fixed",
                name="fea_recharge_covid_fixed",
            ),
            node(
                func=fea_recharge_covid_fixed("post_covid"),
                inputs=[
                    "l4_rech_weekly_final_post_covid",
                    "l4_digi_rech_weekly_final_post_covid",
                    "l4_acc_bal_weekly_final_post_covid",
                    "l4_chg_pkg_prchse_weekly_final_post_covid",
                    "params:config_feature",
                ],
                outputs="l5_fea_recharge_post_covid_fixed",
                name="fea_recharge_post_covid_fixed",
            ),
            node(
                func=fea_recharge_covid_fixed_all,
                inputs=[
                    "l5_fea_recharge_pre_covid_fixed",
                    "l5_fea_recharge_covid_fixed",
                    "l5_fea_recharge_post_covid_fixed",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_recharge_covid_fixed_all",
                name="fea_recharge_covid_fixed_all",
            ),
        ],
        tags=["fea_recharge_covid", "de_feature_covid_pipeline"],
    )
