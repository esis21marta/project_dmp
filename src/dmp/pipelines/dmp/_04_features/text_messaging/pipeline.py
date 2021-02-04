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

from src.dmp.pipelines.dmp._04_features.text_messaging.fea_commercial_text import (
    fea_comm_text_messaging,
)
from src.dmp.pipelines.dmp._04_features.text_messaging.fea_commercial_text_covid import (
    fea_comm_text_messaging_covid,
    fea_comm_text_messaging_covid_final,
)
from src.dmp.pipelines.dmp._04_features.text_messaging.fea_txt_msg_count import (
    fea_txt_msg_count,
)
from src.dmp.pipelines.dmp._04_features.text_messaging.fea_txt_msg_count_covid import (
    fea_txt_msg_count_covid,
    fea_txt_msg_count_covid_final,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to create the feature layer for `text messaging` after the data
    has been run through the primary layer. This function will be executed as part of the `feature` layer. The pipeline takes a series of nodes; with tags to
    help uniquely identify it. More information on running can be found here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the `text messaging` nodes to execute the `feature` layer.
    """

    return Pipeline(
        [
            node(
                func=fea_txt_msg_count,
                inputs=[
                    "l2_text_messaging_scaffold_weekly",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l4_fea_text_messaging",
                name="fea_text_messaging",
                tags=["fea_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=fea_comm_text_messaging,
                inputs=[
                    "l2_commercial_text_messaging_scaffold_weekly",
                    "l2_kredivo_sms_recipients",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l4_fea_commercial_text_messaging",
                name="fea_commercial_text_messaging",
                tags=["fea_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=fea_txt_msg_count_covid_final,
                inputs=[
                    "l2_text_messaging_scaffold_weekly",
                    "l5_fea_text_messaging_covid",
                    "params:pre_covid_last_weekstart",
                    "params:covid_last_weekstart",
                    "params:post_covid_last_weekstart",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_text_messaging_covid_final",
                name="fea_text_messaging_covid_final",
            ),
            node(
                func=fea_comm_text_messaging_covid_final,
                inputs=[
                    "l2_commercial_text_messaging_scaffold_weekly",
                    "l5_fea_commercial_text_messaging_covid",
                    "params:pre_covid_last_weekstart",
                    "params:covid_last_weekstart",
                    "params:post_covid_last_weekstart",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_commercial_text_messaging_covid_final",
                name="fea_commercial_text_messaging_covid_final",
            ),
        ],
        tags=[
            "fea_text_messaging",
            "text_messaging_pipeline",
            "de_feature_pipeline",
            "de_pipeline",
        ],
    )


def create_covid_pipeline(**kwargs) -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to create the feature layer for `text messaging` after the data
    has been run through the primary layer. This function will be executed as part of the `feature` layer. The pipeline takes a series of nodes; with tags to
    help uniquely identify it. More information on running can be found here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the `text messaging` nodes to execute the `feature` layer.
    """

    return Pipeline(
        [
            node(
                func=fea_txt_msg_count_covid,
                inputs=[
                    "l4_text_messaging_scaffold_weekly_pre_covid",
                    "l4_text_messaging_scaffold_weekly_covid",
                    "l4_text_messaging_scaffold_weekly_post_covid",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_text_messaging_covid",
                name="fea_text_messaging_covid",
            ),
            node(
                func=fea_comm_text_messaging_covid,
                inputs=[
                    "l4_commercial_text_messaging_scaffold_weekly_pre_covid",
                    "l4_commercial_text_messaging_scaffold_weekly_covid",
                    "l4_commercial_text_messaging_scaffold_weekly_post_covid",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_commercial_text_messaging_covid",
                name="fea_commercial_text_messaging_covid",
            ),
        ],
        tags=["fea_text_messaging_covid", "de_feature_covid_pipeline"],
    )
