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

from src.dmp.pipelines.dmp._04_features.internet_app_usage.fea_internet_app_usage import (
    fea_internet_app_usage,
    pivot_features,
)
from src.dmp.pipelines.dmp._04_features.internet_app_usage.fea_internet_app_usage_competitor import (
    fea_internet_app_usage_competitor,
)
from src.dmp.pipelines.dmp._04_features.internet_app_usage.fea_internet_app_usage_covid import (
    fea_int_app_usage_covid,
    fea_int_app_usage_covid_fixed_all,
    fea_int_app_usage_covid_trend,
    fea_internet_app_usage_covid,
    pivot_features_covid,
    rename_and_prep_covid_feature_df,
)
from src.dmp.pipelines.dmp._04_features.internet_app_usage.fea_internet_connection import (
    fea_internet_connection,
)
from src.dmp.pipelines.dmp._04_features.internet_app_usage.fea_social_media import (
    fea_social_media,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to create the feature layer for `Internet App Usage` after the data
    has been run through the primary layer. This function will be executed as part of the `feature` layer. The pipeline takes a series of nodes; with tags to
    help uniquely identify it. More information on running can be found here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the `Internet App Usage` nodes to execute the `feature` layer.
    """

    return Pipeline(
        [
            node(
                func=fea_internet_connection,
                inputs=[
                    "l4_internet_connection_weekly_final",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_internet_connection",
                name="fea_internet_connection",
                tags=["fea_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=fea_internet_app_usage,
                inputs=[
                    "l4_internet_app_usage_weekly_final",
                    "params:features_mode",
                    "params:output_features",
                    "params:shuffle_partitions_10k",
                ],
                outputs="l5_internet_app_usage",
                name="fea_internet_app_usage",
                tags=["fea_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=pivot_features,
                inputs=[
                    "l5_internet_app_usage",
                    "params:features_mode",
                    "params:output_features",
                    "params:shuffle_partitions_10k",
                ],
                outputs="l5_fea_internet_app_usage",
                name="fea_internet_app_usage_pivot",
                tags=["fea_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=fea_social_media,
                inputs=[
                    "l5_fea_internet_app_usage",
                    "l5_fea_internet_connection",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_social_media",
                name="fea_social_media",
                tags=["fea_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=fea_internet_app_usage_competitor,
                inputs=[
                    "l4_internet_app_usage_competitor_monthly_final",
                    "params:sla_date",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_internet_app_usage_competitor",
                name="fea_internet_app_usage_competitor",
            ),
            node(
                func=fea_int_app_usage_covid,
                inputs=[
                    "l5_fea_internet_app_usage",
                    "l5_fea_internet_app_usage_pre_covid",
                    "l5_fea_internet_app_usage_covid_fixed",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_internet_app_usage_covid_final",
                name="fea_internet_app_usage_covid_final",
            ),
            node(
                func=fea_int_app_usage_covid_trend,
                inputs=[
                    "l5_fea_internet_app_usage",
                    "l5_fea_internet_app_usage_pre_covid",
                    "params:config_feature",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_internet_app_usage_covid_trend",
                name="fea_internet_app_usage_covid_trend",
            ),
        ],
        tags=[
            "fea_internet_app_usage",
            "internet_app_usage_pipeline",
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
                func=fea_internet_app_usage_covid(
                    "pre_covid_last_weekstart", "pre_covid_last_weekstart"
                ),
                inputs=[
                    "l4_internet_app_usage_weekly_pre_covid_final",
                    "params:features_mode",
                    "params:output_features",
                    "params:shuffle_partitions_10k",
                ],
                outputs="l5_internet_app_usage_pre_covid",
                name="fea_internet_app_usage_pre_covid",
            ),
            node(
                func=pivot_features_covid,
                inputs=[
                    "l5_internet_app_usage_pre_covid",
                    "params:features_mode",
                    "params:output_features",
                    "params:shuffle_partitions_10k",
                ],
                outputs="l5_pivoted_internet_app_usage_pre_covid",
                name="fea_internet_app_usage_pivot_pre_covid",
            ),
            node(
                func=rename_and_prep_covid_feature_df("pre_covid"),
                inputs=[
                    "l5_pivoted_internet_app_usage_pre_covid",
                    "params:config_feature",
                    "params:pre_covid_last_weekstart",
                ],
                outputs="l5_fea_internet_app_usage_pre_covid",
                name="fea_internet_app_usage_rename_pre_covid",
            ),
            node(
                func=fea_internet_app_usage_covid(
                    "covid_last_weekstart", "covid_last_weekstart"
                ),
                inputs=[
                    "l4_internet_app_usage_weekly_covid_final",
                    "params:features_mode",
                    "params:output_features",
                    "params:shuffle_partitions_10k",
                ],
                outputs="l5_internet_app_usage_covid",
                name="fea_internet_app_usage_covid",
            ),
            node(
                func=pivot_features_covid,
                inputs=[
                    "l5_internet_app_usage_covid",
                    "params:features_mode",
                    "params:output_features",
                    "params:shuffle_partitions_10k",
                ],
                outputs="l5_pivoted_internet_app_usage_covid",
                name="fea_internet_app_usage_pivot_covid",
            ),
            node(
                func=rename_and_prep_covid_feature_df("covid"),
                inputs=[
                    "l5_pivoted_internet_app_usage_covid",
                    "params:config_feature",
                    "params:covid_last_weekstart",
                ],
                outputs="l5_fea_internet_app_usage_covid",
                name="fea_internet_app_usage_rename_covid",
            ),
            node(
                func=fea_internet_app_usage_covid(
                    "post_covid_last_weekstart", "post_covid_last_weekstart"
                ),
                inputs=[
                    "l4_internet_app_usage_weekly_post_covid_final",
                    "params:features_mode",
                    "params:output_features",
                    "params:shuffle_partitions_10k",
                ],
                outputs="l5_internet_app_usage_post_covid",
                name="fea_internet_app_usage_post_covid",
            ),
            node(
                func=pivot_features_covid,
                inputs=[
                    "l5_internet_app_usage_post_covid",
                    "params:features_mode",
                    "params:output_features",
                    "params:shuffle_partitions_10k",
                ],
                outputs="l5_pivoted_internet_app_usage_post_covid",
                name="fea_internet_app_usage_pivot_post_covid",
            ),
            node(
                func=rename_and_prep_covid_feature_df("post_covid"),
                inputs=[
                    "l5_pivoted_internet_app_usage_post_covid",
                    "params:config_feature",
                    "params:post_covid_last_weekstart",
                ],
                outputs="l5_fea_internet_app_usage_post_covid",
                name="fea_internet_app_usage_rename_post_covid",
            ),
            node(
                func=fea_int_app_usage_covid_fixed_all,
                inputs=[
                    "l5_fea_internet_app_usage_pre_covid",
                    "l5_fea_internet_app_usage_covid",
                    "l5_fea_internet_app_usage_post_covid",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l5_fea_internet_app_usage_covid_fixed",
                name="fea_internet_app_usage_covid_fixed",
            ),
        ],
        tags=["fea_internet_app_usage_covid", "internet_app_usage_covid_pipeline",],
    )
