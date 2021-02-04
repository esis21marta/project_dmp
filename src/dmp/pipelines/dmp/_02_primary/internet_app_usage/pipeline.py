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

from src.dmp.pipelines.dmp._02_primary.filter_partner_data.filter_partner_data import (
    filter_partner_data,
    filter_partner_data_fixed_weekstart,
)
from src.dmp.pipelines.dmp._02_primary.internet_app_usage.fill_time_series import (
    fill_bcp_time_series,
)
from src.dmp.pipelines.dmp._02_primary.scaffold.fill_time_series import fill_time_series
from src.dmp.pipelines.dmp._02_primary.scaffold.fill_time_series_monthly import (
    fill_time_series_monthly,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to create the primary layer for `Internet App Usage` after the data
    has been aggregated to weekly-level. This function will be executed as part of the `primary` layer. The pipeline takes a series of nodes; with tags to
    help uniquely identify it. More information on running can be found here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the `Internet App Usage` nodes to execute the `primary` layer.
    """

    return Pipeline(
        [
            node(
                func=filter_partner_data,
                inputs=[
                    "partner_msisdns",
                    "l2_internet_app_usage_weekly",
                    "params:filter_partner_data",
                ],
                outputs="l3_internet_app_usage_weekly_filtered",
                name="filter_partner_data_internet_app_usage",
                tags=["prm_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=fill_bcp_time_series,
                inputs=[
                    "l1_weekly_scaffold",
                    "l1_internet_app_feature_mapping",
                    "l3_internet_app_usage_weekly_filtered",
                ],
                outputs="l4_internet_app_usage_weekly_final",
                name="fill_time_series_internet_app_usage",
                tags=["prm_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=filter_partner_data,
                inputs=[
                    "partner_msisdns",
                    "l2_internet_connection_weekly",
                    "params:filter_partner_data",
                ],
                outputs="l3_internet_connection_weekly_filtered",
                name="filter_partner_data_internet_connection",
                tags=["prm_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=fill_time_series(),
                inputs=["l1_weekly_scaffold", "l3_internet_connection_weekly_filtered"],
                outputs="l4_internet_connection_weekly_final",
                name="fill_time_series_internet_connection",
                tags=["prm_scoring_pipeline", "scoring_pipeline"],
            ),
            node(
                func=filter_partner_data,
                inputs=[
                    "partner_msisdns",
                    "l2_internet_app_usage_competitor_monthly",
                    "params:filter_partner_data",
                ],
                outputs="l3_internet_app_usage_competitor_monthly_filtered",
                name="filter_partner_data_internet_app_usage_competitor",
            ),
            node(
                func=fill_time_series_monthly(),
                inputs=[
                    "l1_monthly_scaffold",
                    "l3_internet_app_usage_competitor_monthly_filtered",
                ],
                outputs="l4_internet_app_usage_competitor_monthly_final",
                name="fill_time_series_internet_app_usage_competitor",
            ),
        ],
        tags=[
            "prm_internet_app_usage",
            "internet_app_usage_pipeline",
            "de_primary_pipeline",
            "de_pipeline",
        ],
    )


def create_covid_pipeline(**kwargs) -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to create the primary layer for Covid-19 related features after the data
    has been aggregated to weekly-level. This function will be executed as part of the `primary` layer. The pipeline takes a series of nodes; with tags to
    help uniquely identify it. More information on running can be found here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the Covid-19 nodes to execute the `primary` layer.
    """

    return Pipeline(
        [
            node(
                func=filter_partner_data_fixed_weekstart,
                inputs=[
                    "partner_msisdns",
                    "l2_internet_app_usage_weekly_all",
                    "params:filter_partner_data",
                    "params:pre_covid_first_weekstart",
                    "params:pre_covid_last_weekstart",
                ],
                outputs="l3_internet_app_usage_weekly_filtered_pre_covid",
                name="internet_app_usage_weekly_filtered_pre_covid",
                tags=[
                    "prm_internet_app_usage_covid",
                    "internet_app_usage_covid_pipeline",
                ],
            ),
            node(
                func=fill_bcp_time_series,
                inputs=[
                    "l1_weekly_scaffold_pre_covid",
                    "l1_internet_app_feature_mapping",
                    "l3_internet_app_usage_weekly_filtered_pre_covid",
                ],
                outputs="l4_internet_app_usage_weekly_pre_covid_final",
                name="fill_time_series_internet_app_usage_pre_covid",
                tags=[
                    "prm_internet_app_usage_covid",
                    "internet_app_usage_covid_pipeline",
                ],
            ),
            node(
                func=filter_partner_data_fixed_weekstart,
                inputs=[
                    "partner_msisdns",
                    "l2_internet_app_usage_weekly_all",
                    "params:filter_partner_data",
                    "params:post_covid_first_weekstart",
                    "params:post_covid_last_weekstart",
                ],
                outputs="l3_internet_app_usage_weekly_filtered_post_covid",
                name="internet_app_usage_weekly_filtered_post_covid",
                tags=[
                    "prm_internet_app_usage_covid",
                    "internet_app_usage_covid_pipeline",
                ],
            ),
            node(
                func=fill_bcp_time_series,
                inputs=[
                    "l1_weekly_scaffold_post_covid",
                    "l1_internet_app_feature_mapping",
                    "l3_internet_app_usage_weekly_filtered_post_covid",
                ],
                outputs="l4_internet_app_usage_weekly_post_covid_final",
                name="fill_time_series_internet_app_usage_post_covid",
                tags=[
                    "prm_internet_app_usage_covid",
                    "internet_app_usage_covid_pipeline",
                ],
            ),
            node(
                func=filter_partner_data_fixed_weekstart,
                inputs=[
                    "partner_msisdns",
                    "l2_internet_app_usage_weekly_all",
                    "params:filter_partner_data",
                    "params:covid_first_weekstart",
                    "params:covid_last_weekstart",
                ],
                outputs="l3_internet_app_usage_weekly_filtered_covid",
                name="internet_app_usage_weekly_filtered_covid",
                tags=[
                    "prm_internet_app_usage_covid",
                    "internet_app_usage_covid_pipeline",
                ],
            ),
            node(
                func=fill_bcp_time_series,
                inputs=[
                    "l1_weekly_scaffold_covid",
                    "l1_internet_app_feature_mapping",
                    "l3_internet_app_usage_weekly_filtered_covid",
                ],
                outputs="l4_internet_app_usage_weekly_covid_final",
                name="fill_time_series_internet_app_usage_covid",
                tags=[
                    "prm_internet_app_usage_covid",
                    "internet_app_usage_covid_pipeline",
                ],
            ),
        ],
        tags=[
            "prm_covid",
            "prm_internet_app_usage_covid",
            "internet_app_usage_covid_pipeline",
        ],
    )
