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

from src.dmp.pipelines.dmp._04_features.handset_internal.fea_handset_internal_all import (
    calculate_count_distincts,
    fea_device_modes,
    retrieve_mode,
    retrieve_month_windows,
    retrieve_week_windows,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to create the feature layer for `Handset for internal` after the data
    has been run through the primary layer. This function will be executed as part of the `feature` layer. The pipeline takes a series of nodes; with tags to
    help uniquely identify it. More information on running can be found here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the `Handset for internal` nodes to execute the `feature` layer.
    """
    return Pipeline(
        [
            node(
                func=retrieve_week_windows,
                inputs=[
                    "l2_internal_handset_time_series_filled",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l2_device_internal_rolling_windows_weekly",
                name="prep_device_internal_windows_week",
            ),
            node(
                func=retrieve_month_windows,
                inputs=[
                    "l2_internal_handset_time_series_filled",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l2_device_internal_rolling_windows_monthly",
                name="prep_device_internal_windows_month",
            ),
            node(
                func=retrieve_mode,
                inputs=[
                    "l2_device_internal_rolling_windows_weekly",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l2_device_internal_mode_weekly",
                name="prep_device_internal_mode_weekly",
            ),
            node(
                func=retrieve_mode,
                inputs=[
                    "l2_device_internal_rolling_windows_monthly",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l2_device_internal_mode_monthly",
                name="prep_device_internal_mode_monthly",
            ),
            node(
                func=calculate_count_distincts,
                inputs=[
                    "l2_internal_handset_time_series_filled",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs=dict(
                    tac_count_df="l2_device_internal_tac_counts",
                    man_count_df="l2_device_internal_man_counts",
                ),
                name="prep_device_tac_man_counts",
            ),
            node(
                func=fea_device_modes,
                inputs=[
                    "l2_internal_handset_time_series_filled",
                    "l2_device_internal_mode_weekly",
                    "l2_device_internal_mode_monthly",
                    "l2_device_internal_tac_counts",
                    "l2_device_internal_man_counts",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l2_fea_device_internal",
                name="fea_handset_internal",
            ),
        ],
        tags=[
            "fea_handset_internal",
            "handset_internal_pipeline",
            # "de_feature_pipeline",
            # "de_pipeline",
        ],
    )
