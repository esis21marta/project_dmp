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
)
from src.dmp.pipelines.dmp._02_primary.scaffold.fill_time_series_monthly import (
    fill_time_series_monthly,
)


def create_pipeline():
    pipeline = Pipeline(
        [
            node(
                func=filter_partner_data,
                inputs=[
                    "partner_msisdns",
                    "l2_home_stay",
                    "params:filter_partner_data",
                ],
                outputs="l3_home_stay_filtered",
                name="filter_home_stay_data",
            ),
            node(
                func=fill_time_series_monthly(),
                inputs=["l1_monthly_scaffold", "l3_home_stay_filtered"],
                outputs="l4_home_stay_filtered",
                name="fill_time_series_home_stay_data",
            ),
            node(
                func=filter_partner_data,
                inputs=[
                    "partner_msisdns",
                    "l2_work_stay",
                    "params:filter_partner_data",
                ],
                outputs="l3_work_stay_filtered",
                name="filter_work_stay_data",
            ),
            node(
                func=fill_time_series_monthly(),
                inputs=["l1_monthly_scaffold", "l3_work_stay_filtered"],
                outputs="l4_work_stay_filtered",
                name="fill_time_series_work_stay_data",
            ),
            node(
                func=filter_partner_data,
                inputs=[
                    "partner_msisdns",
                    "l2_staypoint",
                    "params:filter_partner_data",
                ],
                outputs="l3_staypoint_filtered",
                name="filter_staypoint_data",
            ),
            node(
                func=fill_time_series_monthly(),
                inputs=["l1_monthly_scaffold", "l3_staypoint_filtered"],
                outputs="l4_staypoint_filtered",
                name="fill_time_series_staypoint_data",
            ),
        ],
        tags=[
            "prm_mobility",
            "de_mobility_pipeline",
            "de_primary_pipeline",
            "de_pipeline",
        ],
    )

    return pipeline
