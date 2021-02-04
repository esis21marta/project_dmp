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

from src.dmp.pipelines.external.lookalike_sample.nodes import (
    generate_population,
    generate_sample,
    pick_sample,
)


def create_pipeline():
    return Pipeline(
        [
            node(
                generate_population,
                [
                    "home_location",
                    "l1_pre_paid_mck_customer_base_dd",
                    "l1_post_paid_mck_customer_base_dd",
                    "kr_default_probability",
                    "params:latest_home_location_month",
                    "params:date_user_active_on",
                    "params:home_days_threshold",
                    "params:territory_filter",
                ],
                "population",
            ),
            node(generate_sample, ["population", "params:num_sample"], "samples"),
            node(
                pick_sample,
                ["kr_default_probability", "samples"],
                "kr_lookalike_random_sample",
            ),
        ],
        tags=["generate_random_lookalike"],
    )
