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

from src.dmp.pipelines.external.segmentation.common.nodes import clean_kr, join_tables
from src.dmp.pipelines.external.segmentation.home_location.nodes import (
    clean_home_location,
)
from src.dmp.pipelines.external.segmentation.revenue.nodes import (
    bin_revenues,
    clean_payu,
    clean_pkg,
    sum_revenues,
)
from src.dmp.pipelines.external.segmentation.revenue_and_home_location.nodes import (
    get_revenue_and_home_location_cross_tabs,
    join_revenue_to_home_location,
)


def create_pipeline():
    return Pipeline(
        [
            node(
                clean_kr,
                [
                    "l1_mck_kredivo_badflags",
                    "params:segmentation_year",
                    "params:segmentation_month",
                ],
                "kr_clean",
            ),
            node(
                clean_payu,
                [
                    "l1_abt_payu",
                    "params:segmentation_year",
                    "params:segmentation_month",
                ],
                "payu_clean",
            ),
            node(
                clean_pkg,
                ["l1_abt_pkg", "params:segmentation_year", "params:segmentation_month"],
                "pkg_clean",
            ),
            node(sum_revenues, ["payu_clean", "pkg_clean"], "revenue_sums",),
            node(bin_revenues, "revenue_sums", "revenue_bins",),
            node(
                clean_home_location,
                [
                    "l1_mig_home_locations",
                    "params:segmentation_year",
                    "params:segmentation_month",
                ],
                "home_location_clean",
            ),
            node(
                join_revenue_to_home_location,
                ["home_location_clean", "revenue_bins"],
                "revenue_and_home_location",
            ),
            node(
                join_tables,
                [
                    "exploratory_segmentation_msisdns",
                    "revenue_and_home_location",
                    "kr_clean",
                ],
                "revenue_and_home_location_master_table",
            ),
            node(
                get_revenue_and_home_location_cross_tabs,
                "revenue_and_home_location_master_table",
                "exploratory_revenue_and_home_location_cross_tabs",
            ),
        ]
    )
