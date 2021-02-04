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
from src.dmp.pipelines.external.segmentation.demography.nodes import (
    clean_demography,
    get_demography_cross_tabs,
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
                clean_demography,
                [
                    "l1_lynx_demography_estimation_result",
                    "params:segmentation_year",
                    "params:segmentation_month",
                ],
                "demography_clean",
            ),
            node(
                join_tables,
                ["exploratory_segmentation_msisdns", "demography_clean", "kr_clean"],
                "demography_master_table",
            ),
            node(
                get_demography_cross_tabs,
                "demography_master_table",
                "exploratory_demography_cross_tabs",
            ),
        ]
    )