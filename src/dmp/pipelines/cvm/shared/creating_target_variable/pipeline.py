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

# from src.cvm.pipelines.shared.creating_target_variable.target_variable import create_total_revenue_table, \
#     create_dls_payu_revenue_table, creating_grid, filling_grid_with_revenue_values, creating_window_functions
from src.dmp.pipelines.cvm.shared.creating_target_variable.target_variable import (
    create_dls_payu_revenue_table,
    create_total_revenue_table,
    creating_grid,
    creating_window_functions,
    filling_grid_with_revenue_values,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    Create a pipeline for the target variable creation

    :param kwargs: Ignore any additional arguments added in the future.

    :return: A Pipeline object
    """
    return Pipeline(
        [
            node(
                func=create_total_revenue_table,
                inputs=["l1_abt_payu", "l1_abt_pkg"],
                outputs="sh_l2_mck_int_payu_pkg_rev",
                name="total_revenue_table",
            ),
            node(
                func=create_dls_payu_revenue_table,
                inputs="l1_abt_payu",
                outputs="sh_l2_mck_int_dls_rev",
                name="dls_payu_revenue_table",
            ),
            node(
                func=creating_grid,
                inputs=["sh_l2_mck_int_payu_pkg_rev"],
                outputs="sh_l2_int_mck_grid",
            ),
            node(
                func=filling_grid_with_revenue_values,
                inputs=[
                    "sh_l2_mck_int_payu_pkg_rev",
                    "sh_l2_mck_int_dls_rev",
                    "sh_l2_int_mck_grid",
                ],
                outputs="sh_l2_int_mck_payu_pkg_rev_grid",
            ),
            node(
                func=creating_window_functions,
                inputs=["sh_l2_int_mck_payu_pkg_rev_grid"],
                outputs="sh_l3_int_mck_payu_pkg_rev_grid_window_functions",
            ),
        ],
    )
