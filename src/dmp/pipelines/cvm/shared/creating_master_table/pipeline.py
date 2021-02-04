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

from src.dmp.pipelines.cvm.shared.creating_master_table.master_table import (
    join_inputs_to_master,
    prepare_dmp_features_table,
    prepare_ds_features_table,
    prepare_target_var_table,
    removing_duplicats,
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
                func=prepare_ds_features_table,
                inputs=["sh_mck_int_mck_features_ds", "params:master_table"],
                outputs="rc_ds_features_to_master",
                name="rc_ds_features_to_master",
            ),
            node(
                func=prepare_target_var_table,
                inputs=["sh_l3_int_mck_payu_pkg_rev_grid_window_functions"],
                outputs="rc_target_to_master",
                name="rc_target_to_master",
            ),
            node(
                func=prepare_dmp_features_table,
                inputs=["sh_mck_int_features_dmp", "params:dmp_features"],
                outputs="rc_dmp_features_to_master",
                name="rc_dmp_features_to_master",
            ),
            node(
                func=join_inputs_to_master,
                inputs=[
                    "sh_trackingstream_wording_downsmpl",
                    "rc_target_to_master",
                    "rc_ds_features_to_master",
                    "rc_dmp_features_to_master",
                ],
                outputs="rc_master_intial",
                name="rc_master_intial",
            ),
            node(
                func=removing_duplicats,
                inputs=["rc_master_intial"],
                outputs="rc_master",
                name="rc_master",
            ),
        ],
    )
