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

from src.dmp.pipelines.common.packing.nodes import pack


def create_pipeline():
    """
    Create the packing input data pipeline.
    Returns:
        A Pipeline object containing packing input Nodes.
    """
    return Pipeline(
        [
            node(
                pack,
                inputs=[
                    # External
                    "kredivo_default_probability_input",
                    "params:kredivo_default_probability",
                    "campaign_eval_multisim_stratified_input",
                    "params:campaign_eval_multisim_stratified",
                    "campaign_eval_tnl_grabngo_stratified_input",
                    "params:campaign_eval_tnl_grabngo_stratified",
                    "campaign_eval_tnl_pathtochurn_stratified_input",
                    "params:campaign_eval_tnl_pathtochurn_stratified",
                    "kredivo_default_probability_validation_input",
                    "params:kredivo_default_probability_validation",
                    "kredivo_income_input",
                    "params:kredivo_income",
                    "kredivo_precampaign_input",
                    "params:kredivo_precampaign",
                    "kredivo_poc_input",
                    "params:kredivo_poc",
                    "home_credit_default_probability_input",
                    "params:home_credit_default_probability",
                    "home_credit_default_probability_validation_input",
                    "params:home_credit_default_probability_validation",
                    # Internal
                    # "internal_random_sample_input",
                    # "params:internal_random_sample",
                    "internal_hvc_input",
                    "params:internal_hvc",
                    "internal_hvc_may_input",
                    "params:internal_hvc_may",
                    "internal_ucg_201910_input",
                    "params:internal_ucg_201910",
                    # "internal_4g_campaigns_input",
                    # "params:internal_4g_campaigns",
                    # Inlife and 4G
                    "fg_scale_up_samples_input",
                    "params:fg_scale_up_samples",
                    "gamers_samples_input",
                    "params:gamers_samples",
                    "drgb_input",
                    "params:drgb",
                    "rgb_a_samples_input",
                    "params:rgb_a_samples",
                    "hvc_lapser_a_input",
                    "params:hvc_lapser_a",
                    "hvc_lapser_b_input",
                    "params:hvc_lapser_b",
                    "multisim_4g_input",
                    "params:multisim_4g",
                    "multisim_non_4g_samples_input",
                    "params:multisim_non_4g_samples",
                    "hvc_prop_input",
                    "params:hvc_prop",
                    "4g_lapser_input",
                    "params:4g_lapser",
                    "acq_input",
                    "params:acq",
                    # Interface with DE
                    "params:de_msisdn_column",
                ],
                outputs="de_input",
                name="de_input_node",
            ),
        ],
        tags=["de_pipeline", "de_pimary_pipeline", "de_packing_pipeline"],
    )
