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

from src.dmp.pipelines.common.unpacking.nodes import unpack


def create_pipeline():
    """
    Create the unpacking input data Pipeline.
    Returns:
        A Pipeline object containing unpacking input Nodes.
    """
    return Pipeline(
        [
            # External
            node(
                unpack,
                inputs=[
                    "de_output",
                    "kredivo_default_probability_input",
                    "params:kredivo_default_probability",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="kredivo_default_probability_output",
            ),
            node(
                unpack,
                inputs=[
                    "de_output",
                    "campaign_eval_multisim_stratified_input",
                    "params:campaign_eval_multisim_stratified",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="campaign_eval_multisim_stratified_output",
            ),
            node(
                unpack,
                inputs=[
                    "de_output",
                    "campaign_eval_tnl_grabngo_stratified_input",
                    "params:campaign_eval_tnl_grabngo_stratified",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="campaign_eval_tnl_grabngo_stratified_output",
            ),
            node(
                unpack,
                inputs=[
                    "de_output",
                    "campaign_eval_tnl_pathtochurn_stratified_input",
                    "params:campaign_eval_tnl_pathtochurn_stratified",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="campaign_eval_tnl_pathtochurn_stratified_output",
            ),
            node(
                unpack,
                inputs=[
                    "de_output",
                    "kredivo_default_probability_validation_input",
                    "params:kredivo_default_probability_validation",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="kredivo_default_probability_validation_output",
            ),
            node(
                unpack,
                inputs=[
                    "de_output",
                    "kredivo_income_input",
                    "params:kredivo_income",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="kredivo_income_output",
            ),
            node(
                unpack,
                inputs=[
                    "de_output",
                    "home_credit_default_probability_input",
                    "params:home_credit_default_probability",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="home_credit_default_probability_output",
            ),
            node(
                unpack,
                inputs=[
                    "de_output",
                    "home_credit_default_probability_validation_input",
                    "params:home_credit_default_probability_validation",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="home_credit_default_probability_validation_output",
            ),
            # Inlife and 4G
            node(
                unpack,
                inputs=[
                    "de_output",
                    "fg_scale_up_samples_input",
                    "params:fg_scale_up_samples",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="fg_scale_up_samples_output",
            ),
            node(
                unpack,
                inputs=[
                    "de_output",
                    "gamers_samples_input",
                    "params:gamers_samples",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="gamers_samples_output",
            ),
            node(
                unpack,
                inputs=[
                    "de_output",
                    "drgb_input",
                    "params:drgb",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="drgb_output",
            ),
            node(
                unpack,
                inputs=[
                    "de_output",
                    "rgb_a_samples_input",
                    "params:rgb_a_samples",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="rgb_a_samples_output",
            ),
            node(
                unpack,
                inputs=[
                    "de_output",
                    "hvc_lapser_a_input",
                    "params:hvc_lapser_a",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="hvc_lapser_a_output",
            ),
            node(
                unpack,
                inputs=[
                    "de_output",
                    "hvc_lapser_b_input",
                    "params:hvc_lapser_b",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="hvc_lapser_b_output",
            ),
            node(
                unpack,
                inputs=[
                    "de_output",
                    "multisim_4g_input",
                    "params:multisim_4g",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="multisim_4g_output",
            ),
            node(
                unpack,
                inputs=[
                    "de_output",
                    "multisim_non_4g_samples_input",
                    "params:multisim_non_4g_samples",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="multisim_non_4g_samples_output",
            ),
            node(
                unpack,
                inputs=[
                    "de_output",
                    "hvc_prop_input",
                    "params:hvc_prop",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="hvc_prop_output",
            ),
            node(
                unpack,
                inputs=[
                    "de_output",
                    "4g_lapser_input",
                    "params:4g_lapser",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="4g_lapser_output",
            ),
            node(
                unpack,
                inputs=[
                    "de_output",
                    "acq_input",
                    "params:acq",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="acq_output",
            ),
            # internal
            node(
                unpack,
                inputs=[
                    "de_output",
                    "internal_hvc_may_input",
                    "params:internal_hvc_may",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="internal_hvc_may_output",
            ),
            node(
                unpack,
                inputs=[
                    "de_output",
                    "internal_hvc_input",
                    "params:internal_hvc",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="internal_hvc_output",
            ),
            node(
                unpack,
                inputs=[
                    "de_output",
                    "internal_ucg_201910_input",
                    "params:internal_ucg_201910",
                    "params:de_output_date_column",
                    "params:de_msisdn_column",
                ],
                outputs="internal_ucg_201910_output",
            ),
        ]
    )
