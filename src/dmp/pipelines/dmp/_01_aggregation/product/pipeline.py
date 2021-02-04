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

from src.dmp.pipelines.dmp._01_aggregation.product.create_non_macro_product_weekly import (
    create_non_macro_product_weekly,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to aggregate `product` data into a [MSISDN, Weekstart]
    granularity.  This function will be executed as part of the `aggregation` layer. The pipeline takes a series of nodes; with tags to
    help uniquely identify it. More information on running can be found here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the `product` nodes to execute the `aggregation` layer.
    """
    return Pipeline(
        [
            node(
                func=create_non_macro_product_weekly,
                inputs=["l1_smy_usage_ocs_chg_dd", "l1_mck_sku_bucket_pivot"],
                outputs=None,  # l1_nonmacro_product_weekly_data
                name="non_macro_product_weekly",
            ),
            ##############
            # Deprecated #
            ##############
            # node(
            #     func=create_macro_product_mapping,
            #     inputs=[
            #         "l1_smy_usage_ocs_chg_dd",
            #         "l1_mck_sku_bucket_pivot",
            #         "l1_smy_product_catalogue_sku_c2c_dd",
            #         "l1_macroproduct_map_base",
            #     ],
            #     outputs=None, # l1_macroproduct_map_new
            #     name="product_create_macroproduct_mapping",
            # ),
            # node(
            #     func=create_macro_product_weekly,
            #     inputs=[
            #         "l1_base_ifrs_c2p",
            #         "l1_base_ifrs_c2p_reject",
            #         "l1_macroproduct_map_base",
            #         "l1_macroproduct_map_new",
            #         "params:macroproduct_list",
            #     ],
            #     outputs=None, # l2_macroproduct_weekly
            #     name="aggregate_product_to_weekly",
            # ),
            ##############
            # Deprecated #
            ##############
        ],
        tags=[
            "agg_product",
            # "product_pipeline",
            "de_aggregation_pipeline",
            # "de_pipeline"
        ],
    )
