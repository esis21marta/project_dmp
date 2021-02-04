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

from src.dmp.pipelines.dmp._04_features.product.fea_nonmacro_product import (
    fea_nonmacro_product,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to create the feature layer for `product` after the data
    has been run through the primary layer. This function will be executed as part of the `feature` layer. The pipeline takes a series of nodes; with tags to
    help uniquely identify it. More information on running can be found here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the `product` nodes to execute the `feature` layer.
    """
    return Pipeline(
        [
            node(
                func=fea_nonmacro_product,
                inputs=[
                    "l1_nonmacro_product_weekly_data_final",
                    "params:features_mode",
                    "params:output_features",
                ],
                outputs="l4_fea_nonmacro_product",
                name="fea_non_macro_product",
            ),
            ##############
            # Deprecated #
            ##############
            # node(
            #     func=create_product_features_monthly,
            #     inputs=[
            #         "l2_macroproduct_scaffold_weekly",
            #         "params:macroproduct_list",
            #         "params:features_mode",
            #         "params:output_features",
            #     ],
            #     outputs="l4_fea_macroproduct_monthly",
            #     name="fea_product_macro_monthly",
            # ),
            # node(
            #     func=create_product_features_quarterly,
            #     inputs=[
            #         "l2_macroproduct_scaffold_weekly",
            #         "params:macroproduct_list",
            #         "params:features_mode",
            #         "params:output_features",
            #     ],
            #     outputs="l4_fea_macroproduct_quarterly",
            #     name="fea_product_macro_quarterly",
            # ),
            # node(
            #     func=create_macroproduct_features,
            #     inputs=[
            #         "l2_macroproduct_scaffold_weekly",
            #         "params:features_mode",
            #         "params:output_features",
            #     ],
            #     outputs="l4_fea_macroproduct_explode",
            #     name="fea_product_macro_explode",
            # ),
            # node(
            #     func=fea_macroproduct_all,
            #     inputs=[
            #         "l2_macroproduct_scaffold_weekly",
            #         "l4_fea_macroproduct_monthly",
            #         "l4_fea_macroproduct_quarterly",
            #         "l4_fea_macroproduct_explode",
            #         "params:features_mode",
            #         "params:output_features",
            #     ],
            #     outputs="l4_fea_macroproduct",
            #     name="fea_product_macro",
            # ),
            ##############
            # Deprecated #
            ##############
        ],
        tags=["fea_product", "product_pipeline", "de_feature_pipeline", "de_pipeline",],
    )
