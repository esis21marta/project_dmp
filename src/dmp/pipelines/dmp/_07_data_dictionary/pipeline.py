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

from src.dmp.pipelines.dmp._07_data_dictionary.data_dictionary import data_dictionary
from src.dmp.pipelines.dmp._07_data_dictionary.data_dictionary_column import (
    data_dictionary_column_tabular,
)
from src.dmp.pipelines.dmp._07_data_dictionary.data_dictionary_table import (
    data_dictionary_table,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    This function returns a pipeline which executes the necessary nodes to create the Data Dictionary layer.
    This function will be executed as part of the `data dictionary` layer.
    The pipeline takes a series of nodes; with tags to help uniquely identify it.
    More information on running can be found
    here: https://tsdmp.atlassian.net/wiki/spaces/IN/pages/205914223/How+to+run+Pipelines.

    Returns:
        A Pipeline object containing all the `Data Dictionary` nodes to execute the `data dictionary` layer.
    """

    return Pipeline(
        [
            node(
                func=data_dictionary_column_tabular,
                inputs=[
                    "data_dictionary",
                    "params:hdfs_base_path",
                    "params:pii_level_columns",
                ],
                outputs="data_dictionary_column_tabular",
                name="data_dictionary_column_tabular",
                tags=[
                    "de_dmp_data_dictionary",
                    "de_dmp_data_dictionary_column_tabular",
                ],
            ),
            node(
                func=data_dictionary_table,
                inputs=[
                    "data_dictionary",
                    "data_dictionary_table_raw",
                    "params:hdfs_base_path",
                    "params:pii_level_columns",
                ],
                outputs="data_dictionary_table",
                name="data_dictionary_table",
                tags=["de_dmp_data_dictionary", "de_dmp_data_dictionary_table"],
            ),
            node(
                func=data_dictionary,
                inputs=["data_dictionary_column_tabular", "data_dictionary_table",],
                outputs=None,
                name="data_dictionary",
                tags=["de_dmp_data_dictionary"],
            ),
        ],
        tags=["de_pipeline", "de_data_dictionary"],
    )
