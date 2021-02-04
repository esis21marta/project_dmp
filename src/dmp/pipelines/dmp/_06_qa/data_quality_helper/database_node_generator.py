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

from kedro.pipeline import node

from src.dmp.pipelines.dmp._06_qa.data_quality_helper.spark_df_to_oracle_db import (
    spark_df_to_oracle_db_wrapper,
)


def get_database_nodes(layer: str):

    nodes = [
        node(
            func=spark_df_to_oracle_db_wrapper(
                f"l4_qa_{layer}_metrics_db",
                upsert=True,
                primary_keys=["layer", "table_name", "run_time"],
            ),
            inputs=f"l4_qa_{layer}_metrics",
            outputs=f"l4_qa_{layer}_metrics_db",
            name=f"qa_{layer}_metrics_db",
            tags=["de_qa", "de_qa_to_db", f"de_qa_to_db_{layer}", f"de_qa_{layer}"],
        ),
        node(
            func=spark_df_to_oracle_db_wrapper(
                f"l4_qa_{layer}_monthly_unique_msisdn_db",
                upsert=True,
                primary_keys=["layer", "table_name", "run_time"],
            ),
            inputs=f"l4_qa_{layer}_monthly_unique_msisdn",
            outputs=f"l4_qa_{layer}_monthly_unique_msisdn_db",
            name=f"qa_{layer}_monthly_unique_msisdn_db",
            tags=["de_qa", "de_qa_to_db", f"de_qa_to_db_{layer}", f"de_qa_{layer}"],
        ),
        node(
            func=spark_df_to_oracle_db_wrapper(
                f"l4_qa_{layer}_outliers_db",
                upsert=True,
                primary_keys=["layer", "table_name", "run_time"],
            ),
            inputs=f"l4_qa_{layer}_outliers",
            outputs=f"l4_qa_{layer}_outliers_db",
            name=f"qa_{layer}_outliers_db",
            tags=["de_qa", "de_qa_to_db", f"de_qa_to_db_{layer}", f"de_qa_{layer}"],
        ),
    ]

    return nodes
