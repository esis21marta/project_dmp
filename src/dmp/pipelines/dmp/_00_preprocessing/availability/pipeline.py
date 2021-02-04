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

from src.dmp.pipelines.dmp._06_qa.data_quality_helper.spark_df_to_oracle_db import (
    spark_df_to_oracle_db_wrapper,
)

from .qa_availability import qa_availability
from .qa_availability_score import availability_score
from .send_availability_email import send_availability_email


def create_pipeline(**kwargs) -> Pipeline:
    """
    Timeliness Pipeline.

    Args:
        kwargs: Ignore any additional arguments added in the future.

    Returns:
        A Pipeline object containing all the Preprocessing Nodes.
    """

    return Pipeline(
        [
            node(
                func=qa_availability,
                inputs=["params:qa_raw.catalog",],
                outputs={
                    "availability_detailed": "availability_detailed",
                    "availability_summary": "availability_summary",
                },
                name="check_availability",
                tags=[
                    "de_preprocess_pipeline",
                    "preprocess_scoring_pipeline",
                    "de_pipeline",
                    "de_qa_availability",
                ],
            ),
            node(
                func=spark_df_to_oracle_db_wrapper(
                    target_catalog="l1_qa_availability_db",
                    upsert=True,
                    primary_keys=["table_name", "domain", "run_time"],
                ),
                inputs="availability_detailed",
                outputs="l1_qa_availability_db",
                name="check_availability_db",
                tags=[
                    "de_preprocess_pipeline",
                    "preprocess_scoring_pipeline",
                    "de_qa_availability",
                    "scoring_pipeline",
                    "de_pipeline",
                ],
            ),
            node(
                func=send_availability_email,
                inputs=["availability_summary", "params:email_notif_setup",],
                outputs=None,
                name="send_availability_email",
                tags=[
                    "de_preprocess_pipeline",
                    "preprocess_scoring_pipeline",
                    "de_qa_availability",
                    "scoring_pipeline",
                    "de_pipeline",
                ],
            ),
            node(
                func=availability_score,
                inputs=["availability_detailed", "params:pipeline"],
                outputs="availability_score",
                name="availability_score",
                tags=[
                    "de_preprocess_pipeline",
                    "preprocess_scoring_pipeline",
                    "de_qa_availability",
                    "scoring_pipeline",
                    "de_pipeline",
                ],
            ),
            node(
                func=spark_df_to_oracle_db_wrapper(
                    target_catalog="l1_qa_availability_score_db",
                    upsert=True,
                    primary_keys=[
                        "run_time",
                        "layer",
                        "master_mode",
                        "dimension",
                        "domain",
                    ],
                ),
                inputs="availability_score",
                outputs="l1_qa_availability_score_db",
                name="availability_score_db",
                tags=[
                    "de_preprocess_pipeline",
                    "preprocess_scoring_pipeline",
                    "de_qa_availability",
                    "scoring_pipeline",
                    "de_pipeline",
                ],
            ),
        ],
    )
