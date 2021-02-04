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

from datetime import datetime

import pyspark
import pyspark.sql.functions as f
from kedro.pipeline import node


def get_sample_fraction(sample_count: int, distinct_msisdn_count: int):
    """
    Returns fraction for sample using sample count and distinct_msisdn_count
    Args:
        sample_count (int):
        distinct_msisdn_count (int):

    Returns:
        fraction for sample
    """
    fraction = min(sample_count / distinct_msisdn_count, 1.0)
    return fraction


def sample_msisdn(
    df: pyspark.sql.DataFrame, sample_count: int, pipeline: str
) -> pyspark.sql.DataFrame:
    """
    Sample msisdns from a dataframe
    Args:
        df (pyspark.sql.DataFrame): Dataframe with msisdns from which we have to take sample
        sample_count (int): Number of samples we want to take

    Returns:
        Dataframe with unique msisdns
    """
    latest_weekstart = df.agg({"weekstart": "max"}).collect()[0][0]
    distinct_msisdn = df.filter(f.col("weekstart") == latest_weekstart).select("msisdn")
    distinct_msisdn_count = distinct_msisdn.count()
    sample_fraction = get_sample_fraction(sample_count, distinct_msisdn_count)
    unique_msisdns_df = (
        distinct_msisdn.sample(withReplacement=False, fraction=sample_fraction)
        .withColumn("master_mode", f.lit(pipeline))
        .withColumn("run_time", f.lit(datetime.now()))
    )
    return unique_msisdns_df


def get_sample_msisdn_node(pipeline_mode: str):
    """
    Returns the node for sampling MSISDNs
    Args:
        pipeline_mode: Pipeline Mode.
    """

    return node(
        func=sample_msisdn,
        inputs=["l6_master", "params:sample_size", "params:pipeline"],
        outputs="l4_qa_sample_unique_msisdn",
        name="qa_sample_msisdn",
        tags=["de_qa"],
    )
