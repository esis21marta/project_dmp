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

from typing import Any, Dict

import pyspark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType


def packing_partner_msisdns(files_to_pack: Dict[str, Any]) -> pyspark.sql.DataFrame:
    """
    Creates a table of distinct MSISDNs and their respective use cases to build the features for.

    Returns:
        A two column DataFrame of distinct MSISDNs mapped to their respective use cases.
    """
    spark = SparkSession.builder.getOrCreate()
    schema = StructType(
        [
            StructField("msisdn", StringType(), False),
            StructField("use_case", StringType(), False),
        ]
    )
    data = []
    packed_df = spark.createDataFrame(schema=schema, data=data)
    for use_case, metadata in files_to_pack.items():
        metadata_input = metadata["input"]
        input_filepath = metadata_input.get("filepath")
        packing_input = spark.read.parquet(input_filepath)
        packing_input = (
            packing_input.withColumnRenamed(metadata["msisdn_column"], "msisdn")
            .select("msisdn")
            .withColumn("use_case", f.lit(use_case))
        )
        packed_df = packed_df.union(packing_input)

    partner_msisdns = packed_df.groupby(f.col("msisdn")).agg(
        f.collect_set(f.col("use_case")).alias("use_case")
    )

    return partner_msisdns
