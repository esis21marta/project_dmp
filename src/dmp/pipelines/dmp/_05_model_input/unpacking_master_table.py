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

import logging
import os

import pyspark
import pyspark.sql.functions as f
from kedro.pipeline import node
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

from utils import get_project_context, previous_week_start_day

log = logging.getLogger(__name__)


def get_output_partition_count(df_partner_msisdn, usecase):
    distinct_msisdn_count = (
        df_partner_msisdn.withColumn("use_case", f.explode(f.col("use_case")))
        .filter(f.col("use_case") == usecase)
        .count()
    )
    return max(
        1, int(distinct_msisdn_count * (25 / 150_000))
    )  # For every 150_000 MSISDNs, 25 partitions.


def unpack_master_table_node_wrapper(usecase: str):
    def unpack_master_table_node(
        df_master: pyspark.sql.DataFrame,
        df_partner_msisdn: pyspark.sql.DataFrame,
        dmp_msisdn_column: str,
        dmp_output_date_column: str,
        output_directory: str,
        metadata,
    ):
        spark = SparkSession.builder.getOrCreate()

        usecase_master = df_master.filter(f.col("use_case") == usecase)

        input_filepath = metadata.get("input").get("filepath")
        data = spark.read.parquet(input_filepath)
        output_partitions = metadata.get("input").get("output_partitions", None)
        if not output_partitions:
            output_partitions = get_output_partition_count(df_partner_msisdn, usecase)

        # Hash MSISDN
        salt = os.getenv("MSISDN_HASH_SALT")

        data = data.withColumn(
            dmp_msisdn_column,
            f.concat(
                f.col(metadata.get("msisdn_column")).substr(1, 5),
                f.md5(f.concat(f.col(metadata.get("msisdn_column")), f.lit(salt))),
            ),
        )

        if dmp_msisdn_column != metadata.get("msisdn_column"):
            data.drop(f.col(metadata.get("msisdn_column")))

        date_column_name = (
            metadata.get("input").get("date_column", {}).get("name", None)
        )
        date_column_format = (
            metadata.get("input").get("date_column", {}).get("format", None)
        )

        other_columns_of_interest = (
            metadata.get("input").get("other_columns_of_interest", []) or []
        )

        partitionby_columns = metadata.get("input").get("partitionBy")

        if date_column_name is None:
            to_select = [dmp_msisdn_column] + other_columns_of_interest
            join_on = [dmp_msisdn_column]
        else:
            data = data.withColumn(
                dmp_output_date_column,
                previous_week_start_day(
                    to_date(col(date_column_name), date_column_format)
                ),
            )
            to_select = (
                [dmp_msisdn_column]
                + other_columns_of_interest
                + [dmp_output_date_column]
            )
            join_on = [
                dmp_msisdn_column,
                dmp_output_date_column,
            ]

        result = data.select(to_select).join(usecase_master, on=join_on, how="inner")
        target_path = os.path.join(output_directory, f"{usecase}.parquet")
        result = result.drop(f.col("use_case"))
        if not partitionby_columns:
            result.repartition(output_partitions).write.mode("overwrite").save(
                target_path, format="parquet"
            )
        else:
            result.repartition(output_partitions).write.partitionBy(
                partitionby_columns
            ).mode("overwrite").save(target_path, format="parquet")

    return unpack_master_table_node


def unpacking_node_generator():
    context = get_project_context()
    unpacking_usecases = context.params.get("packing_unpacking", {}).get(
        "files_to_pack", []
    )
    nodes = []
    for usecase in unpacking_usecases:
        nodes.append(
            node(
                func=unpack_master_table_node_wrapper(usecase=usecase),
                inputs=[
                    "l6_master",
                    "partner_msisdns",
                    "params:packing_unpacking.dmp_msisdn_column",
                    "params:packing_unpacking.dmp_output_date_column",
                    "params:packing_unpacking.output_directory",
                    f"params:packing_unpacking.files_to_pack.{usecase}",
                ],
                outputs=None,
                name=f"unpacking_{usecase}_master_table",
                tags=["unpacking_pipeline", f"unpacking_{usecase}"],
            )
        )
    return nodes
