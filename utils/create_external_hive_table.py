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

from pyspark.sql import SparkSession

log = logging.getLogger(__name__)


def create_external_hive_table(
    table_schema: str,
    table_name: str,
    file_path: str,
    file_format: str = "PARQUET",
    partition_columns: list = None,
) -> None:
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.parquet.mergeSchema", "true")

    df = spark.read.load(file_path, file_format)

    log.info(f"CREATING TABLE {table_schema}.{table_name}")

    drop_sql = f"DROP TABLE IF EXISTS {table_schema}.{table_name}"
    repair_sql = f"MSCK REPAIR TABLE {table_schema}.{table_name}"

    if partition_columns:
        # Get All the columns and their data types
        list_of_partitions = []
        list_of_columns = []
        for curr_col in df.schema.jsonValue()["fields"]:
            if curr_col["name"] in partition_columns:
                list_of_partitions.append(
                    f"{curr_col['name']}  {get_hive_data_type(spark_data_type=curr_col['type'], file_format=file_format)}"
                )
            else:
                list_of_columns.append(
                    f"{curr_col['name']}  {get_hive_data_type(spark_data_type=curr_col['type'], file_format=file_format)}"
                )
        partition_list = ", ".join(list_of_partitions)
        column_list = ", ".join(list_of_columns)

        # Create Hive SQL
        create_sql = f"""
            CREATE EXTERNAL TABLE {table_schema}.{table_name}
            (
            {column_list}
            )
            PARTITIONED BY ({partition_list})
            STORED AS {file_format}
            LOCATION '{file_path}'
        """
    else:
        # Get All the columns and their data types
        list_of_columns = []
        for curr_col in df.schema.jsonValue()["fields"]:
            list_of_columns.append(
                f"{curr_col['name']}  {get_hive_data_type(spark_data_type=curr_col['type'], file_format=file_format)}"
            )
        column_list = ", ".join(list_of_columns)

        # Create Hive SQL
        create_sql = f"""
            CREATE EXTERNAL TABLE {table_schema}.{table_name}
            (
            {column_list}
            )
            STORED AS {file_format}
            LOCATION '{file_path}';
        """

    # Run Hive SQL
    spark.sql(sqlQuery=drop_sql)
    spark.sql(sqlQuery=create_sql)
    spark.sql(sqlQuery=repair_sql)


def get_hive_data_type(spark_data_type, file_format: str) -> str:
    # Spark Hive Data Type Mapping
    spark_hive_mapping = {
        "byte": "TinyInt",
        "short": "SmallInt",
        "integer": "Int",
        "long": "BigInt",
        "float": "Float",
        "double": "Double",
        "string": "String",
        "binary": "Binary",
        "boolean": "Boolean",
        "timestamp": "Timestamp",
        "date": {"PARQUET": "Timestamp", "ORC": "Date"},
    }

    file_format = file_format.upper()

    if type(spark_data_type) == str:
        spark_data_type = spark_data_type.lower()
        if spark_data_type.startswith("decimal"):
            hive_data_type = spark_data_type
        elif "date" == spark_data_type:
            hive_data_type = spark_hive_mapping.get(spark_data_type).get(
                file_format, "date"
            )
        else:
            hive_data_type = spark_hive_mapping.get(spark_data_type, "")
            if hive_data_type == "":
                log.error(f"UNSUPPORTED DATA TYPE: {spark_data_type}")
    elif type(spark_data_type) == dict and spark_data_type["type"] == "array":
        hive_data_type = (
            spark_data_type["type"] + "<" + spark_data_type["elementType"] + ">"
        )
    else:
        log.error(f"UNSUPPORTED DATA TYPE: {spark_data_type}")
        hive_data_type = ""

    return hive_data_type
