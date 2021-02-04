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

import datetime as dt
import logging
from datetime import datetime
from typing import List

import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from utils import get_config_parameters, get_end_date, union_all_with_name

log = logging.getLogger(__name__)


def check_table_availability(
    table_name: str,
    start_date,
    end_date,
    partition_column: str,
    partition_date_format: str,
    spark_session,
):
    """
    Check whether the data is available in the given date

    Args:
        table_name: Full name of the table (dbname.table_name)
        start_date: Date from which we need to check the data
        end_date: Date till which we need to check the data
        partition_column: Partition Column
        partition_date_format: Date format

    Return:
        Dataframe with date and is_available column

    """
    availability_status = []

    current_date = start_date

    def check_if_partition_columns_exist(partitions, partition_column):
        partition_list = partitions.split("/")
        partition_list_keys = [partition.split("=")[0] for partition in partition_list]
        return partition_column in partition_list_keys

    def extract_partition_value(partition, partition_column):
        all_partions = partition.split("/")
        required_partiton = [
            partition
            for partition in all_partions
            if partition.split("=")[0] == partition_column
        ]
        partition_date = required_partiton[0].split("=")[-1]
        return partition_date

    partition_rows = spark_session.sql(f"SHOW partitions {table_name}").collect()

    # If table is partitioned using partition column
    if len(partition_rows) > 0 and check_if_partition_columns_exist(
        partition_rows[0].partition, partition_column
    ):
        available_dates = [
            extract_partition_value(row.partition, partition_column)
            for row in partition_rows
        ]

    # If table is not partitioned using partition column
    else:
        df = spark_session.read.table(table_name)
        available_dates = [
            row[partition_column]
            for row in df.select(partition_column).distinct().collect()
        ]

    available_dates_dt_objects = []

    for available_date in available_dates:
        try:
            available_dates_dt_objects.append(
                datetime.strptime(available_date, partition_date_format).date()
            )
        except:
            log.warn(
                f"Failed to read partition {available_date} for table {table_name}"
            )

    while current_date <= end_date:
        availability_status.append(
            dict(
                date=current_date,
                is_available=int(current_date in available_dates_dt_objects),
            )
        )
        current_date += dt.timedelta(days=1)

    return spark_session.createDataFrame(availability_status)


def add_metadata(df, full_table_name, domain_name):
    availability_status_max_date = df.agg(
        f.date_add(f.max("date"), 1).alias(
            "run_time"
        )  # Runtime is the last weekstart for which check is running
    )
    return (
        df.withColumn("table_name", f.lit(full_table_name))
        .withColumn("domain", f.lit(domain_name))
        .crossJoin(availability_status_max_date)
    )


def check_domain_availability(domain_name: str, catalog_names_list: List[str]):
    """
    This function will check whether the data sources are updated based on SLA defined in the catalog.yml

    Args:
        df_data: Dataframe of source tables.

    Returns:
        Dataframe that contain one column for flag
    """
    end_date = get_end_date()

    conf_catalog = get_config_parameters(config="catalog")

    availability_status_df_list = []

    spark_session = SparkSession.builder.getOrCreate()

    for catalog_name in catalog_names_list:
        try:
            load_args = conf_catalog[catalog_name].get("load_args", None)
            sla_interval = conf_catalog[catalog_name].get("sla_interval", None)
            if load_args:
                partition_column = load_args["partition_column"]
                partition_date_format = load_args["partition_date_format"]
            else:
                partition_column = sla_interval["column_reference"]
                partition_date_format = sla_interval["date_format"]
            start_date = datetime.strptime(
                sla_interval["start_date"], "%Y-%m-%d"
            ).date()

            db_name = conf_catalog[catalog_name]["database"]
            table_name = conf_catalog[catalog_name]["table"]
            full_table_name = f"{db_name}.{table_name}"

            log.info(f"Checking availability date for: {full_table_name}")

            availability_status_df = check_table_availability(
                full_table_name,
                start_date,
                end_date,
                partition_column,
                partition_date_format,
                spark_session,
            )

            availability_status_df = add_metadata(
                availability_status_df, full_table_name, domain_name
            )

            availability_status_df_list.append(availability_status_df)
        except Exception:
            raise Exception(f"Failed to check availability for {catalog_name} table")

    availability_status_df_combined = union_all_with_name(availability_status_df_list)

    return availability_status_df_combined
