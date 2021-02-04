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

import copy
from typing import Any, Dict, List

from kedro.extras.datasets.spark.spark_hive_dataset import SparkHiveDataSet as SHDS
from kedro.io import DataSetError
from pyspark.sql import DataFrame

from src.dmp.io.log_to_db import log_to_db
from utils import generate_timestamp, get_end_date, get_start_date


def log_load_data(
    database,
    table,
    version="",
    dataset_name="",
    file_format="hive",
    partition_column="",
    start_date="",
    end_date="",
):
    """
    get log data
    """

    filepath = "{0}.{1}".format(database, table)
    last_modified = ""

    logdata = {
        "version": "{}".format(version),
        "filepath": "{}".format(filepath),
        "dataset_name": "{}".format(dataset_name),
        "data_type": "{}".format(file_format),
        "start_date": "{}".format(start_date),
        "end_date": "{}".format(end_date),
        "last_modified": "{}".format(last_modified),
        "run_time": "{}".format(generate_timestamp()),
    }

    # log to db
    log_to_db(**logdata)


class SparkHiveDataSet(SHDS):
    """``SparkHiveDataSet`` loads and saves Spark data frames stored on Hive.
    This data set also handles some incompatible file types such as using partitioned parquet on
    hive which will not normally allow upserts to existing data without a complete replacement
    of the existing file/partition.

    This DataSet has some key assumptions:
    - Schemas do not change during the pipeline run (defined PKs must be present for the
    duration of the pipeline)
    - Tables are not being externally modified during upserts. The upsert method is NOT ATOMIC
    to external changes to the target table while executing.

    Example:
    ::

        >>> from pyspark.sql import SparkSession
        >>> from pyspark.sql.types import (StructField, StringType,
        >>>                                IntegerType, StructType)
        >>>
        >>> from src.dmp.io.spark_hive_dataset import SparkHiveDataSet
        >>>
        >>> schema = StructType([StructField("name", StringType(), True),
        >>>                      StructField("age", IntegerType(), True)])
        >>>
        >>> data = [('Alex', 31), ('Bob', 12), ('Clarke', 65), ('Dave', 29)]
        >>>
        >>> spark_df = SparkSession.builder.getOrCreate()\
        >>>                        .createDataFrame(data, schema)
        >>>
        >>> data_set = SparkHiveDataSet(database="test_database", table="test_table",
        >>>                             write_mode="overwrite")
        >>> data_set.save(spark_df)
        >>> reloaded = data_set.load()
        >>>
        >>> reloaded.take(4)
    """

    def __init__(
        self,
        database: str,
        table: str,
        write_mode: str = None,
        table_pk: List[str] = None,
        load_args: Dict[str, Any] = None,
        sla_interval: Dict[str, Any] = None,
    ) -> None:

        self._load_args = copy.deepcopy({})
        if load_args is not None:
            self._load_args.update(load_args)

        if not write_mode:
            write_mode = "overwrite"
        super(SparkHiveDataSet, self).__init__(
            database=database, table=table, write_mode=write_mode, table_pk=table_pk
        )

    def _load(self) -> DataFrame:

        if not self._exists():
            raise DataSetError(
                "requested table not found: {database}.{table}".format(
                    database=self._database, table=self._table
                )
            )
        partition_column = self._load_args.get("partition_column", None)
        filter_period = self._load_args.get("partition_filter_period", None)
        date_format = self._load_args.get("partition_date_format", None)

        ## default start_date and end_date for logging
        start_date = ""
        end_date = ""

        if not partition_column:
            query = "SELECT * FROM {database}.{table}".format(
                database=self._database, table=self._table
            )
        else:
            if not filter_period:
                raise IOError("partition_filter_period is missing from load_args")
            if not date_format:
                raise IOError("partition_date_format is missing from load_args")

            start_date = get_start_date(period=filter_period)
            end_date = get_end_date(
                period=filter_period, partition_column=partition_column
            )

            query = (
                "SELECT * FROM {database}.{table} "
                "WHERE {partition_column} >= '{start_date}' AND {partition_column} <= '{end_date}'"
            ).format(
                database=self._database,
                table=self._table,
                partition_column=partition_column,
                start_date=start_date.strftime(date_format),
                end_date=end_date.strftime(date_format),
            )

        # log to db
        log_load_data(
            database=self._database,
            table=self._table,
            file_format="hive",
            partition_column=partition_column,
            start_date=start_date,
            end_date=end_date,
        )

        return self._get_spark().sql(query)
