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

from kedro.extras.datasets.spark.spark_dataset import SparkDataSet as SDS
from kedro.extras.datasets.spark.spark_dataset import _strip_dbfs_prefix
from kedro.io import Version
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.dmp.io.log_to_db import log_to_db
from utils import generate_timestamp, get_end_date, get_start_date
from utils.spark_data_set_helper import (
    get_file_path,
    get_versioned_load_file_path,
    get_versioned_save_file_path,
)


def _get_repartitioned_data(data: DataFrame, partitions: int) -> DataFrame:
    if partitions:
        data = data.repartition(numPartitions=partitions)

    return data


def log_load_data(
    filepath,
    version,
    version_index,
    start_date="",
    end_date="",
    file_format="parquet",
    dataset_name="",
):
    """
    get log data
    """
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


class SparkDataSet(SDS):
    """``SparkDataSet`` loads and saves Spark data frames.

    Example:
    ::

        >>> from pyspark.sql import SparkSession
        >>> from pyspark.sql.types import (StructField, StringType,
        >>>                                IntegerType, StructType)
        >>>
        >>> from src.dmp.io.spark_data_set import SparkDataSet
        >>>
        >>> schema = StructType([StructField("name", StringType(), True),
        >>>                      StructField("age", IntegerType(), True)])
        >>>
        >>> data = [('Alex', 31), ('Bob', 12), ('Clarke', 65), ('Dave', 29)]
        >>>
        >>> spark_df = SparkSession.builder.getOrCreate()\
        >>>                        .createDataFrame(data, schema)
        >>>
        >>> data_set = SparkDataSet(filepath="test_data")
        >>> data_set.save(spark_df)
        >>> reloaded = data_set.load()
        >>>
        >>> reloaded.take(4)
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        filepath: str,
        file_format: str = "parquet",
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
        version: Version = None,
        credentials: Dict[str, Any] = None,
        create_versions: bool = False,
        partitions: int = None,
        is_raw_source: bool = False,
        sla_interval: Dict[str, Any] = None,
    ) -> None:

        self._create_versions = create_versions
        self._partitions = partitions
        self._is_raw_source = is_raw_source

        filepath = get_file_path(filepath=filepath)

        super(SparkDataSet, self).__init__(
            filepath=filepath,
            file_format=file_format,
            load_args=load_args,
            save_args=save_args,
            version=version,
            credentials=credentials,
        )

    def _load(self) -> DataFrame:
        """
        Loads a dataset
        :return: Dataframe
        """

        load_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_load_path()))

        version = self._load_args.pop("version", None)
        version_index = self._load_args.pop("version_index", None)
        partition_column = self._load_args.pop("partition_column", None)
        date_format = self._load_args.pop("partition_date_format", None)
        filter_period = self._load_args.pop("partition_filter_period", None)

        # If create_versions flag is set, Load the pinned version if set otherwise load the latest version.
        if self._create_versions:
            load_path = get_versioned_load_file_path(
                filepath=load_path, version=version, version_index=version_index
            )

        # If no partition column is specified, load the whole dataset
        if not partition_column:
            # log to db
            log_load_data(
                filepath=load_path,
                version=version,
                version_index=version_index,
                file_format=self._file_format,
            )
            return self._get_spark().read.load(
                load_path, self._file_format, **self._load_args
            )
        else:
            if not filter_period:
                raise IOError("partition_filter_period is missing from load_args")
            if not date_format:
                raise IOError("partition_date_format is missing from load_args")

            start_date = get_start_date(
                period=filter_period, partition_column=partition_column
            )
            end_date = get_end_date(
                period=filter_period, partition_column=partition_column
            )

            # log to db
            log_load_data(
                filepath=load_path,
                version=version,
                version_index=version_index,
                start_date=start_date,
                end_date=end_date,
                file_format=self._file_format,
            )

            return (
                self._get_spark()
                .read.load(load_path, self._file_format, **self._load_args)
                .filter(
                    col(partition_column).between(
                        start_date.strftime(date_format), end_date.strftime(date_format)
                    )
                )
            )

    def _save(self, data: DataFrame) -> None:
        """
        Writes the dataframe, passed as an argument.

        :param data: dataframe to be saved
        :return: None
        """
        save_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_save_path()))

        # If create_versions flag is set, add version (created_at) in the save_path
        if self._create_versions:
            save_path = get_versioned_save_file_path(save_path)

        # If Partitions are set, then repartition the data before saving to HDFS
        if self._partitions:
            data = _get_repartitioned_data(data=data, partitions=self._partitions)

        data.write.save(save_path, self._file_format, **self._save_args)
