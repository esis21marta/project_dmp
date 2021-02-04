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
# or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import re
import subprocess

import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from utils import get_config_parameters
from utils.spark_data_set_helper import get_catalog_file_path


def get_ls_output_for_filepath(filepath: str) -> pd.DataFrame:
    """Execute hdfs dfs ls command on filepath of table

    Args:
        filepath (str): Filepath of table

    Raises:
        IOError

    Returns:
        pd.DataFrame: Pandas dataframe with column ls_output
    """
    process = subprocess.Popen(
        f"hdfs dfs -ls -R {filepath}".split(" "),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = process.communicate()
    stdout = stdout.decode("utf-8")
    stderr = stderr.decode("utf-8")

    if stderr:
        raise IOError(stderr)

    df = pd.DataFrame(stdout.split("\n"), columns=["ls_output"])
    return df


def get_timeliness(
    filepath: str,
    table_name: str,
    sla_date: int = 6,
    versioned: bool = True,
    date_partitioned_column: str = "weekstart",
    date_partitioned_column_format: str = "%Y-%m-%d",
) -> pd.DataFrame:
    """Calculate timeliness metrics

    Args:
        filepath (str): Filepath of the table
        table_name (str): Table name
        sla_date (int, optional): SLA period to calculate expected delivery data of table. Defaults to 6.
        versioned (bool, optional): Whether table is versioned or not. Defaults to True.
        date_partitioned_column (str, optional): Partition of table for date. Defaults to "weekstart".
        date_partitioned_column_format (str, optional): Format of values in date_partitioned_column. Defaults to "%Y-%m-%d"
    Returns:
        pd.DataFrame: Pandas dataframe of timeliness QA metrics
    """
    df = get_ls_output_for_filepath(filepath)

    # Remove rows without modified time column
    modified_time_regex = "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2} " + filepath
    df = df[df["ls_output"].str.contains(modified_time_regex)]

    # If master table is versioned, remove rows without versioned path
    if versioned:
        created_at_regex = filepath + "/created_at=(.+?)/"
        df = df[df["ls_output"].str.contains(created_at_regex)]

    # Filter for date partitioned column
    date_partitioned_column_regex = filepath + f".*/{date_partitioned_column}=(.+?)/"
    df = df[df["ls_output"].str.contains(date_partitioned_column_regex)]

    # Extract modified time
    df["modified_time"] = df["ls_output"].apply(
        lambda x: datetime.datetime.strptime(
            re.findall("([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}) " + filepath, x)[
                0
            ],
            "%Y-%m-%d %H:%M",
        )
    )

    # If master table is versioned, extract versioned
    if versioned:
        df["version"] = df["ls_output"].apply(
            lambda x: datetime.datetime.strptime(
                re.findall(filepath + "/created_at=(.+?)/", x)[0], "%Y-%m-%d"
            )
        )

    # Date partitoned column
    df[date_partitioned_column] = df["ls_output"].apply(
        lambda x: datetime.datetime.strptime(
            re.findall(f"{filepath}.*/{date_partitioned_column}=(.+?)/", x)[0],
            date_partitioned_column_format,
        )
    )

    if versioned:
        df_version_run_time = df.groupby(["version"], as_index=False)[
            [date_partitioned_column]
        ].max()
        df_version_last_modified = df.groupby(["version"], as_index=False)[
            ["modified_time"]
        ].max()
        df_version_run_time_last_modified = df_version_run_time.merge(
            df_version_last_modified, on="version"
        )
        df_timeliness = (
            df_version_run_time_last_modified.sort_values(
                by=["version", "modified_time", date_partitioned_column]
            )
            .groupby([date_partitioned_column], as_index=False)
            .last()
        )

    # If not versioned, then select the latest date_partitioned_column and modified time
    else:
        df_timeliness = pd.DataFrame(
            [
                {
                    "modified_time": df["modified_time"].max(),
                    date_partitioned_column: df[date_partitioned_column].max(),
                }
            ]
        )

    df_timeliness["run_time"] = df_timeliness[date_partitioned_column]
    df_timeliness["expected"] = df_timeliness[date_partitioned_column].apply(
        lambda x: x + datetime.timedelta(days=sla_date)
    )
    df_timeliness["delay"] = (
        df_timeliness["modified_time"] - df_timeliness["expected"]
    ).apply(lambda x: float(round(max(0.0, x.total_seconds()) / 3600, 3)))
    df_timeliness = df_timeliness.rename(columns={"modified_time": "actual"})
    df_timeliness["table_name"] = table_name
    return df_timeliness[["table_name", "run_time", "actual", "expected", "delay"]]


def check_timeliness_wrapper(table_name, catalog_name: str):
    """Wrapper function for check_timeliness

    Args:
        catalog_name (str): Catalog name
    """

    def check_timeliness(master_mode: str,) -> pyspark.sql.DataFrame:
        """Calculate timeliness metrics

        Returns:
            pyspark.sql.DataFrame: Timeliness metrics
        """

        # Check Path and SLA
        filepath = get_catalog_file_path(catalog_name=catalog_name)

        master_catalog = get_config_parameters(config="catalog")[catalog_name]
        versioned = master_catalog["create_versions"]

        sla_date = master_catalog["sla_interval"]["date"]
        date_partitioned_column = master_catalog["sla_interval"][
            "date_partitioned_column"
        ]
        date_partitioned_column_format = master_catalog["sla_interval"][
            "date_partitioned_column_format"
        ]

        # Calculate timeliness metrics
        df_timeliness = get_timeliness(
            filepath=filepath,
            table_name=table_name,
            sla_date=sla_date,
            versioned=versioned,
            date_partitioned_column=date_partitioned_column,
            date_partitioned_column_format=date_partitioned_column_format,
        )

        df_timeliness["layer"] = "master"
        df_timeliness["master_mode"] = master_mode

        # Export Result to spark dataframe
        spark_session = SparkSession.builder.getOrCreate()
        df_timeliness = spark_session.createDataFrame(
            df_timeliness,
            schema=StructType(
                [
                    StructField("table_name", StringType(), False),
                    StructField("run_time", DateType(), False),
                    StructField("actual", TimestampType(), False),
                    StructField("expected", TimestampType(), False),
                    StructField("delay", DoubleType(), False),
                    StructField("layer", StringType(), False),
                    StructField("master_mode", StringType(), False),
                ]
            ),
        )
        return df_timeliness

    return check_timeliness
