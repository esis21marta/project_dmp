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
from datetime import datetime, timedelta
from typing import Dict, List

import pyspark
import pyspark.sql.functions as f
import pytz
from kedro.pipeline import node
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from src.dmp.pipelines.dmp._05_model_input.check_feature_mapping import (
    check_feature_mapping,
)
from utils import (
    add_prefix_to_fea_columns,
    get_config_parameters,
    get_end_date,
    get_required_output_columns,
    get_start_date,
)
from utils.spark_data_set_helper import get_file_path, get_versioned_save_file_path

log = logging.getLogger(__name__)


def build_msisdn_master_table_weekly_wrapper(weekstart, save_version: datetime = None):
    def build_msisdn_master_table_weekly(
        pipeline: str,
        shuffle_partitions: int,
        features_mapping: Dict,
        df_master_dimension: pyspark.sql.DataFrame,
        *df_features: pyspark.sql.DataFrame,
    ):
        """Process one week of master table data. Join data from all the feature tables and write to master table path.

        Args:
            pipeline (str): Pipeline kedro env variable.
            features_mapping (Dict): DE to DS feature mapping.
            df_master_dimension (pyspark.sql.DataFrame): Master Dimension DataFrame which have all the msisdn & Weekstart
            *df_features (pyspark.sql.DataFrame): Feature tables
        """

        # Updated Shuffle Partitions
        spark = SparkSession.builder.getOrCreate()
        spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)

        conf_catalog = get_config_parameters(config="catalog")

        master_catalog = conf_catalog["l6_master"]
        save_args = master_catalog["save_args"]

        save_args = master_catalog["save_args"]
        save_args["partitionBy"].remove("weekstart")
        if len(save_args["partitionBy"]) == 0:
            save_args.pop("partitionBy", None)

        file_path = get_file_path(filepath=master_catalog["filepath"])
        file_path = get_versioned_save_file_path(file_path, version=save_version)
        file_format = master_catalog["file_format"]
        partitions = int(master_catalog["partitions"])

        # Feature tables renamed:
        df_features_renamed = []
        for df_feature in df_features:
            df_features_renamed.append(
                check_feature_mapping(df_feature, features_mapping)
            )

        # Filter weekstart
        df_master = df_master_dimension.filter(f.col("weekstart") == weekstart).drop(
            "weekstart"
        )

        for df_feature_renamed in df_features_renamed:
            df_feature_renamed = df_feature_renamed.filter(
                f.col("weekstart") == weekstart
            ).drop("weekstart")
            df_master = df_master.join(df_feature_renamed, "msisdn", how="left")

        partition_file_path = f"{file_path}/weekstart={weekstart}"

        if pipeline == "training":
            # Because of hashing
            df_master = df_master.drop("msisdn")
            df_master = df_master.withColumnRenamed("msisdn_hash", "msisdn")
        df_master.repartition(numPartitions=partitions).write.save(
            partition_file_path, file_format, **save_args
        )

    return build_msisdn_master_table_weekly


def build_master_table_node_generator(
    pipeline: str, inputs: List[str], tags: List[str]
):
    """Generates nodes for master table. Each node is for one weekstart.

    Args:
        pipeline (str): Pipeline kedro env variable.
        inputs (List[str]): Inputs used in the build_msisdn_master_table_weekly node
        tags (List[str]): Tags used in build_msisdn_master_table_weekly_wrapper node
    """
    nodes = []

    # Get first and last weekstart, we will generate nodes for each weekstart
    first_weekstart = get_start_date(partition_column="weekstart")
    last_weekstart = get_end_date(partition_column="weekstart")

    current_weekstart = first_weekstart + timedelta(days=1)

    save_version = datetime.now(pytz.timezone("Asia/Jakarta"))

    while current_weekstart <= last_weekstart:
        nodes.append(
            node(
                func=build_msisdn_master_table_weekly_wrapper(
                    weekstart=current_weekstart, save_version=save_version
                ),
                inputs=inputs,
                outputs=None,
                name=f"build_{pipeline}_master_table_msisdn_{current_weekstart.strftime('%Y-%m-%d')}",
                tags=tags,
            )
        )
        current_weekstart += timedelta(days=7)
    return nodes


def build_master_dimension(
    df_scaffold: pyspark.sql.DataFrame, df_msisdns: pyspark.sql.DataFrame, pipeline: str
):
    """
        Creates Master Table Dimension

        Args:
            df_scaffold: Weekly Scaffold Data
            df_msisdns: Partner MSISDNs
            pipeline: Pipeline Name

        Returns:
            df_master_dim: Master Dimension Data
    """
    if "training" == pipeline:
        salt = os.getenv("MSISDN_HASH_SALT")
        df_master_dim = (
            df_scaffold.join(df_msisdns, ["msisdn"])
            .withColumn("use_case", f.explode(f.col("use_case")))
            .withColumn("use_case", f.trim(f.col("use_case")))
            .select("msisdn", "use_case", "weekstart")
            .distinct()
            .withColumn(
                "msisdn_hash",
                f.concat(
                    f.col("msisdn").substr(1, 5),
                    f.md5(f.concat(f.col("msisdn"), f.lit(salt))),
                ),
            )
        )
    else:
        df_master_dim = df_scaffold

    return df_master_dim


def build_master_table(key: str):
    def _build_master_table(
        df_master_dimension: pyspark.sql.DataFrame,
        features_mapping: dict,
        *df_features: pyspark.sql.DataFrame,
    ) -> pyspark.sql.DataFrame:
        """
        Final DataFrame created by joining all the Feature DataFrames

        Args:
            df_master_dimension: Master Dimension DataFrame which have all the Key & Weekstart
            feature_mapping: parameters mapping between existing feature name and the one being developed
            *df_features: All the Feature DataFrames which needs to be joined into Master Table
                        (JOIN WILL BE DONE ON Key & Weekstart)

        Returns:
            df_master_dimension: Final DataFrame created by joining all the Feature DataFrames
        """

        for df_feature in df_features:
            df_fea = check_feature_mapping(df_feature, features_mapping)
            df_master_dimension = df_master_dimension.join(
                df_fea, [key, "weekstart"], how="left"
            )

        return df_master_dimension

    return _build_master_table


def _create_feature_metadata_table(
    df: pyspark.sql.DataFrame, spark: SparkSession, created_at: str
) -> pyspark.sql.DataFrame:
    row_count = df.limit(2).count()

    # Create Schema & Data for the metadata
    column_list = [
        StructField(name="created_at", dataType=StringType(), nullable=False)
    ]
    column_data = [created_at]
    for col in df.columns:
        if col not in ["msisdn", "weekstart"]:
            column = StructField(name=col, dataType=BooleanType(), nullable=False)
            column_list.append(column)
            if row_count == 0:
                column_data.append(False)
            else:
                column_data.append(True)

    schema = StructType(column_list)
    data = [tuple(column_data)]

    # Create Metadata DataFrame
    df_meta = spark.createDataFrame(data=data, schema=schema)

    return df_meta


def build_feature_metadata_table(
    created_at: str, *df_features: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    df_meta = None
    spark = SparkSession.builder.getOrCreate()
    for df_fea in df_features:
        # df_fea = check_feature_mapping(
        #     df_feature=df_fea, features_mapping=features_mapping
        # )
        df = _create_feature_metadata_table(
            df=df_fea, spark=spark, created_at=created_at
        )
        if df_meta is None:
            df_meta = df
        else:
            df_meta = df_meta.join(df, ["created_at"])

    return df_meta


def build_msisdn_master_table(
    pipeline: str,
    features_mapping: dict,
    df_master_dimension: pyspark.sql.DataFrame,
    *df_features: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Final DataFrame created by joining all the Feature DataFrames

    Args:
        pipeline: Pipeline Name
        features_mapping: Mapping of DMP Features Names to DS Names
        df_master_dimension: Master Dimension DataFrame which have all the msisdn & Weekstart
        *df_features: All the Feature DataFrames which needs to be joined into Master Table
                    (JOIN WILL BE DONE ON msisdn & Weekstart)
    """

    conf_catalog = get_config_parameters(config="catalog")

    first_weekstart = get_start_date(partition_column="weekstart")
    last_weekstart = get_end_date(partition_column="weekstart")

    master_catalog = conf_catalog["l6_master"]

    save_args = master_catalog["save_args"]
    save_args["partitionBy"].remove("weekstart")
    if len(save_args["partitionBy"]) == 0:
        save_args.pop("partitionBy", None)

    file_path = get_file_path(filepath=master_catalog["filepath"])
    file_path = get_versioned_save_file_path(file_path)
    file_format = master_catalog["file_format"]
    partitions = int(master_catalog["partitions"])
    log.info(
        "Starting Weekly Master Table Generation for WeekStart {first_weekstart} to {last_weekstart}".format(
            first_weekstart=first_weekstart.strftime("%Y-%m-%d"),
            last_weekstart=last_weekstart.strftime("%Y-%m-%d"),
        )
    )
    log.info(f"File Path: {file_path}")
    log.info(f"File Format: {file_format}")
    log.info(f"Save Args: {save_args}")
    log.info(f"Partitions: {partitions}")

    total_feature_count = len(df_features)
    df_feature_mapped = []

    for i in range(total_feature_count):
        # check features mapping interface
        df_feature_mapped.append(
            check_feature_mapping(df_features[i], features_mapping)
        )

    while first_weekstart <= last_weekstart:
        week_start = last_weekstart.strftime("%Y-%m-%d")

        log.info(
            "Starting Weekly Master Table Generation for WeekStart: {}".format(
                week_start
            )
        )

        df_master = df_master_dimension.filter(f.col("weekstart") == week_start).drop(
            "weekstart"
        )

        for i in range(total_feature_count):
            # check features mapping interface
            df_fea = (
                df_feature_mapped[i]
                .filter(f.col("weekstart") == week_start)
                .drop("weekstart")
            )

            df_master = df_master.join(df_fea, "msisdn", how="left")

        partition_file_path = "{file_path}/weekstart={weekstart}".format(
            file_path=file_path, weekstart=week_start
        )

        if "training" == pipeline:
            df_master = df_master.drop("msisdn")
            df_master = df_master.withColumnRenamed("msisdn_hash", "msisdn")
        df_master.repartition(numPartitions=partitions).write.save(
            partition_file_path, file_format, **save_args
        )
        log.info(
            "Completed Weekly Master Table Generation for WeekStart: {}".format(
                week_start
            )
        )

        last_weekstart -= timedelta(days=7)

    created_at = file_path[-10:]
    df_meta = build_feature_metadata_table(created_at=created_at, *df_feature_mapped)

    return df_meta


def build_outlet_geospatial_master_table(
    feature_mode: str,
    required_output_features: List[str],
    features_mapping: dict,
    df_outlets_characteristics: pyspark.sql.DataFrame,
    df_ext_bps_susenas: pyspark.sql.DataFrame,
    df_ext_bps_podes: pyspark.sql.DataFrame,
    df_ext_bps_sakernas: pyspark.sql.DataFrame,
    *df_features: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Full joins all outlet dataframes
    :param df_outlets_characteristics: Outlet characteristics table (city, area ..)
    :param df_ext_bps_susenas: BPS feature table
    :param df_ext_bps_podes: BPS feature table
    :param df_ext_bps_sakernas: BPS feature table
    :param df_features: All the Feature DataFrames which needs to be joined into Master Table
    :return: df_all: All features dataframes for outlet full joined
    """
    # BPS Features, need to be only joined on admin_1 and province
    df_ext_bps_susenas = df_ext_bps_susenas.withColumnRenamed("province", "province_1")
    df_ext_bps_podes = df_ext_bps_podes.withColumnRenamed("province", "province_2")
    df_ext_bps_sakernas = df_ext_bps_sakernas.withColumnRenamed(
        "province", "province_3"
    )

    df_ext_bps_susenas = add_prefix_to_fea_columns(
        df_ext_bps_susenas, ["province_1"], "fea_outlets_"
    )
    df_ext_bps_podes = add_prefix_to_fea_columns(
        df_ext_bps_podes, ["province_2"], "fea_outlets_"
    )
    df_ext_bps_sakernas = add_prefix_to_fea_columns(
        df_ext_bps_sakernas, ["province_3"], "fea_outlets_"
    )

    # Some outlets will include some features while others will not, eg: some may be close to a primary
    # road, and others may be close to a school. We do a 'full' join to make sure we capture all the outlets
    # and all the features.

    total_feature_count = len(df_features)
    counter = 1
    for i in range(total_feature_count):
        if counter == 1:
            df_all = df_features[i]
        else:
            df_all = df_all.join(df_features[i], ["outlet_id"], how="full")
        counter += 1

    df_all = df_all.join(
        f.broadcast(df_ext_bps_susenas),
        df_all.fea_outlets_admin_1 == df_ext_bps_susenas.province_1,
        how="full",
    )
    df_all = df_all.join(
        f.broadcast(df_ext_bps_podes),
        df_all.fea_outlets_admin_1 == df_ext_bps_podes.province_2,
        how="full",
    )
    df_all = df_all.join(
        f.broadcast(df_ext_bps_sakernas),
        df_all.fea_outlets_admin_1 == df_ext_bps_sakernas.province_3,
        how="full",
    )

    df_outlets_characteristics = (
        df_outlets_characteristics.withColumnRenamed(
            "latitude", "fea_outlets_mode_latitude"
        )
        .withColumnRenamed("longitude", "fea_outlets_mode_longitude")
        .withColumnRenamed("kabupaten", "fea_outlets_kabupaten")
        .withColumnRenamed("outlet_type", "fea_outlets_type")
        .withColumnRenamed("cluster", "fea_outlets_cluster")
    )

    df_all_with_outlet_characteristics = df_all.join(
        df_outlets_characteristics, ["outlet_id"], how="left"
    )
    fea_columns = ["outlet_id"] + [
        col for col in df_all_with_outlet_characteristics.columns if "fea_" in col
    ]

    required_output_columns = get_required_output_columns(
        output_features=fea_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["outlet_id"],
    )
    df_all_with_outlet_characteristics = df_all_with_outlet_characteristics.select(
        required_output_columns
    )

    # check features mapping interface
    df_all_with_outlet_characteristics = check_feature_mapping(
        df_all_with_outlet_characteristics, features_mapping
    )

    month = get_start_date(period="1cm").strftime("%Y-%m-%d")

    df_all_with_outlet_characteristics = df_all_with_outlet_characteristics.withColumn(
        "month", f.lit(month)
    )

    return df_all_with_outlet_characteristics


def build_msisdn_only_master_table(
    pipeline: str,
    features_mapping: dict,
    df_master_dimension: pyspark.sql.DataFrame,
    *df_features: pyspark.sql.DataFrame,
) -> None:
    """
    Final DataFrame created by joining all the Feature DataFrames

    Args:
        pipeline: Pipeline Name
        df_master_dimension: Master Dimension DataFrame which have all the msisdn & Weekstart
        *df_features: All the Feature DataFrames which needs to be joined into Master Table
                    (JOIN WILL BE DONE ONLY ON msisdn)

    Returns:
        df_master: Final DataFrame created by joining all the Feature DataFrames
    """

    total_feature_count = len(df_features)
    df_feature_mapped = []

    for i in range(total_feature_count):
        # check features mapping interface
        df_feature_mapped.append(
            check_feature_mapping(df_features[i], features_mapping)
        )

    df_master = df_master_dimension.select("msisdn").distinct()

    for i in range(total_feature_count):
        df_fea = df_feature_mapped[i]
        df_master = df_master.join(df_fea, "msisdn", how="left")

    if "training" == pipeline:
        df_master = df_master.drop("msisdn")
        df_master = df_master.withColumnRenamed("msisdn_hash", "msisdn")

    return df_master
