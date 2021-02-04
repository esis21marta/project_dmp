import datetime
from unittest import mock

import pandas as pd
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.dmp.pipelines.dmp._06_qa.timeliness.timeliness import check_timeliness_wrapper
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestTimeliness:
    @mock.patch(
        "src.dmp.pipelines.dmp._06_qa.timeliness.timeliness.get_config_parameters",
        return_value={
            "l6_master": {
                "create_versions": True,
                "sla_interval": {
                    "date": 8,
                    "period": "weekly",
                    "date_partitioned_column": "weekstart",
                    "date_partitioned_column_format": "%Y-%m-%d",
                },
            }
        },
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._06_qa.timeliness.timeliness.get_catalog_file_path",
        return_value="hdfs:///master/file/path.parquet",
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._06_qa.timeliness.timeliness.get_ls_output_for_filepath",
        return_value=pd.DataFrame(
            [
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-08-31 10:40 hdfs:///master/file/path.parquet/created_at=2020-08-31",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-08-31 10:42 hdfs:///master/file/path.parquet/created_at=2020-08-31/weekstart=2020-08-17",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-08-31 10:45 hdfs:///master/file/path.parquet/created_at=2020-08-31/weekstart=2020-08-17/part1",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-08-31 10:43 hdfs:///master/file/path.parquet/created_at=2020-08-31/weekstart=2020-08-17/part2",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-08-31 10:42 hdfs:///master/file/path.parquet/created_at=2020-08-31/weekstart=2020-08-24",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-08-31 10:44 hdfs:///master/file/path.parquet/created_at=2020-08-31/weekstart=2020-08-24/part1",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-08-31 10:43 hdfs:///master/file/path.parquet/created_at=2020-08-31/weekstart=2020-08-24/part2",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-09-08 10:40 hdfs:///master/file/path.parquet/created_at=2020-09-08",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-09-09 10:42 hdfs:///master/file/path.parquet/created_at=2020-09-08/weekstart=2020-08-17",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-09-08 10:45 hdfs:///master/file/path.parquet/created_at=2020-09-08/weekstart=2020-08-17/part1",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-09-08 10:43 hdfs:///master/file/path.parquet/created_at=2020-09-08/weekstart=2020-08-17/part2",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-09-08 10:42 hdfs:///master/file/path.parquet/created_at=2020-09-08/weekstart=2020-08-24",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-09-08 10:44 hdfs:///master/file/path.parquet/created_at=2020-09-08/weekstart=2020-08-24/part1",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-09-08 10:43 hdfs:///master/file/path.parquet/created_at=2020-09-08/weekstart=2020-08-24/part2",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-09-08 10:42 hdfs:///master/file/path.parquet/created_at=2020-09-08/weekstart=2020-08-31",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-09-08 10:44 hdfs:///master/file/path.parquet/created_at=2020-09-08/weekstart=2020-08-31/part1",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-09-08 10:43 hdfs:///master/file/path.parquet/created_at=2020-09-08/weekstart=2020-08-31/part2",
            ],
            columns=["ls_output"],
        ),
        autospec=True,
    )
    def test_check_check_timeliness_with_versioned_master_table(
        self,
        mock_get_ls_output_for_filepath,
        mock_get_catalog_file_path,
        mock_get_config_parameters,
        spark_session,
    ):

        actual_timeliness_df = check_timeliness_wrapper("master", "l6_master")(
            master_mode="training",
        )

        expected_timeliness_df = spark_session.createDataFrame(
            data=[
                [
                    "master",  # table_name
                    datetime.datetime(2020, 8, 24),  # run_time
                    datetime.datetime(2020, 8, 31, 10, 45),  # actual
                    datetime.datetime(2020, 9, 1, 0, 0),  # expected
                    0.0,  # delay
                    "master",  # layer
                    "training",  # master_mode
                ],
                [
                    "master",  # table_name
                    datetime.datetime(2020, 8, 31),  # run_time
                    datetime.datetime(2020, 9, 8, 10, 45),  # actual
                    datetime.datetime(2020, 9, 8, 0, 0),  # expected
                    10.75,  # delay
                    "master",  # layer
                    "training",  # master_mode
                ],
            ],
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

        assert_df_frame_equal(
            actual_timeliness_df,
            expected_timeliness_df,
            order_by=actual_timeliness_df.columns,
        )

    @mock.patch(
        "src.dmp.pipelines.dmp._06_qa.timeliness.timeliness.get_config_parameters",
        return_value={
            "l6_master": {
                "create_versions": True,
                "sla_interval": {
                    "date": 8,
                    "period": "weekly",
                    "date_partitioned_column": "weekstart",
                    "date_partitioned_column_format": "%Y-%m-%d",
                },
            }
        },
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._06_qa.timeliness.timeliness.get_catalog_file_path",
        return_value="hdfs:///master/file/path.parquet",
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._06_qa.timeliness.timeliness.get_ls_output_for_filepath",
        return_value=pd.DataFrame(
            [
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-09-09 10:42 hdfs:///master/file/path.parquet/weekstart=2020-08-17",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-09-08 10:45 hdfs:///master/file/path.parquet/weekstart=2020-08-17/part1",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-09-08 10:43 hdfs:///master/file/path.parquet/weekstart=2020-08-17/part2",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-09-08 10:42 hdfs:///master/file/path.parquet/weekstart=2020-08-24",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-09-08 10:46 hdfs:///master/file/path.parquet/weekstart=2020-08-24/part1",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-09-08 10:43 hdfs:///master/file/path.parquet/weekstart=2020-08-24/part2",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-09-08 10:42 hdfs:///master/file/path.parquet/weekstart=2020-08-31",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-09-08 10:44 hdfs:///master/file/path.parquet/weekstart=2020-08-31/part1",
                "drwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-09-08 10:43 hdfs:///master/file/path.parquet/weekstart=2020-08-31/part2",
            ],
            columns=["ls_output"],
        ),
        autospec=True,
    )
    def test_check_check_timeliness_with_non_versioned_master_table(
        self,
        mock_get_ls_output_for_filepath,
        mock_get_catalog_file_path,
        mock_get_config_parameters,
        spark_session,
    ):

        actual_timeliness_df = check_timeliness_wrapper("master", "l6_master")(
            master_mode="training",
        )

        expected_timeliness_df = spark_session.createDataFrame(
            data=[
                [
                    "master",  # table_name
                    datetime.datetime(2020, 8, 31),  # run_time
                    datetime.datetime(2020, 9, 8, 10, 46),  # actual
                    datetime.datetime(2020, 9, 8, 0, 0),  # expected
                    10.767,  # delay
                    "master",  # layer
                    "training",  # master_mode
                ],
            ],
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

        assert_df_frame_equal(
            actual_timeliness_df,
            expected_timeliness_df,
            order_by=actual_timeliness_df.columns,
        )


# TODO: Add test for reseller master table
# TODO: Add test for geospacial master table
