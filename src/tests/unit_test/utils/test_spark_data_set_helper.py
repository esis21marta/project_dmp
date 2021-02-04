from datetime import datetime
from unittest import mock

import pytest
import pytz
from freezegun import freeze_time
from kedro.io import DataSetError

from utils.spark_data_set_helper import get_latest_version, get_versioned_save_file_path


class TestGetVersionedSaveFilePath:
    @freeze_time("2020-10-10 12:00:01")
    def test_get_versioned_save_file_path(self, spark_session):
        actual_versioned_file_path = get_versioned_save_file_path(
            filepath="/test/file/path"
        )

        expected_versioned_file_path = "/test/file/path/created_at=2020-10-10"

        assert actual_versioned_file_path == expected_versioned_file_path

    @freeze_time("2020-10-09 20:00:00", tz_offset=0)
    def test_get_versioned_save_file_path_different_timezone_date(self, spark_session):
        actual_versioned_file_path = get_versioned_save_file_path(
            filepath="/test/file/path"
        )

        expected_versioned_file_path = "/test/file/path/created_at=2020-10-10"

        assert actual_versioned_file_path == expected_versioned_file_path

    @freeze_time("2020-10-10 12:00:01")
    def test_get_versioned_save_file_path_with_user_provided_version(
        self, spark_session
    ):
        actual_versioned_file_path = get_versioned_save_file_path(
            filepath="/test/file/path",
            version=datetime(
                2020, 10, 23, 10, 23, 17, 1000, tzinfo=pytz.timezone("Asia/Jakarta")
            ),
        )

        expected_versioned_file_path = "/test/file/path/created_at=2020-10-23"

        assert actual_versioned_file_path == expected_versioned_file_path

    @freeze_time("2020-10-09 20:00:00", tz_offset=0)
    def test_get_versioned_save_file_path_different_timezone_date_with_user_provided_version(
        self, spark_session
    ):
        actual_versioned_file_path = get_versioned_save_file_path(
            filepath="/test/file/path",
            version=datetime(
                2020, 10, 23, 10, 23, 17, 1000, tzinfo=pytz.timezone("Asia/Jakarta")
            ),
        )

        expected_versioned_file_path = "/test/file/path/created_at=2020-10-23"

        assert actual_versioned_file_path == expected_versioned_file_path


@mock.patch("subprocess.Popen")
class TestGetLatestVersion:
    def test_get_latest_version(self, mock_subprocess_popen, spark_session):
        process_mock = mock.Mock()
        attrs = {
            "communicate.return_value": (
                b"Found 5 items\ndrwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-07-28 12:29 hdfs:///data/landing/dmp_remote/users/rahul/mck_dmp_training/05_model_input/master.parquet/created_at=2020-07-31\ndrwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-07-28 11:55 hdfs:///data/landing/dmp_remote/users/rahul/mck_dmp_training/05_model_input/master.parquet/created_at=2020-07-30\ndrwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-07-28 11:35 hdfs:///data/landing/dmp_remote/users/rahul/mck_dmp_training/05_model_input/master.parquet/created_at=2020-07-29\ndrwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-07-28 11:06 hdfs:///data/landing/dmp_remote/users/rahul/mck_dmp_training/05_model_input/master.parquet/created_at=2020-07-28\ndrwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-07-28 10:41 hdfs:///data/landing/dmp_remote/users/rahul/mck_dmp_training/05_model_input/master.parquet/created_at=2020-07-27\n",
                b"",
            )
        }
        process_mock.configure_mock(**attrs)
        mock_subprocess_popen.return_value = process_mock

        actual_version = get_latest_version("/test/file/path.parquet")
        expected_version = "2020-07-31"

        assert actual_version == expected_version

    def test_get_latest_version_with_version_index_0(
        self, mock_subprocess_popen, spark_session
    ):
        process_mock = mock.Mock()
        attrs = {
            "communicate.return_value": (
                b"Found 5 items\ndrwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-07-28 12:29 hdfs:///data/landing/dmp_remote/users/rahul/mck_dmp_training/05_model_input/master.parquet/created_at=2020-07-31\ndrwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-07-28 11:55 hdfs:///data/landing/dmp_remote/users/rahul/mck_dmp_training/05_model_input/master.parquet/created_at=2020-07-30\ndrwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-07-28 11:35 hdfs:///data/landing/dmp_remote/users/rahul/mck_dmp_training/05_model_input/master.parquet/created_at=2020-07-29\ndrwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-07-28 11:06 hdfs:///data/landing/dmp_remote/users/rahul/mck_dmp_training/05_model_input/master.parquet/created_at=2020-07-28\ndrwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-07-28 10:41 hdfs:///data/landing/dmp_remote/users/rahul/mck_dmp_training/05_model_input/master.parquet/created_at=2020-07-27\n",
                b"",
            )
        }
        process_mock.configure_mock(**attrs)
        mock_subprocess_popen.return_value = process_mock

        actual_version = get_latest_version("/test/file/path.parquet", version_index=0)
        expected_version = "2020-07-31"

        assert actual_version == expected_version

    def test_get_latest_version_with_version_index_1(
        self, mock_subprocess_popen, spark_session
    ):
        process_mock = mock.Mock()
        attrs = {
            "communicate.return_value": (
                b"Found 5 items\ndrwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-07-28 12:29 hdfs:///data/landing/dmp_remote/users/rahul/mck_dmp_training/05_model_input/master.parquet/created_at=2020-07-31\ndrwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-07-28 11:55 hdfs:///data/landing/dmp_remote/users/rahul/mck_dmp_training/05_model_input/master.parquet/created_at=2020-07-30\ndrwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-07-28 11:35 hdfs:///data/landing/dmp_remote/users/rahul/mck_dmp_training/05_model_input/master.parquet/created_at=2020-07-29\ndrwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-07-28 11:06 hdfs:///data/landing/dmp_remote/users/rahul/mck_dmp_training/05_model_input/master.parquet/created_at=2020-07-28\ndrwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-07-28 10:41 hdfs:///data/landing/dmp_remote/users/rahul/mck_dmp_training/05_model_input/master.parquet/created_at=2020-07-27\n",
                b"",
            )
        }
        process_mock.configure_mock(**attrs)
        mock_subprocess_popen.return_value = process_mock

        actual_version = get_latest_version("/test/file/path.parquet", version_index=1)
        expected_version = "2020-07-30"

        assert actual_version == expected_version

    def test_get_latest_version_with_version_index_more_than_valid_version(
        self, mock_subprocess_popen, spark_session
    ):
        process_mock = mock.Mock()
        attrs = {
            "communicate.return_value": (
                b"Found 5 items\ndrwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-07-28 12:29 hdfs:///data/landing/dmp_remote/users/rahul/mck_dmp_training/05_model_input/master.parquet/created_at=2020-07-31\ndrwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-07-28 11:55 hdfs:///data/landing/dmp_remote/users/rahul/mck_dmp_training/05_model_input/master.parquet/created_at=2020-07-30\ndrwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-07-28 11:35 hdfs:///data/landing/dmp_remote/users/rahul/mck_dmp_training/05_model_input/master.parquet/created_at=2020-07-29\ndrwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-07-28 11:06 hdfs:///data/landing/dmp_remote/users/rahul/mck_dmp_training/05_model_input/master.parquet/created_at=2020-07-28\ndrwxr-xr-x+  - 20335985 gx_pnt_remote          0 2020-07-28 10:41 hdfs:///data/landing/dmp_remote/users/rahul/mck_dmp_training/05_model_input/master.parquet/created_at=2020-07-27\n",
                b"",
            )
        }
        process_mock.configure_mock(**attrs)
        mock_subprocess_popen.return_value = process_mock

        with pytest.raises(DataSetError) as e_info:
            get_latest_version("/test/file/path.parquet", version_index=7)
            assert "No version found" in str(e_info.value)


# @mock.patch('utils.spark_data_set_helper.get_latest_version', return_value="")
# class TestGetVersionedLoadFilePath():
#     def test_get_versioned_load_file_path_with_version(self, mock_get_latest_version, spark_session):
#         actual_versioned_load_file_path = get_versioned_load_file_path(filepath="/test/file/path.parquet", version="2020-10-10")
#         expected_versioned_load_file_path = "/test/file/path.parquet/created_at=2020-10-10"

#         assert actual_versioned_load_file_path == expected_versioned_load_file_path

#     def test_get_versioned_load_file_path_with_version_index_positive_integer(self, mock_get_latest_version, spark_session):
#         actual_versioned_load_file_path = get_versioned_load_file_path(filepath="/test/file/path.parquet", version_index=1)

#         expected_versioned_load_file_path = "/test/file/path.parquet/created_at=2020-10-10"

#         assert actual_versioned_load_file_path == expected_versioned_load_file_path
