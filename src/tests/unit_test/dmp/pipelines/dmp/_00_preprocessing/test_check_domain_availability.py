import datetime
from unittest import mock

from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType

from src.dmp.pipelines.dmp._00_preprocessing.availability.check_domain_availability import (
    add_metadata,
    check_domain_availability,
    check_table_availability,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestDomainAvailability:
    full_table_name = "test_db.table_1"
    start_date = datetime.datetime(2019, 1, 1).date()
    end_date = datetime.datetime(2019, 1, 10).date()
    partition_column = "event_date"
    partition_date_format = "%Y%m%d"

    def test_check_table_availability_with_partitioned_table(self, spark_session):

        spark_session.sql("DROP DATABASE IF EXISTS test_db CASCADE")
        spark_session.sql("CREATE DATABASE test_db")
        spark_session.sql("USE test_db")
        spark_session.sql(
            "CREATE TABLE table_1 (col_a STRING, col_b LONG) PARTITIONED BY (event_date STRING, load_date STRING)"
        )

        df = spark_session.createDataFrame(
            data=[
                ["20190102", "20190103", "abc", 2],
                ["20190103", "20190103", "xyz", 5],
                ["20190105", "20190103", "123", 3],
                ["20190106", "20190104", "dem", 6],
                ["20190107", "20190104", "oea", 8],
                ["20190108", "20190106", "qsa", 9],
            ],
            schema=StructType(
                [
                    StructField("event_date", StringType(), True),
                    StructField("load_date", StringType(), True),
                    StructField("col_a", StringType(), True),
                    StructField("col_b", LongType(), True),
                ]
            ),
        )

        df.write.partitionBy(["event_date", "load_date"]).mode("overwrite").saveAsTable(
            "test_db.table_1"
        )

        actual_availability_status_df = check_table_availability(
            self.full_table_name,
            self.start_date,
            self.end_date,
            self.partition_column,
            self.partition_date_format,
            spark_session,
        )

        expected_availability_status_df = spark_session.createDataFrame(
            data=[
                (datetime.date(2019, 1, 1), 0),
                (datetime.date(2019, 1, 2), 1),
                (datetime.date(2019, 1, 3), 1),
                (datetime.date(2019, 1, 4), 0),
                (datetime.date(2019, 1, 5), 1),
                (datetime.date(2019, 1, 6), 1),
                (datetime.date(2019, 1, 7), 1),
                (datetime.date(2019, 1, 8), 1),
                (datetime.date(2019, 1, 9), 0),
                (datetime.date(2019, 1, 10), 0),
            ],
            schema=StructType(
                [
                    StructField("date", DateType(), True),
                    StructField("is_available", LongType(), True),
                ]
            ),
        )

        assert_df_frame_equal(
            actual_availability_status_df, expected_availability_status_df
        )

        spark_session.sql("DROP DATABASE IF EXISTS test_db CASCADE")

    def test_check_data_availability_without_partitioned_by_ref_column(
        self, spark_session
    ):

        spark_session.sql("DROP DATABASE IF EXISTS test_db CASCADE")
        spark_session.sql("CREATE DATABASE test_db")
        spark_session.sql("USE test_db")
        spark_session.sql(
            "CREATE TABLE table_1 (event_date STRING, col_a STRING, col_b LONG) PARTITIONED BY (load_date STRING)"
        )

        df = spark_session.createDataFrame(
            data=[
                ["20190102", "20190103", "abc", 2],
                ["20190103", "20190103", "xyz", 5],
                ["20190105", "20190103", "123", 3],
                ["20190106", "20190104", "dem", 6],
                ["20190107", "20190104", "oea", 8],
                ["20190108", "20190106", "qsa", 9],
            ],
            schema=StructType(
                [
                    StructField("event_date", StringType(), True),
                    StructField("load_date", StringType(), True),
                    StructField("col_a", StringType(), True),
                    StructField("col_b", LongType(), True),
                ]
            ),
        )

        df.write.partitionBy(["event_date", "load_date"]).mode("overwrite").saveAsTable(
            "test_db.table_1"
        )

        actual_availability_status_df = check_table_availability(
            self.full_table_name,
            self.start_date,
            self.end_date,
            self.partition_column,
            self.partition_date_format,
            spark_session,
        )

        expected_availability_status_df = spark_session.createDataFrame(
            data=[
                (datetime.date(2019, 1, 1), 0),
                (datetime.date(2019, 1, 2), 1),
                (datetime.date(2019, 1, 3), 1),
                (datetime.date(2019, 1, 4), 0),
                (datetime.date(2019, 1, 5), 1),
                (datetime.date(2019, 1, 6), 1),
                (datetime.date(2019, 1, 7), 1),
                (datetime.date(2019, 1, 8), 1),
                (datetime.date(2019, 1, 9), 0),
                (datetime.date(2019, 1, 10), 0),
            ],
            schema=StructType(
                [
                    StructField("date", DateType(), True),
                    StructField("is_available", LongType(), True),
                ]
            ),
        )

        assert_df_frame_equal(
            actual_availability_status_df, expected_availability_status_df
        )

        spark_session.sql("DROP DATABASE IF EXISTS test_db CASCADE")

    def test_check_data_availability_with_partitioned_table_with_hive_default_partition(
        self, spark_session
    ):
        spark_session.sql("DROP DATABASE IF EXISTS test_db CASCADE")
        spark_session.sql("CREATE DATABASE test_db")
        spark_session.sql("USE test_db")
        spark_session.sql(
            "CREATE TABLE table_1 (col_a STRING, col_b LONG) PARTITIONED BY (event_date STRING, load_date STRING)"
        )

        df = spark_session.createDataFrame(
            data=[
                ["20190102", "20190103", "abc", 2],
                ["20190103", "20190103", "xyz", 5],
                ["20190105", "20190103", "123", 3],
                ["20190106", "20190104", "dem", 6],
                ["20190107", "20190104", "oea", 8],
                ["20190108", "20190106", "qsa", 9],
                [None, "20190107", "qsa", 9],
                [None, "20190108", "qsa", 9],
            ],
            schema=StructType(
                [
                    StructField("event_date", StringType(), True),
                    StructField("load_date", StringType(), True),
                    StructField("col_a", StringType(), True),
                    StructField("col_b", LongType(), True),
                ]
            ),
        )

        df.write.partitionBy(["event_date", "load_date"]).mode("overwrite").saveAsTable(
            "test_db.table_1"
        )

        actual_availability_status_df = check_table_availability(
            self.full_table_name,
            self.start_date,
            self.end_date,
            self.partition_column,
            self.partition_date_format,
            spark_session,
        )

        expected_availability_status_df = spark_session.createDataFrame(
            data=[
                (datetime.date(2019, 1, 1), 0),
                (datetime.date(2019, 1, 2), 1),
                (datetime.date(2019, 1, 3), 1),
                (datetime.date(2019, 1, 4), 0),
                (datetime.date(2019, 1, 5), 1),
                (datetime.date(2019, 1, 6), 1),
                (datetime.date(2019, 1, 7), 1),
                (datetime.date(2019, 1, 8), 1),
                (datetime.date(2019, 1, 9), 0),
                (datetime.date(2019, 1, 10), 0),
            ],
            schema=StructType(
                [
                    StructField("date", DateType(), True),
                    StructField("is_available", LongType(), True),
                ]
            ),
        )

        assert_df_frame_equal(
            actual_availability_status_df, expected_availability_status_df
        )

        spark_session.sql("DROP DATABASE IF EXISTS test_db CASCADE")


class TestAddMetadata:
    def test_add_metadata(self, spark_session):
        availability_status_df = spark_session.createDataFrame(
            data=[
                (datetime.date(2019, 1, 1), 0),
                (datetime.date(2019, 1, 2), 1),
                (datetime.date(2019, 1, 3), 1),
                (datetime.date(2019, 1, 4), 0),
                (datetime.date(2019, 1, 5), 1),
                (datetime.date(2019, 1, 6), 1),
                (datetime.date(2019, 1, 7), 1),
                (datetime.date(2019, 1, 8), 1),
                (datetime.date(2019, 1, 9), 0),
                (datetime.date(2019, 1, 10), 0),
            ],
            schema=StructType(
                [
                    StructField("date", DateType(), False),
                    StructField("is_available", LongType(), False),
                ]
            ),
        )

        actual_availability_status_df = add_metadata(
            df=availability_status_df,
            full_table_name="test_db.table_1",
            domain_name="voice",
        )

        # fmt: off
        expected_availability_status_df = spark_session.createDataFrame(
            data=[
                [datetime.date(2019, 1, 1), 0, "test_db.table_1", "voice", datetime.date(2019, 1, 11)],
                [datetime.date(2019, 1, 2), 1, "test_db.table_1", "voice", datetime.date(2019, 1, 11)],
                [datetime.date(2019, 1, 3), 1, "test_db.table_1", "voice", datetime.date(2019, 1, 11)],
                [datetime.date(2019, 1, 4), 0, "test_db.table_1", "voice", datetime.date(2019, 1, 11)],
                [datetime.date(2019, 1, 5), 1, "test_db.table_1", "voice", datetime.date(2019, 1, 11)],
                [datetime.date(2019, 1, 6), 1, "test_db.table_1", "voice", datetime.date(2019, 1, 11)],
                [datetime.date(2019, 1, 7), 1, "test_db.table_1", "voice", datetime.date(2019, 1, 11)],
                [datetime.date(2019, 1, 8), 1, "test_db.table_1", "voice", datetime.date(2019, 1, 11)],
                [datetime.date(2019, 1, 9), 0, "test_db.table_1", "voice", datetime.date(2019, 1, 11)],
                [datetime.date(2019, 1, 10), 0, "test_db.table_1", "voice", datetime.date(2019, 1, 11)],
            ],
            schema=StructType(
                [
                    StructField("date", DateType(), False),
                    StructField("is_available", LongType(), False),
                    StructField("table_name", StringType(), False),
                    StructField("domain", StringType(), False),
                    StructField("run_time", DateType(), False),
                ]
            ),
        )
        # fmt: on

        assert_df_frame_equal(
            actual_availability_status_df, expected_availability_status_df
        )


class TestCheckDomainAvailability:
    @mock.patch(
        "src.dmp.pipelines.dmp._00_preprocessing.availability.check_domain_availability.get_end_date",
        return_value=datetime.date(2019, 1, 20),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._00_preprocessing.availability.check_domain_availability.get_config_parameters",
        return_value={
            "test_catalog_name_1": {
                "load_args": {
                    "partition_column": "event_date",
                    "partition_date_format": "%Y%m%d",
                    "start_date": "2019-01-01",
                },
                "sla_interval": {"start_date": "2019-01-01"},
                "database": "test_db_a",
                "table": "table_1",
            },
            "test_catalog_name_2": {
                "sla_interval": {
                    "column_reference": "event_date",
                    "date_format": "%Y-%m-%d",
                    "start_date": "2019-01-01",
                },
                "database": "test_db_b",
                "table": "table_2",
            },
        },
        autospec=True,
    )
    def test_check_domain_availability(
        self, mock_get_config_parameters, mock_get_end_date, spark_session
    ):
        domain_name = "test_domain"
        catalog_names = ["test_catalog_name_1", "test_catalog_name_2"]

        spark_session.sql("DROP DATABASE IF EXISTS test_db_a CASCADE")
        spark_session.sql("CREATE DATABASE test_db_a")
        spark_session.sql("USE test_db_a")
        spark_session.sql(
            "CREATE TABLE table_1 (col_a STRING, col_b LONG) PARTITIONED BY (event_date STRING, load_date STRING)"
        )

        df = spark_session.createDataFrame(
            data=[
                ["20190102", "20190103", "abc", 2],
                ["20190103", "20190103", "xyz", 5],
                ["20190105", "20190103", "123", 3],
                ["20190106", "20190104", "dem", 6],
                ["20190107", "20190104", "oea", 8],
                ["20190108", "20190106", "qsa", 9],
            ],
            schema=StructType(
                [
                    StructField("event_date", StringType(), True),
                    StructField("load_date", StringType(), True),
                    StructField("col_a", StringType(), True),
                    StructField("col_b", LongType(), True),
                ]
            ),
        )

        df.write.partitionBy(["event_date", "load_date"]).mode("overwrite").saveAsTable(
            "test_db_a.table_1"
        )

        spark_session.sql("DROP DATABASE IF EXISTS test_db_b CASCADE")
        spark_session.sql("CREATE DATABASE test_db_b")
        spark_session.sql("USE test_db_b")
        spark_session.sql(
            "CREATE TABLE table_2 (col_a STRING, col_b LONG) PARTITIONED BY (event_date STRING, load_date STRING)"
        )

        df = spark_session.createDataFrame(
            data=[
                ["2019-01-01", "2019-01-03", "abc", 2],
                ["2019-01-02", "2019-01-03", "xyz", 5],
                ["2019-01-03", "2019-01-03", "123", 3],
                ["2019-01-04", "2019-01-04", "dem", 6],
                ["2019-01-05", "2019-01-04", "oea", 8],
                ["2019-01-06", "2019-01-06", "qsa", 9],
                ["2019-01-07", "2019-01-03", "abc", 2],
                ["2019-01-08", "2019-01-03", "xyz", 5],
                ["2019-01-09", "2019-01-03", "123", 3],
                ["2019-01-10", "2019-01-04", "dem", 6],
                ["2019-01-11", "2019-01-04", "oea", 8],
                ["2019-01-12", "2019-01-06", "qsa", 9],
                ["2019-01-13", "2019-01-03", "abc", 2],
                ["2019-01-14", "2019-01-03", "xyz", 5],
                ["2019-01-15", "2019-01-03", "123", 3],
                ["2019-01-16", "2019-01-04", "dem", 6],
                ["2019-01-17", "2019-01-04", "oea", 8],
                ["2019-01-18", "2019-01-06", "qsa", 9],
                ["2019-01-19", "2019-01-06", "qsa", 9],
                ["2019-01-20", "2019-01-06", "qsa", 9],
            ],
            schema=StructType(
                [
                    StructField("event_date", StringType(), True),
                    StructField("load_date", StringType(), True),
                    StructField("col_a", StringType(), True),
                    StructField("col_b", LongType(), True),
                ]
            ),
        )

        df.write.partitionBy(["event_date", "load_date"]).mode("overwrite").saveAsTable(
            "test_db_b.table_2"
        )

        actual_availability_status_df_combined = check_domain_availability(
            domain_name, catalog_names
        )

        # fmt: off
        expected_availability_status_df_combined = spark_session.createDataFrame(
            data=[
                [datetime.date(2019, 1, 1), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 2), 1, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 3), 1, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 4), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 5), 1, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 6), 1, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 7), 1, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 8), 1, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 9), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 10), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 11), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 12), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 13), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 14), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 15), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 16), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 17), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 18), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 19), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 20), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 1), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 2), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 3), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 4), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 5), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 6), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 7), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 8), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 9), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 10), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 11), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 12), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 13), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 14), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 15), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 16), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 17), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 18), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 19), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21)],
                [datetime.date(2019, 1, 20), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21)],
            ],
            schema=StructType(
                [
                    StructField("date", DateType(), False),
                    StructField("is_available", LongType(), False),
                    StructField("table_name", StringType(), False),
                    StructField("domain", StringType(), False),
                    StructField("run_time", DateType(), False),
                ]
            ),
        )
        # fmt: on

        assert_df_frame_equal(
            actual_availability_status_df_combined,
            expected_availability_status_df_combined,
        )

        spark_session.sql("DROP DATABASE IF EXISTS test_db_a CASCADE")
        spark_session.sql("DROP DATABASE IF EXISTS test_db_b CASCADE")
