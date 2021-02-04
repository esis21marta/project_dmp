import datetime
from unittest import mock

from pyspark.sql.types import (
    ArrayType,
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._00_preprocessing.availability.qa_availability import (
    qa_availability,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestQaAvailability:
    @mock.patch(
        "src.dmp.pipelines.dmp._00_preprocessing.availability.qa_availability.check_domain_availability",
        autospec=True,
    )
    def test_qa_availability(self, mock_check_domain_availability, spark_session):
        raw_tables = {
            "test_domain_1": ["test_catalog_name_1", "test_catalog_name_2"],
        }

        # fmt: off
        mock_check_domain_availability.return_value = spark_session.createDataFrame(
            data=[
                [datetime.date(2019, 1, 1), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 2), 1, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 3), 1, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 4), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 5), 1, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 6), 1, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 7), 1, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 8), 1, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 9), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 10), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 11), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 12), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 13), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 14), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 15), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 16), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 17), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 18), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 19), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 20), 0, "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 1), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 2), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 3), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 4), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 5), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 6), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 7), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 8), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 9), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 10), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 11), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 12), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 13), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 14), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 15), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 16), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 17), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 18), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 19), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21),],
                [datetime.date(2019, 1, 20), 1, "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21),],
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

        result = qa_availability(raw_tables)

        actual_availability_detailed = result["availability_detailed"]
        actual_availability_summary = result["availability_summary"]

        # fmt: off
        expected_availability_detailed = spark_session.createDataFrame(
            data=[
                [datetime.date(2019, 1, 1), "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21), 1],
                [datetime.date(2019, 1, 2), "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 3), "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 4), "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21), 1],
                [datetime.date(2019, 1, 5), "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 6), "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 7), "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 8), "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 9), "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21), 1],
                [datetime.date(2019, 1, 10), "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21), 1],
                [datetime.date(2019, 1, 11), "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21), 1],
                [datetime.date(2019, 1, 12), "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21), 1],
                [datetime.date(2019, 1, 13), "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21), 1],
                [datetime.date(2019, 1, 14), "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21), 1],
                [datetime.date(2019, 1, 15), "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21), 1],
                [datetime.date(2019, 1, 16), "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21), 1],
                [datetime.date(2019, 1, 17), "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21), 1],
                [datetime.date(2019, 1, 18), "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21), 1],
                [datetime.date(2019, 1, 19), "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21), 1],
                [datetime.date(2019, 1, 20), "test_db_a.table_1", "test_domain", datetime.date(2019, 1, 21), 1],
                [datetime.date(2019, 1, 1), "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 2), "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 3), "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 4), "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 5), "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 6), "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 7), "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 8), "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 9), "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 10), "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 11), "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 12), "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 13), "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 14), "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 15), "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 16), "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 17), "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 18), "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 19), "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21), 0],
                [datetime.date(2019, 1, 20), "test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21), 0],
            ],
            schema=StructType(
                [
                    StructField("date", DateType(), False),
                    StructField("table_name", StringType(), False),
                    StructField("domain", StringType(), False),
                    StructField("run_time", DateType(), False),
                    StructField("is_missing", LongType(), False),
                ]
            ),
        )

        expected_availability_summary = spark_session.createDataFrame(
            data=[
                [
                    "test_db_a.table_1",
                    "test_domain",
                    datetime.date(2019, 1, 21),
                    14,
                    [
                        datetime.date(2019, 1, 1),
                        datetime.date(2019, 1, 4),
                        datetime.date(2019, 1, 9),
                        datetime.date(2019, 1, 10),
                        datetime.date(2019, 1, 11),
                        datetime.date(2019, 1, 12),
                        datetime.date(2019, 1, 13),
                        datetime.date(2019, 1, 14),
                        datetime.date(2019, 1, 15),
                        datetime.date(2019, 1, 16),
                        datetime.date(2019, 1, 17),
                        datetime.date(2019, 1, 18),
                        datetime.date(2019, 1, 19),
                        datetime.date(2019, 1, 20),
                    ],
                ],
                ["test_db_b.table_2", "test_domain", datetime.date(2019, 1, 21), 0, None,],
            ],
            schema=StructType(
                [
                    StructField("table_name", StringType(), False),
                    StructField("domain", StringType(), False),
                    StructField("run_time", DateType(), False),
                    StructField("number_of_missing_days", LongType(), False),
                    StructField("missing_days", ArrayType(DateType(), False), True),
                ]
            ),
        )
        # fmt: on

        assert_df_frame_equal(
            actual_availability_summary, expected_availability_summary
        )
        assert_df_frame_equal(
            actual_availability_detailed, expected_availability_detailed
        )
