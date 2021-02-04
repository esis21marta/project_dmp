from unittest import mock

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._02_primary.msisdn_list.check_new_msisdn_count import (
    check_new_msisdn_count,
    create_html,
    get_msisdn_count_by_usecase,
    get_msisdn_count_to_move_to_remote,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestCheckNewMSISDNCount:
    def test_get_msisdn_count_by_usecases(self, spark_session):
        df = spark_session.createDataFrame(
            data=[
                ["111", ["usecase_a", "usecase_b"]],
                ["222", ["usecase_b"]],
                ["333", ["usecase_c"]],
                ["444", ["usecase_a", "usecase_b", "usecase_f"]],
                ["555", ["usecase_a", "usecase_b"]],
                ["666", ["usecase_c", "usecase_b"]],
                ["777", ["usecase_c", "usecase_b"]],
                ["888", ["usecase_d", "usecase_b"]],
                ["999", ["usecase_e", "usecase_b"]],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("use_case", ArrayType(StringType(), False), False),
                ]
            ),
        )

        actual_msisdn_count = get_msisdn_count_by_usecase(df)

        expected_msisdn_count = spark_session.createDataFrame(
            data=[
                ["usecase_a", 3],
                ["usecase_b", 8],
                ["usecase_c", 3],
                ["usecase_f", 1],
                ["usecase_d", 1],
                ["usecase_e", 1],
            ],
            schema=StructType(
                [
                    StructField("use_case", StringType(), False),
                    StructField("number_of_msisdn", LongType(), False),
                ]
            ),
        )
        assert_df_frame_equal(actual_msisdn_count, expected_msisdn_count)

    def test_get_msisdn_count_to_move_to_remote(self, spark_session):
        df = spark_session.createDataFrame(
            data=[
                ["111", ["usecase_a", "usecase_b"]],
                ["222", ["usecase_b"]],
                ["333", ["usecase_c"]],
                ["444", ["usecase_a", "usecase_b", "usecase_f"]],
                ["555", ["usecase_a", "usecase_b"]],
                ["666", ["usecase_c", "usecase_b"]],
                ["777", ["usecase_c", "usecase_b"]],
                ["888", ["usecase_d", "usecase_b"]],
                ["999", ["usecase_e", "usecase_b"]],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("use_case", ArrayType(StringType(), False), False),
                ]
            ),
        )

        usecase_to_move_to_remote = ["usecase_a", "usecase_f", "usecase_d"]

        actual_msisdn_count_to_move_to_remote = get_msisdn_count_to_move_to_remote(
            df, usecase_to_move_to_remote
        )

        assert actual_msisdn_count_to_move_to_remote == 4

    @mock.patch(
        "src.dmp.pipelines.dmp._02_primary.msisdn_list.check_new_msisdn_count.send_notification_to_telegram"
    )
    def test_check_new_msisdn_count_with_new_msisdn(
        self, mock_send_notification_to_telegram, spark_session
    ):
        df_partner_msisdn_old = spark_session.createDataFrame(
            data=[
                ["222", ["usecase_b"]],
                ["333", ["usecase_c"]],
                ["444", ["usecase_a", "usecase_b", "usecase_f"]],
                ["555", ["usecase_a", "usecase_b"]],
                ["666", ["usecase_c", "usecase_b"]],
                ["777", ["usecase_c", "usecase_b"]],
                ["888", ["usecase_d", "usecase_b"]],
                ["999", ["usecase_e", "usecase_b"]],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("use_case", ArrayType(StringType(), False), False),
                ]
            ),
        )

        df_partner_msisdn = spark_session.createDataFrame(
            data=[
                ["111", ["usecase_a", "usecase_b"]],
                ["222", ["usecase_b"]],
                ["333", ["usecase_c"]],
                ["444", ["usecase_a", "usecase_b", "usecase_f"]],
                ["555", ["usecase_a", "usecase_b"]],
                ["666", ["usecase_c", "usecase_b"]],
                ["777", ["usecase_c", "usecase_b"]],
                ["888", ["usecase_d", "usecase_b"]],
                ["999", ["usecase_e", "usecase_b"]],
                ["000", ["usecase_a", "usecase_c"]],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("use_case", ArrayType(StringType(), False), False),
                ]
            ),
        )

        files_to_pack = {
            "usecase_a": {"move_to_remote": True},
            "usecase_b": {"move_to_remote": False},
            "usecase_c": {"move_to_remote": False},
            "usecase_d": {"move_to_remote": False},
            "usecase_e": {"move_to_remote": True},
            "usecase_f": {"move_to_remote": True},
        }

        actual_msisdn_count_to_move_to_remote = check_new_msisdn_count(
            df_partner_msisdn, df_partner_msisdn_old, files_to_pack, {}
        )

        # msisdn_count_by_usecase = spark_session.createDataFrame(
        #     data = [
        #         ['usecase_a', 4, 2],
        #         ['usecase_b', 8, 1],
        #         ['usecase_c', 4, 1],
        #         ['usecase_d', 1, 0],
        #         ['usecase_e', 1, 0],
        #         ['usecase_f', 1, 0],
        #     ],
        #     schema=StructType(
        #         [
        #             StructField("use_case", StringType(), False),
        #             StructField("number_of_msisdn", LongType(), True),
        #             StructField("number_of_new_msisdn", LongType(), True)
        #         ]
        #     )
        # )
        assert actual_msisdn_count_to_move_to_remote == 5
        assert mock_send_notification_to_telegram.call_count == 1

    @mock.patch(
        "src.dmp.pipelines.dmp._02_primary.msisdn_list.check_new_msisdn_count.send_notification_to_telegram"
    )
    def test_check_new_msisdn_count_with_no_new_msisdn(
        self, mock_send_notification_to_telegram, spark_session
    ):
        df_partner_msisdn_old = spark_session.createDataFrame(
            data=[
                ["222", ["usecase_b"]],
                ["333", ["usecase_c"]],
                ["444", ["usecase_a", "usecase_b", "usecase_f"]],
                ["555", ["usecase_a", "usecase_b"]],
                ["666", ["usecase_c", "usecase_b"]],
                ["777", ["usecase_c", "usecase_b"]],
                ["888", ["usecase_d", "usecase_b"]],
                ["999", ["usecase_e", "usecase_b"]],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("use_case", ArrayType(StringType(), False), False),
                ]
            ),
        )

        df_partner_msisdn = spark_session.createDataFrame(
            data=[
                ["222", ["usecase_b"]],
                ["333", ["usecase_c"]],
                ["444", ["usecase_a", "usecase_b", "usecase_f", "usecase_g"]],
                ["555", ["usecase_a", "usecase_b"]],
                ["666", ["usecase_c", "usecase_b"]],
                ["777", ["usecase_c", "usecase_b"]],
                ["888", ["usecase_d", "usecase_b"]],
                ["999", ["usecase_e", "usecase_b"]],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("use_case", ArrayType(StringType(), False), False),
                ]
            ),
        )

        files_to_pack = {
            "usecase_a": {"move_to_remote": True},
            "usecase_b": {"move_to_remote": True},
            "usecase_c": {"move_to_remote": True},
            "usecase_d": {"move_to_remote": True},
            "usecase_e": {"move_to_remote": True},
            "usecase_f": {"move_to_remote": True},
            "usecase_g": {"move_to_remote": True},
        }

        check_new_msisdn_count(
            df_partner_msisdn, df_partner_msisdn_old, files_to_pack, {}
        )

        # msisdn_count_by_usecase = spark_session.createDataFrame(
        #     data = [
        #         ['usecase_a', 4, 2],
        #         ['usecase_b', 8, 1],
        #         ['usecase_c', 4, 1],
        #         ['usecase_d', 1, 0],
        #         ['usecase_e', 1, 0],
        #         ['usecase_f', 1, 0],
        #     ],
        #     schema=StructType(
        #         [
        #             StructField("use_case", StringType(), False),
        #             StructField("number_of_msisdn", LongType(), True),
        #             StructField("number_of_new_msisdn", LongType(), True)
        #         ]
        #     )
        # )
        assert mock_send_notification_to_telegram.call_count == 1

    def test_create_html(self, spark_session):
        msisdn_count_by_usecase = spark_session.createDataFrame(
            data=[
                ["usecase_a", 4, 2, True],
                ["usecase_b", 8, 1, True],
                ["usecase_c", 4, 1, False],
                ["usecase_d", 1, 0, False],
                ["usecase_e", 1, 0, True],
                ["usecase_f", 1, 0, False],
            ],
            schema=StructType(
                [
                    StructField("use_case", StringType(), False),
                    StructField("number_of_msisdn", LongType(), True),
                    StructField("number_of_new_msisdn", LongType(), True),
                    StructField("move_to_remote", BooleanType(), True),
                ]
            ),
        )

        actual_html_output = create_html(
            msisdn_count_by_usecase=msisdn_count_by_usecase,
            msisdn_count_to_move=3,
            msisdn_count_total=10,
            msisdn_count_new=2,
        )
        expected_html_output = "<!DOCTYPE html>\n<html lang=\"en\">\n\n<head>\n    <meta charset=\"UTF-8\">\n    <title>New MSISDNs Found</title>\n</head>\n\n<body>\n    <h1 style=\"color: red;\">ALERT: MSISDNs count in the partner_msisdn file</h1>\n    <br />\n    <h3>Total number of unique MSISDNs to move to remote: 3</h3>\n    <h3>Total number of unique MSISDNs: 10</h3>\n    <h3>Total number of new unique MSISDNs: 2</h3>\n\n    <table style=\"border: 3px solid black;\">\n        <tr style=\"font-weight: bold; background-color: black; color: white;\">\n            <th style=\"width: 350px\">Use Case</th>\n            <th style=\"width: 250px\">Number of new MSISDNs</th>\n            <th style=\"width: 250px\">Number of unique MSISDNs</th>\n            <th style=\"width: 250px\">Move to DMP Remote</th>\n        </tr>\n        <tr style='background-color: peachpuff;'><td>usecase_a</td><td>2</td><td>4</td><td style='background-color:#7FFF00'>True</td></tr><tr style='background-color: peachpuff;'><td>usecase_b</td><td>1</td><td>8</td><td style='background-color:#7FFF00'>True</td></tr><tr style='background-color: peachpuff;'><td>usecase_c</td><td>1</td><td>4</td><td style=''>False</td></tr><tr style='background-color: peachpuff;'><td>usecase_d</td><td>0</td><td>1</td><td style=''>False</td></tr><tr style='background-color: peachpuff;'><td>usecase_e</td><td>0</td><td>1</td><td style='background-color:#7FFF00'>True</td></tr><tr style='background-color: peachpuff;'><td>usecase_f</td><td>0</td><td>1</td><td style=''>False</td></tr>\n        <!-- INSERT TABLE ROWS HERE -->\n        <!-- <tr style=\"background-color: peachpuff;\"><td>use_case</td><td>msisdn_count</td></tr> -->\n    </table>\n\n    <br />\n    <p><small>Note: This email is auto generated and does not need reply.</small></p>\n\n</body>\n\n</html>\n"

        assert actual_html_output == expected_html_output
