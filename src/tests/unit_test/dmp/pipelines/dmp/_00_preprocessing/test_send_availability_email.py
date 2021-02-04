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

from src.dmp.pipelines.dmp._00_preprocessing.availability.send_availability_email import (
    send_availability_email,
)


class TestSendAvailabilityEmail:
    @mock.patch(
        "src.dmp.pipelines.dmp._00_preprocessing.availability.send_availability_email.send_email"
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._00_preprocessing.availability.send_availability_email.GetKedroContext.get_environment",
        return_value="dmp/training",
        autospec=True,
    )
    def test_send_availability_email_sandbox_training_env(
        self, mock_get_kedro_context_get_environment, mock_send_email, spark_session
    ):

        availability_summary = spark_session.createDataFrame(
            data=[
                [
                    "test_db_a.table_1",
                    "test_domain",
                    datetime.date(2019, 1, 21),
                    14,
                    [
                        datetime.date(2019, 1, 20),
                        datetime.date(2019, 1, 1),
                        datetime.date(2019, 1, 15),
                        datetime.date(2019, 1, 4),
                        datetime.date(2019, 1, 9),
                        datetime.date(2019, 1, 10),
                        datetime.date(2019, 1, 14),
                        datetime.date(2019, 1, 11),
                        datetime.date(2019, 1, 12),
                        datetime.date(2019, 1, 13),
                        datetime.date(2019, 1, 16),
                        datetime.date(2019, 1, 17),
                        datetime.date(2019, 1, 18),
                        datetime.date(2019, 1, 19),
                    ],
                ],
                [
                    "test_db_b.table_2",
                    "test_domain",
                    datetime.date(2019, 1, 21),
                    0,
                    None,
                ],
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

        email_notif_setup = {
            "smtp_relay_host": "dummy_host",
            "smtp_relay_port": 100,
            "from_email": "dummy@email.id",
            "to_emails": ["dummy_1@email.id", "dummy_1@email.id"],
        }

        send_availability_email(availability_summary, email_notif_setup)

        expected_email_subject = "[Darwin] Source Table DQ Availability check - Sandbox"
        expected_email_body = '<!DOCTYPE html>\n<html lang="en">\n\n<head>\n    <meta charset="UTF-8">\n    <title>DQ Availability Report - Sandbox</title>\n    <style>\n        table,\n        th,\n        td {\n            border: 1px solid black;\n            border-collapse: collapse;\n            padding: 8px\n        }\n\n        .issue {\n            background-color: #FFA6A6;\n        }\n    </style>\n</head>\n\n<body>\n    <p>Hi all,</p>\n    <p>The availability check runs every week and checks for data availability from the start date to one day before the\n        current weekstart.</p>\n\n    <table>\n        <tbody>\n            <tr>\n                <th>Domain</th>\n                <th>Table</th>\n                <th>Status</th>\n                <th>Missing Days</th>\n            </tr>\n            <tr  class="issue" >\n                <td>test_domain</td>\n                <td>test_db_a.table_1</td>\n                \n                <td> 14 days missing</td>\n                \n                <td>2019-01-01<br>2019-01-04<br>2019-01-09 to 2019-01-20 (12 days)</td>\n            </tr>\n            <tr >\n                <td>test_domain</td>\n                <td>test_db_b.table_2</td>\n                \n                <td>No Issues</td>\n                \n                <td></td>\n            </tr>\n            \n        </tbody>\n    </table>\n\n    <p><small>Note: This email is auto generated and does not need reply.</small></p>\n</body>\n\n</html>'
        expected_email_attachment = [
            {
                "name": "dq_availability_report.html",
                "body": '<!DOCTYPE html>\n<html lang="en">\n\n<head>\n    <meta charset="UTF-8">\n    <title>DQ Availability Report - Sandbox</title>\n    <style>\n        table,\n        th,\n        td {\n            border: 1px solid black;\n            border-collapse: collapse;\n            padding: 8px\n        }\n\n        .issue {\n            background-color: #FFA6A6;\n        }\n    </style>\n</head>\n\n<body>\n    <p>Hi all,</p>\n    <p>The availability check runs every week and checks for data availability from the start date to one day before the\n        current weekstart.</p>\n\n    <table>\n        <tbody>\n            <tr>\n                <th>Domain</th>\n                <th>Table</th>\n                <th>Status</th>\n                <th>Missing Days</th>\n            </tr>\n            <tr  class="issue" >\n                <td>test_domain</td>\n                <td>test_db_a.table_1</td>\n                \n                <td> 14 days missing</td>\n                \n                <td>2019-01-01<br>2019-01-04<br>2019-01-09 to 2019-01-20 (12 days)</td>\n            </tr>\n            <tr >\n                <td>test_domain</td>\n                <td>test_db_b.table_2</td>\n                \n                <td>No Issues</td>\n                \n                <td></td>\n            </tr>\n            \n        </tbody>\n    </table>\n\n    <p><small>Note: This email is auto generated and does not need reply.</small></p>\n</body>\n\n</html>',
            }
        ]

        assert mock_send_email.call_count == 1
        assert mock_send_email.call_args[0][4] == expected_email_subject
        assert mock_send_email.call_args[0][5] == expected_email_body
        assert mock_send_email.call_args[0][7] == expected_email_attachment

    @mock.patch(
        "src.dmp.pipelines.dmp._00_preprocessing.availability.send_availability_email.send_email"
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._00_preprocessing.availability.send_availability_email.GetKedroContext.get_environment",
        return_value="dmp/scoring",
        autospec=True,
    )
    def test_send_availability_email_sandbox_scoring_env(
        self, mock_get_kedro_context_get_environment, mock_send_email, spark_session
    ):

        availability_summary = spark_session.createDataFrame(
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
                [
                    "test_db_b.table_2",
                    "test_domain",
                    datetime.date(2019, 1, 21),
                    0,
                    None,
                ],
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

        email_notif_setup = {
            "smtp_relay_host": "dummy_host",
            "smtp_relay_port": 100,
            "from_email": "dummy@email.id",
            "to_emails": ["dummy_1@email.id", "dummy_1@email.id"],
        }

        send_availability_email(availability_summary, email_notif_setup)

        expected_email_subject = "[Darwin] Source Table DQ Availability check - Sandbox"
        expected_email_body = '<!DOCTYPE html>\n<html lang="en">\n\n<head>\n    <meta charset="UTF-8">\n    <title>DQ Availability Report - Sandbox</title>\n    <style>\n        table,\n        th,\n        td {\n            border: 1px solid black;\n            border-collapse: collapse;\n            padding: 8px\n        }\n\n        .issue {\n            background-color: #FFA6A6;\n        }\n    </style>\n</head>\n\n<body>\n    <p>Hi all,</p>\n    <p>The availability check runs every week and checks for data availability from the start date to one day before the\n        current weekstart.</p>\n\n    <table>\n        <tbody>\n            <tr>\n                <th>Domain</th>\n                <th>Table</th>\n                <th>Status</th>\n                <th>Missing Days</th>\n            </tr>\n            <tr  class="issue" >\n                <td>test_domain</td>\n                <td>test_db_a.table_1</td>\n                \n                <td> 14 days missing</td>\n                \n                <td>2019-01-01<br>2019-01-04<br>2019-01-09 to 2019-01-20 (12 days)</td>\n            </tr>\n            <tr >\n                <td>test_domain</td>\n                <td>test_db_b.table_2</td>\n                \n                <td>No Issues</td>\n                \n                <td></td>\n            </tr>\n            \n        </tbody>\n    </table>\n\n    <p><small>Note: This email is auto generated and does not need reply.</small></p>\n</body>\n\n</html>'
        expected_email_attachment = [
            {
                "name": "dq_availability_report.html",
                "body": '<!DOCTYPE html>\n<html lang="en">\n\n<head>\n    <meta charset="UTF-8">\n    <title>DQ Availability Report - Sandbox</title>\n    <style>\n        table,\n        th,\n        td {\n            border: 1px solid black;\n            border-collapse: collapse;\n            padding: 8px\n        }\n\n        .issue {\n            background-color: #FFA6A6;\n        }\n    </style>\n</head>\n\n<body>\n    <p>Hi all,</p>\n    <p>The availability check runs every week and checks for data availability from the start date to one day before the\n        current weekstart.</p>\n\n    <table>\n        <tbody>\n            <tr>\n                <th>Domain</th>\n                <th>Table</th>\n                <th>Status</th>\n                <th>Missing Days</th>\n            </tr>\n            <tr  class="issue" >\n                <td>test_domain</td>\n                <td>test_db_a.table_1</td>\n                \n                <td> 14 days missing</td>\n                \n                <td>2019-01-01<br>2019-01-04<br>2019-01-09 to 2019-01-20 (12 days)</td>\n            </tr>\n            <tr >\n                <td>test_domain</td>\n                <td>test_db_b.table_2</td>\n                \n                <td>No Issues</td>\n                \n                <td></td>\n            </tr>\n            \n        </tbody>\n    </table>\n\n    <p><small>Note: This email is auto generated and does not need reply.</small></p>\n</body>\n\n</html>',
            }
        ]

        assert mock_send_email.call_count == 1
        assert mock_send_email.call_args[0][4] == expected_email_subject
        assert mock_send_email.call_args[0][5] == expected_email_body
        assert mock_send_email.call_args[0][7] == expected_email_attachment

    @mock.patch(
        "src.dmp.pipelines.dmp._00_preprocessing.availability.send_availability_email.send_email"
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._00_preprocessing.availability.send_availability_email.GetKedroContext.get_environment",
        return_value="dmp_production",
        autospec=True,
    )
    def test_send_availability_email_prod_scoring_env(
        self, mock_get_kedro_context_get_environment, mock_send_email, spark_session
    ):

        availability_summary = spark_session.createDataFrame(
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
                [
                    "test_db_b.table_2",
                    "test_domain",
                    datetime.date(2019, 1, 21),
                    0,
                    None,
                ],
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

        email_notif_setup = {
            "smtp_relay_host": "dummy_host",
            "smtp_relay_port": 100,
            "from_email": "dummy@email.id",
            "to_emails": ["dummy_1@email.id", "dummy_1@email.id"],
        }

        send_availability_email(availability_summary, email_notif_setup)

        expected_email_subject = (
            "[Darwin] Source Table DQ Availability check - Production"
        )
        expected_email_body = '<!DOCTYPE html>\n<html lang="en">\n\n<head>\n    <meta charset="UTF-8">\n    <title>DQ Availability Report - Production</title>\n    <style>\n        table,\n        th,\n        td {\n            border: 1px solid black;\n            border-collapse: collapse;\n            padding: 8px\n        }\n\n        .issue {\n            background-color: #FFA6A6;\n        }\n    </style>\n</head>\n\n<body>\n    <p>Hi all,</p>\n    <p>The availability check runs every week and checks for data availability from the start date to one day before the\n        current weekstart.</p>\n\n    <table>\n        <tbody>\n            <tr>\n                <th>Domain</th>\n                <th>Table</th>\n                <th>Status</th>\n                <th>Missing Days</th>\n            </tr>\n            <tr  class="issue" >\n                <td>test_domain</td>\n                <td>test_db_a.table_1</td>\n                \n                <td> 14 days missing</td>\n                \n                <td>2019-01-01<br>2019-01-04<br>2019-01-09 to 2019-01-20 (12 days)</td>\n            </tr>\n            <tr >\n                <td>test_domain</td>\n                <td>test_db_b.table_2</td>\n                \n                <td>No Issues</td>\n                \n                <td></td>\n            </tr>\n            \n        </tbody>\n    </table>\n\n    <p><small>Note: This email is auto generated and does not need reply.</small></p>\n</body>\n\n</html>'
        expected_email_attachment = [
            {
                "name": "dq_availability_report.html",
                "body": '<!DOCTYPE html>\n<html lang="en">\n\n<head>\n    <meta charset="UTF-8">\n    <title>DQ Availability Report - Production</title>\n    <style>\n        table,\n        th,\n        td {\n            border: 1px solid black;\n            border-collapse: collapse;\n            padding: 8px\n        }\n\n        .issue {\n            background-color: #FFA6A6;\n        }\n    </style>\n</head>\n\n<body>\n    <p>Hi all,</p>\n    <p>The availability check runs every week and checks for data availability from the start date to one day before the\n        current weekstart.</p>\n\n    <table>\n        <tbody>\n            <tr>\n                <th>Domain</th>\n                <th>Table</th>\n                <th>Status</th>\n                <th>Missing Days</th>\n            </tr>\n            <tr  class="issue" >\n                <td>test_domain</td>\n                <td>test_db_a.table_1</td>\n                \n                <td> 14 days missing</td>\n                \n                <td>2019-01-01<br>2019-01-04<br>2019-01-09 to 2019-01-20 (12 days)</td>\n            </tr>\n            <tr >\n                <td>test_domain</td>\n                <td>test_db_b.table_2</td>\n                \n                <td>No Issues</td>\n                \n                <td></td>\n            </tr>\n            \n        </tbody>\n    </table>\n\n    <p><small>Note: This email is auto generated and does not need reply.</small></p>\n</body>\n\n</html>',
            }
        ]

        assert mock_send_email.call_count == 1
        assert mock_send_email.call_args[0][4] == expected_email_subject
        assert mock_send_email.call_args[0][5] == expected_email_body
        assert mock_send_email.call_args[0][7] == expected_email_attachment
