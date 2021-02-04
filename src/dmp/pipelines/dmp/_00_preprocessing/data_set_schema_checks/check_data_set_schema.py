import logging
import os
from typing import Any, Dict

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from utils import GetKedroContext, send_email

log = logging.getLogger(__name__)


def check_data_set_schema(schema: Dict[str, Any], email_setup: Dict[str, Any]) -> None:
    kedro_context = GetKedroContext.get_context()
    error_data = []

    for sc in schema.keys():
        dataset = kedro_context.catalog.load(sc)
        df_schema = dataset.schema.jsonValue().get("fields")
        df_schema_2 = {}
        for col in df_schema:
            df_schema_2[col.get("name")] = col.get("type")
        no_error = True

        expected_schema = schema[sc]
        for col in expected_schema.keys():
            col_data_type = df_schema_2.pop(col, None)
            if col_data_type is None:
                error_data.append((sc, col, f"Doesn't Exists in data set `{sc}`"))
                log.error(f"Data Set: `{sc}`")
                log.error(f"Column: `{col}` does not exists in data set `{sc}`")
                no_error = False
            elif col_data_type != expected_schema[col]:
                error_data.append(
                    (
                        sc,
                        col,
                        f"Data Type Mismatch: Expected Data Type: `{expected_schema[col]}`, Actual Data Type: `{col_data_type}`",
                    )
                )
                log.error(f"Data Set: `{sc}`, Column: `{col}`")
                log.error(
                    f"Expected Data Type: `{expected_schema[col]}`, Actual Data Type: `{col_data_type}`"
                )
                no_error = False
        if no_error:
            log.info(f"All the columns are as expected in dataset {sc}")

    if len(error_data) > 0:
        spark = SparkSession.builder.getOrCreate()
        error_schema = StructType(
            [
                StructField("Data Set", StringType()),
                StructField("Column Name", StringType()),
                StructField("Error", StringType()),
            ]
        )
        error_df = spark.createDataFrame(data=error_data, schema=error_schema)

        send_notification_to_telegram(df=error_df, email_setup=email_setup)


def send_notification_to_telegram(
    df: pyspark.sql.DataFrame, email_setup: Dict[str, Any]
) -> None:
    html = create_html(df=df)
    subject = "ALERT: Issues in Data Sets Found"
    attachments = [{"name": f"data_source_errors.html", "body": html}]
    send_email(
        host=email_setup["smtp_relay_host"],
        port=email_setup["smtp_relay_port"],
        sender=email_setup["from_email"],
        recipients=email_setup["to_emails"],
        subject=subject,
        body=html,
        body_type="html",
        attachments=attachments,
    )


def create_html(df: pyspark.sql.DataFrame) -> str:
    html_path = os.path.join(os.path.dirname(__file__), "email_template.html")
    with open(html_path, "r") as et:
        html_template = et.read()
    row_template = (
        "<tr style='background-color: peachpuff;'>"
        "<td>{data_set}</td>"
        "<td>{column_name}</td>"
        "<td>{error}</td>"
        "</tr>"
    )
    table_rows = ""
    for row in df.collect():
        table_rows = table_rows + row_template.format(
            data_set=row["Data Set"], column_name=row["Column Name"], error=row["Error"]
        )
    html = html_template.format(table_rows=table_rows)

    return html
