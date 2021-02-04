import io
import logging
from typing import Any, Dict

import pandas as pd
import pyspark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from utils import get_config_parameters, get_end_date, send_email
from utils.spark_data_set_helper import get_file_path

log = logging.getLogger(__name__)


def get_xlsx_buffer(df):
    """
    Get the Excel buffer that contain the contents of the dataframe

    Args:
        df: Pandas dataframe

    Returns:
    The Excel buffer that contain entire contents of the dataframe

    """
    with io.BytesIO() as buffer:
        writer = pd.ExcelWriter(buffer)
        df.to_excel(writer)
        writer.save()
        return buffer.getvalue()


def get_csv_buffer(df):
    """
    Get the CSV buffer that contain the contents of the dataframe

    Args:
        df: Pandas dataframe

    Returns:
    The CSV buffer that contain entire contents of the dataframe

    """
    with io.StringIO() as buffer:
        df.to_csv(buffer)
        return buffer.getvalue()


def get_file_buffer(df, file_format):
    """
    Get the buffer that contain the contents of the dataframe, this will be useful
    if we want to store the dataframe as the email attachments

    Args:
        df: Pandas dataframe
        file_format: file format to be written, support xlsx or csv

    Returns:
    The buffer that contain entire contents of the dataframe

    """
    if file_format == "xlsx":
        return get_xlsx_buffer(df)
    else:
        return get_csv_buffer(df)


def send_reporting_via_email(
    reporting_email_setup: Dict[Any, Any]
) -> pyspark.sql.DataFrame:
    """
    This function will send a reporting email with its attachments (if any).
    The configuration should be present on parameters yml file with key
    `reporting_email_setup`. You can find the example of `reporting_email_setup`
    on `conf/external/reporting_example/home_credit/parameters_reports.yml`.

    Args:
        reporting_email_setup: Email setup & configuration from parameters file

    """
    email_notif_setup = reporting_email_setup["email_notif_setup"]
    email_components = reporting_email_setup["email_components"]
    spark = SparkSession.builder.getOrCreate()

    if reporting_email_setup.get("email_notif_enabled"):
        log.info(f"Sending Email...")
        log.info(f"from_email: {email_notif_setup['from_email']}")
        log.info(f"to_emails: {email_notif_setup['to_emails']}")
        subject = email_components["subject"]
        if email_components["subject_with_weekstart"]:
            last_weekstart = get_end_date(partition_column="weekstart")
            subject = f"{subject} - {last_weekstart}"

        body = email_components["body"].replace("\\n", "\n")
        body_type = email_components["body_type"]
        attachments = email_components["attachments"]

        final_attachments = []
        for attachment in attachments:
            if "mck_dmp_" in attachment["file_reference"]:
                file_path = get_file_path(attachment["file_reference"])
                file_format = "parquet"
            else:
                conf_catalog = get_config_parameters(config="catalog")
                file_catalog = conf_catalog[attachment["file_reference"]]
                file_path = get_file_path(filepath=file_catalog["filepath"])
                file_format = file_catalog["file_format"]

            df = spark.read.load(file_path, file_format)
            if attachment["only_pick_latest"]:
                time_column = attachment["time_column"]
                dt = get_end_date(partition_column=time_column)
                df = df.filter(f.col(time_column) == dt)

            pandas_df = df.toPandas()
            path = get_file_buffer(pandas_df, attachment["attachment_format"])
            attachment_name = (
                f"{attachment['attachment_name']}.{attachment['attachment_format']}"
            )
            final_attachments.append({"name": attachment_name, "body": path})
        send_email(
            email_notif_setup["smtp_relay_host"],
            email_notif_setup["smtp_relay_port"],
            email_notif_setup["from_email"],
            email_notif_setup["to_emails"],
            subject,
            body,
            body_type,
            final_attachments,
        )
