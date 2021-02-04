import os
from typing import Any, Dict

import pyspark
import pyspark.sql.functions as f

from utils import send_email


def get_msisdn_count_by_usecase(df):
    return (
        df.withColumn("use_case", f.explode(f.col("use_case")))
        .groupBy("use_case")
        .agg(f.count("msisdn").alias("number_of_msisdn"))
    )


def get_msisdn_count_to_move_to_remote(df, usecase_to_move_to_remote):
    return (
        df.withColumn("use_case", f.explode(f.col("use_case")))
        .filter(f.col("use_case").isin(usecase_to_move_to_remote))
        .select("msisdn")
        .distinct()
        .count()
    )


def check_new_msisdn_count(
    df_partner_msisdn: pyspark.sql.DataFrame,
    df_partner_msisdn_old: pyspark.sql.DataFrame,
    files_to_pack: Dict[str, Any],
    email_notif_setup: Dict[str, Any],
) -> int:
    """Check for new msisdn. If new MSISDN is detected, send the alert

    Args:
        df_partner_msisdn (pyspark.sql.DataFrame): New MSISDN table
        df_partner_msisdn_old (pyspark.sql.DataFrame): Old MSISDN table
        email_notif_setup (Dict[str, Any]): Settings for Email Notification
    """
    df_partner_msisdn.cache()

    usecase_to_move_to_remote = [
        key for key, value in files_to_pack.items() if value["move_to_remote"] == True
    ]

    df_new_msisdns = df_partner_msisdn.select("msisdn").exceptAll(
        df_partner_msisdn_old.select("msisdn")
    )

    df_new_msisdns.cache()

    msisdn_count_total = df_partner_msisdn.count()
    msisdn_count_new = df_new_msisdns.count()
    msisdn_count_to_move = get_msisdn_count_to_move_to_remote(
        df_partner_msisdn, usecase_to_move_to_remote
    )

    df_new_msisdns = df_new_msisdns.join(df_partner_msisdn, ["msisdn"]).select(
        "msisdn", "use_case"
    )

    msisdn_count_by_usecase_total_df = get_msisdn_count_by_usecase(df_partner_msisdn)
    msisdn_count_by_usecase_new_df = get_msisdn_count_by_usecase(df_new_msisdns)

    msisdn_count_by_usecase = (
        msisdn_count_by_usecase_total_df.join(
            msisdn_count_by_usecase_new_df.withColumnRenamed(
                "number_of_msisdn", "number_of_new_msisdn"
            ),
            on="use_case",
            how="outer",
        )
        .withColumn("move_to_remote", f.col("use_case").isin(usecase_to_move_to_remote))
        .fillna(0)
    )

    send_notification_to_telegram(
        msisdn_count_by_usecase=msisdn_count_by_usecase,
        msisdn_count_to_move=msisdn_count_to_move,
        msisdn_count_total=msisdn_count_total,
        msisdn_count_new=msisdn_count_new,
        email_notif_setup=email_notif_setup,
    )

    return msisdn_count_to_move


def send_notification_to_telegram(
    msisdn_count_by_usecase: pyspark.sql.DataFrame,
    msisdn_count_to_move: int,
    msisdn_count_total: int,
    msisdn_count_new: int,
    email_notif_setup: Dict[str, Any],
) -> None:
    html = create_html(
        msisdn_count_by_usecase=msisdn_count_by_usecase,
        msisdn_count_to_move=msisdn_count_to_move,
        msisdn_count_total=msisdn_count_total,
        msisdn_count_new=msisdn_count_new,
    )
    subject = "ALERT: MSISDNs count in the partner_msisdn file"
    attachments = [{"name": f"New_MSISDNs_Stats.html", "body": html}]
    send_email(
        email_notif_setup["smtp_relay_host"],
        email_notif_setup["smtp_relay_port"],
        email_notif_setup["from_email"],
        email_notif_setup["to_emails"],
        subject,
        html,
        "html",
        attachments,
    )


def create_html(
    msisdn_count_by_usecase: pyspark.sql.DataFrame,
    msisdn_count_to_move: int,
    msisdn_count_total: int,
    msisdn_count_new: int,
) -> str:
    html_path = os.path.join(os.path.dirname(__file__), "email_template.html")
    with open(html_path, "r") as et:
        html_template = et.read()
    row_template = "<tr style='background-color: peachpuff;'><td>{use_case}</td><td>{number_of_new_msisdn}</td><td>{number_of_msisdn}</td><td style='{move_to_remote_style}'>{move_to_remote}</td></tr>"
    table_rows = ""
    for row in msisdn_count_by_usecase.collect():
        table_rows = table_rows + row_template.format(
            use_case=row["use_case"],
            number_of_new_msisdn=row["number_of_new_msisdn"],
            number_of_msisdn=row["number_of_msisdn"],
            move_to_remote=row["move_to_remote"],
            move_to_remote_style="background-color:#7FFF00"
            if row["move_to_remote"]
            else "",
        )
    html = html_template.format(
        msisdn_count_to_move=msisdn_count_to_move,
        msisdn_count_total=msisdn_count_total,
        msisdn_count_new=msisdn_count_new,
        table_rows=table_rows,
    )

    return html
