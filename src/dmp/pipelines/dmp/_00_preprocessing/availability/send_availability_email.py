import os
from typing import Any, Dict

import pyspark
from jinja2 import Environment, FileSystemLoader

from utils import GetKedroContext, send_email


def format_missing_days(dates):
    if dates == None or len(dates) == 0:
        return ""
    dates.sort()
    dates_range_formatted = []
    dates_ranges = []
    start_date = dates[0]
    end_date = dates[0]
    for date in dates[1:]:
        if (date - end_date).days == 1:
            end_date = date
        else:
            dates_ranges.append((start_date, end_date))
            start_date = date
            end_date = date
    dates_ranges.append((start_date, end_date))
    for dates_range in dates_ranges:
        days = (dates_range[1] - dates_range[0]).days + 1
        if days == 1:
            dates_range_formatted.append(dates_range[1].strftime("%Y-%m-%d"))
        else:
            dates_range_formatted.append(
                f'{dates_range[0].strftime("%Y-%m-%d")} to {dates_range[1].strftime("%Y-%m-%d")} ({days} days)'
            )
    return "<br>".join(dates_range_formatted)


def send_availability_email(
    availability_summary: pyspark.sql.DataFrame, email_notif_setup: Dict[str, Any]
) -> None:

    if GetKedroContext.get_environment() == "dmp_production":
        environment = "Production"
    else:
        environment = "Sandbox"

    email_subject = f"[Darwin] Source Table DQ Availability check - {environment}"

    rows = []

    for row in availability_summary.collect():
        row = row.asDict()
        row["missing_days"] = format_missing_days(row["missing_days"])
        rows.append(row)

    file_loader = FileSystemLoader(os.path.dirname(__file__))
    env = Environment(loader=file_loader)
    template = env.get_template("availability_email_template.html")
    html = template.render(environment=environment, rows=rows)

    attachments = [{"name": "dq_availability_report.html", "body": html}]

    send_email(
        email_notif_setup["smtp_relay_host"],
        email_notif_setup["smtp_relay_port"],
        email_notif_setup["from_email"],
        email_notif_setup["to_emails"],
        email_subject,
        html,
        "html",
        attachments,
    )
