import os
import subprocess
from typing import Any, Dict

import pyspark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from utils import send_email


def _get_hdfs_quota_in_bytes(hdfs_path):
    command = f"hdfs dfs -count -q -v {hdfs_path}"
    command = command.split(" ")
    process = subprocess.Popen(command, stdout=subprocess.PIPE)
    stdout = process.communicate()[0].decode("utf-8")
    rows = stdout.split("\n")

    header = rows[0]
    data = rows[1]
    space_quota = int(
        data[
            header.index("REM_QUOTA")
            + len("REM_QUOTA") : header.index("SPACE_QUOTA")
            + len("SPACE_QUOTA")
        ].strip()
    )
    rem_space_quota = int(
        data[
            header.index("SPACE_QUOTA")
            + len("SPACE_QUOTA") : header.index("REM_SPACE_QUOTA")
            + len("REM_SPACE_QUOTA")
        ].strip()
    )
    filled_quota_pct = "{}%".format(
        round((space_quota - rem_space_quota) * 100 / space_quota, 1,)
    )
    return space_quota, rem_space_quota, filled_quota_pct


def _get_large_directories(hdfs_path, threshold, quota):
    big_dirs = []
    level = hdfs_path.count("/") - 4
    output = _get_directory_size(
        hdfs_path=hdfs_path, threshold=threshold, quota=quota, level=level
    )
    big_dirs += output
    if len(output) > 0:
        for i in output:
            if level <= 4:
                big_dirs += _get_large_directories(
                    hdfs_path=i[0], threshold=threshold, quota=quota
                )
    return big_dirs


def _get_large_tables(hdfs_path, threshold, quota):
    return _get_directory_size(
        hdfs_path=hdfs_path, threshold=threshold, quota=quota, level=1
    )


def _get_directory_size(hdfs_path, threshold, quota, level):
    command = f"hdfs dfs -du {hdfs_path}"
    command = command.split(" ")
    process = subprocess.Popen(command, stdout=subprocess.PIPE)
    stdout = process.communicate()[0].decode("utf-8")
    rows = stdout.split("\n")
    big_dir = []
    for row in rows:
        if row != "":
            size = row[row.index(" ") : row.index("hdfs:///")]
            size = int(size.strip())
            percent_filled = "{}%".format(round(size * 100 / quota, 1))
            if size >= threshold:
                location = row[row.index("hdfs:///") :]
                size_tb = "{} T".format(round(size / 1024 / 1024 / 1024 / 1024, 1))
                big_dir.append([location, size, size_tb, percent_filled, level])
    return big_dir


def _send_notification_to_telegram(
    df_size_parent: pyspark.sql.DataFrame,
    df_size_child: pyspark.sql.DataFrame,
    email_setup: Dict[str, Any],
) -> None:
    html = _create_html(df_size_parent=df_size_parent, df_size_child=df_size_child)
    subject = "ALERT: Storage Capacity"
    attachments = [{"name": f"Storage Capacity.html", "body": html}]
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


def _create_html(
    df_size_parent: pyspark.sql.DataFrame, df_size_child: pyspark.sql.DataFrame
) -> str:
    html_path = os.path.join(os.path.dirname(__file__), "email_template.html")
    with open(html_path, "r") as et:
        html_template = et.read()
    hdfs_size_parent_template = (
        "<tr style='background-color: peachpuff;'>"
        "<td>{path}</td>"
        "<td>{space_quota_tb}</td>"
        "<td>{rem_space_quota_tb}</td>"
        "<td>{filled_space}</td>"
        "</tr>"
    )

    hdfs_location_rows = ""
    for row in df_size_parent.collect():
        hdfs_location_rows = hdfs_location_rows + hdfs_size_parent_template.format(
            path=row["hdfs_path"],
            space_quota_tb=row["space_quota_tb"],
            rem_space_quota_tb=row["rem_space_quota_tb"],
            filled_space=row["filled_space"],
        )

    dmp_production_dir_rows = _get_table_rows(
        df_size_child.filter(
            f.col("hdfs_path").contains("hdfs:///data/landing/dmp_production")
        )
    )

    dmp_staging_dir_rows = _get_table_rows(
        df_size_child.filter(
            f.col("hdfs_path").contains("hdfs:///data/landing/dmp_staging")
        )
    )

    gx_pnt_dir_rows = _get_table_rows(
        df_size_child.filter(f.col("hdfs_path").contains("hdfs:///data/landing/gx_pnt"))
    )

    dmp_remote_dir_rows = _get_table_rows(
        df_size_child.filter(
            f.col("hdfs_path").contains("hdfs:///data/landing/dmp_remote")
        )
    )

    dmp_production_hive_rows = _get_table_rows(
        df_size_child.filter(
            f.col("hdfs_path").contains("hdfs:///user/hive/warehouse/dmp_production.db")
        )
        .withColumn(
            "hdfs_path",
            f.regexp_replace(
                f.col("hdfs_path"),
                "hdfs:///user/hive/warehouse/dmp_production.db/",
                "dmp_production.",
            ),
        )
        .sort(f.col("size").desc())
    )

    mck_hive_rows = _get_table_rows(
        df_size_child.filter(
            f.col("hdfs_path").contains("hdfs:///user/hive/warehouse/mck.db")
        )
        .withColumn(
            "hdfs_path",
            f.regexp_replace(
                f.col("hdfs_path"), "hdfs:///user/hive/warehouse/mck.db/", "mck.",
            ),
        )
        .sort(f.col("size").desc())
    )

    dmp_remote_hive_rows = _get_table_rows(
        df_size_child.filter(
            f.col("hdfs_path").contains("hdfs:///user/hive/warehouse/dmp_remote.db")
        )
        .withColumn(
            "hdfs_path",
            f.regexp_replace(
                f.col("hdfs_path"),
                "hdfs:///user/hive/warehouse/dmp_remote.db/",
                "dmp_remote.",
            ),
        )
        .sort(f.col("size").desc())
    )

    html = html_template.format(
        hdfs_location_rows=hdfs_location_rows,
        gx_pnt_dir_rows=gx_pnt_dir_rows,
        dmp_production_dir_rows=dmp_production_dir_rows,
        dmp_staging_dir_rows=dmp_staging_dir_rows,
        dmp_remote_dir_rows=dmp_remote_dir_rows,
        dmp_production_hive_rows=dmp_production_hive_rows,
        mck_hive_rows=mck_hive_rows,
        dmp_remote_hive_rows=dmp_remote_hive_rows,
    )
    return html


def _get_table_rows(df: pyspark.sql.DataFrame):
    table_rows = ""
    hdfs_size_child_template = (
        "<tr style='background-color: peachpuff;'>"
        "<td>{hdfs_path}</td>"
        "<td>{size_tb}</td>"
        "<td>{filled_pct}</td>"
        "</tr>"
    )
    for row in df.collect():
        table_rows += hdfs_size_child_template.format(
            hdfs_path=row["hdfs_path"],
            size_tb=row["size_tb"],
            filled_pct=row["filled_pct"],
        )

    return table_rows


def storage_checks(email_setup: Dict[str, Any]) -> None:

    paths = [
        "hdfs:///data/landing/dmp_production",
        "hdfs:///data/landing/dmp_staging",
        "hdfs:///data/landing/gx_pnt",
        "hdfs:///data/landing/dmp_remote",
    ]

    tables = [
        "hdfs:///user/hive/warehouse/dmp_production.db",
        "hdfs:///user/hive/warehouse/mck.db",
        "hdfs:///user/hive/warehouse/dmp_remote.db",
    ]

    value_size_parent = []
    value_size_paths = []

    for path in paths:
        (
            space_quota_bytes,
            rem_space_quota_bytes,
            filled_quota_pct,
        ) = _get_hdfs_quota_in_bytes(hdfs_path=path)
        space_quota_tb = round(space_quota_bytes / 1024 / 1024 / 1024 / 1024, 1)
        rem_space_quota_tb = round(rem_space_quota_bytes / 1024 / 1024 / 1024 / 1024, 1)
        value_size_parent.append(
            [path, space_quota_tb, rem_space_quota_tb, filled_quota_pct]
        )
        big_directories = _get_large_directories(
            hdfs_path=path,
            threshold=int(space_quota_bytes / 20),  # 5%
            quota=space_quota_bytes,
        )
        value_size_paths.extend(big_directories)

    for table in tables:
        (
            space_quota_bytes,
            rem_space_quota_bytes,
            filled_quota_pct,
        ) = _get_hdfs_quota_in_bytes(hdfs_path=table)
        space_quota_tb = round(space_quota_bytes / 1024 / 1024 / 1024 / 1024, 1)
        rem_space_quota_tb = round(rem_space_quota_bytes / 1024 / 1024 / 1024 / 1024, 1)
        value_size_parent.append(
            [table, space_quota_tb, rem_space_quota_tb, filled_quota_pct]
        )
        big_directories = _get_large_tables(
            hdfs_path=table,
            threshold=int(space_quota_bytes / 100),  # 1%
            quota=space_quota_bytes,
        )
        value_size_paths.extend(big_directories)

    spark = SparkSession.builder.getOrCreate()
    df_size_parent = spark.createDataFrame(
        data=value_size_parent,
        schema=["hdfs_path", "space_quota_tb", "rem_space_quota_tb", "filled_space"],
    )

    df_size_child = spark.createDataFrame(
        data=value_size_paths,
        schema=["hdfs_path", "size", "size_tb", "filled_pct", "level"],
    )
    df_size_child = df_size_child.filter(f.col("level") <= 4).sort(
        f.col("hdfs_path").asc()
    )

    _send_notification_to_telegram(
        df_size_parent=df_size_parent,
        df_size_child=df_size_child,
        email_setup=email_setup,
    )
