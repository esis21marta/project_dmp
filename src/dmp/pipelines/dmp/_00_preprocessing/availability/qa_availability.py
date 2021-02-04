import pyspark
import pyspark.sql.functions as f

from utils import union_all_with_name

from .check_domain_availability import check_domain_availability


def qa_availability(raw_tables) -> pyspark.sql.DataFrame:
    availability_status_dfs = []
    for domain, tables in raw_tables.items():
        availability_status_dfs.append(check_domain_availability(domain, tables))

    availability_detailed_df = union_all_with_name(availability_status_dfs)
    availability_detailed_df = availability_detailed_df.withColumn(
        "is_missing", 1 - f.col("is_available")
    ).drop("is_available")

    missing_count_df = availability_detailed_df.groupBy(
        ["table_name", "domain", "run_time"]
    ).agg(f.sum("is_missing").alias("number_of_missing_days"))

    missing_days_df = (
        availability_detailed_df.filter(f.col("is_missing") == 1)
        .groupBy(["table_name", "domain", "run_time"])
        .agg(f.collect_list(f.col("date")).alias("missing_days"))
    )

    availability_summary_df = missing_count_df.join(
        missing_days_df, on=["table_name", "domain", "run_time"], how="outer"
    )

    return {
        "availability_detailed": availability_detailed_df,
        "availability_summary": availability_summary_df,
    }
