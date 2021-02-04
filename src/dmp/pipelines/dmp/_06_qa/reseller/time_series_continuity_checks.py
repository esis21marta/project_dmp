import pyspark.sql.functions as f

from utils import next_week_start_day


def check_digipos_time_series_continuity(df_digipos_ref):
    df_outlet_id_min_max_trx_dates = (
        df_digipos_ref.groupBy("outlet_id")
        .agg(
            f.min("event_date").alias("min_date"),
            f.max("event_date").alias("max_date"),
            f.countDistinct("event_date").alias("actual_days"),
        )
        .withColumn("digipos_start_date", f.lit("2019-06-01"))
        .withColumn("min_date", f.greatest("digipos_start_date", "min_date"))
        .withColumn(
            "ideal_days",
            (
                f.lit(1)
                + f.datediff(
                    f.to_date("max_date", "yyyy-MM-dd"),
                    f.to_date("min_date", "yyyy-MM-dd"),
                )
            ),
        )
        .withColumn("diff_days", f.col("ideal_days") - f.col("actual_days"))
        .select(
            "outlet_id",
            "min_date",
            "max_date",
            "ideal_days",
            "actual_days",
            "diff_days",
        )
    )

    df_outlet_id_min_max_trx_dates = df_outlet_id_min_max_trx_dates.withColumn(
        "run_date_time", f.current_timestamp()
    ).withColumn("weekstart", next_week_start_day("run_date_time"))
    return df_outlet_id_min_max_trx_dates
