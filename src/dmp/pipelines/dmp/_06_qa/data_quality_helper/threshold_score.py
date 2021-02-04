import pyspark
import pyspark.sql.functions as f
from pyspark.sql import Window
from pyspark.sql.types import IntegerType


def get_rolling_window(num_days: int):
    """
    Rolling window specific to column, metric and table_name
    Args:
        num_days: Number of days to roll-back to.

    Returns:
        Window from num_days -> today
    """
    days = lambda i: (i - 1) * 86400
    w = (
        Window()
        .partitionBy("columns", "metric", "table_name")
        .orderBy(f.col("weekstart").cast("timestamp").cast("long"))
        .rangeBetween(-days(num_days), 0)
    )
    return w


def get_threshold_score(
    df: pyspark.sql.DataFrame, iqr_lookback_days: int, manual_threshold
) -> pyspark.sql.DataFrame:
    df = df.withColumn(
        "quartile_1",
        f.coalesce(
            f.expr("percentile_approx(value, 0.25)").over(
                get_rolling_window(iqr_lookback_days)
            ),
            f.col("value"),
        ),
    ).withColumn(
        "quartile_3",
        f.coalesce(
            f.expr("percentile_approx(value, 0.75)").over(
                get_rolling_window(iqr_lookback_days)
            ),
            f.col("value"),
        ),
    )

    # TODO: Replace lower_threshold and higher_threshold with manual threshold

    df = (
        df.withColumn("iqr", f.col("quartile_3") - f.col("quartile_1"))
        .withColumn("lower_threshold", f.col("quartile_1") - (1.5 * f.col("iqr")))
        .withColumn("higher_threshold", f.col("quartile_3") + (1.5 * f.col("iqr")))
        .withColumn("weekstart", f.to_timestamp(f.col("weekstart")))
        .withColumn(
            "score",
            f.col("value")
            .between(f.col("lower_threshold"), f.col("higher_threshold"))
            .cast(IntegerType())
            * 100,
        )
    )
    return df.select(
        [
            "columns",
            "weekstart",
            "metric",
            "layer",
            "table_name",
            "master_mode",
            "score",
        ]
    )
