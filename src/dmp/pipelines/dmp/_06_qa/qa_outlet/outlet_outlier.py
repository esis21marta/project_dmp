import pyspark
import pyspark.sql.functions as f
from pyspark.sql.types import LongType

from src.dmp.pipelines.dmp._06_qa.metrics.metrics_helper import (
    convert_array_type_column_to_array_length,
    get_column_name_of_type,
)
from src.dmp.pipelines.dmp._06_qa.qa_outlet.outlet_qa_helper import melt_qa_result


def get_outlier(
    df: pyspark.sql.DataFrame,
    aggregate_column: str,
    df_metrics: pyspark.sql.DataFrame,
    non_feature_columns: list,
    columns: list = [],
):
    """
    Returns a melted dataframe containing higher and lower outlier counts using IQR method for values in that weekstart.
    Args:
        df: Raw dataframe.
        df_metrics: Metrics dataframe.
        columns: Filtered columns

    Returns:
        Dataframe with columns of count
    """
    # If columns are not passed then all columns except non feature columns are considered columns
    if not columns:
        columns = list(set(df.columns) - set(non_feature_columns))

    # Select only columns and non feature columns
    df = df.select(list(set(columns).union(set(non_feature_columns))))

    # If Array exists in df, convert array to array length
    df_with_array_length = convert_array_type_column_to_array_length(df)

    numeric_columns = set(
        get_column_name_of_type(df_with_array_length, required_types=["numeric"])
    ) - set(non_feature_columns)

    if len(numeric_columns) == 0:
        return df_metrics.select(
            f.col(aggregate_column),
            f.col("columns"),
            f.lit(None).cast(LongType()).alias("count_higher_outlier"),
            f.lit(None).cast(LongType()).alias("count_lower_outlier"),
        )

    df_metrics = (
        df_metrics.select(
            aggregate_column, "columns", "percentile_0_25", "percentile_0_75"
        )
        .groupBy(aggregate_column)
        .pivot("columns")
        .agg(
            f.first(f.col("percentile_0_25")).alias("q1"),
            f.first(f.col("percentile_0_75")).alias("q3"),
            (
                f.first(f.col("percentile_0_75")) - f.first(f.col("percentile_0_25"))
            ).alias("iqr"),
        )
    )

    joined_df = df_with_array_length.join(
        f.broadcast(df_metrics), on=aggregate_column, how="inner"
    )

    outlier_results = joined_df.groupBy(aggregate_column).agg(
        *[
            f.count(
                f.when(
                    f.col(num_col)
                    < (f.col(f"{num_col}_q1") - (1.5 * (f.col(f"{num_col}_iqr")))),
                    1,
                )
            )
            .cast(LongType())
            .alias(f"{num_col}__count_lower_outlier")
            for num_col in numeric_columns
        ],
        *[
            f.count(
                f.when(
                    f.col(num_col)
                    > (f.col(f"{num_col}_q3") + (1.5 * (f.col(f"{num_col}_iqr")))),
                    1,
                )
            )
            .cast(LongType())
            .alias(f"{num_col}__count_higher_outlier")
            for num_col in numeric_columns
        ],
    )

    outlier_results_melted = melt_qa_result(outlier_results, aggregate_column)
    return outlier_results_melted.repartition(1)
