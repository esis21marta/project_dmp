from typing import List

import pyspark
import pyspark.sql.functions as f

from src.dmp.pipelines.dmp._06_qa.metrics.metrics_helper import (
    convert_array_type_column_to_array_length,
    convert_percentiles_list_into_separate_columns,
    get_column_name_of_type,
)
from src.dmp.pipelines.dmp._06_qa.qa_outlet.outlet_qa_helper import melt_qa_result
from utils import get_column_name_of_type

_null_count = lambda x: f.count(f.when(f.col(x).isNull(), 1))


def get_accuracy_completeness_metrics(
    df: pyspark.sql.DataFrame,
    aggregate_colum: str,
    non_feature_columns: List[str],
    percentiles: List[float],
    percentiles_accuracy: float,
    feature_columns: List[str] = None,
) -> pyspark.sql.DataFrame:

    # If columns are not passed then all columns except msisdn and weekstart are considered columns
    if feature_columns is None:
        feature_columns = list(set(df.columns) - set(non_feature_columns))

    # Select only columns and msisdn and weekstart
    df = df.select(feature_columns + non_feature_columns)

    # If Array exists in df, convert array to array length
    df_with_array_length = convert_array_type_column_to_array_length(df)

    numeric_columns = set(
        get_column_name_of_type(df_with_array_length, required_types=["numeric"])
    ) - set(non_feature_columns)

    qa_result = df_with_array_length.groupby(aggregate_colum).agg(
        *[
            f.max(col).alias(f"{col}__max") for col in numeric_columns
        ],  # MAX applicable only on numeric data type
        *[
            f.min(col).alias(f"{col}__min") for col in numeric_columns
        ],  # MIN applicable only on numeric data type
        *[
            f.round(f.mean(col), 4).alias(f"{col}__mean") for col in numeric_columns
        ],  # MEAN applicable only on numeric data type
        *[
            _null_count(col).alias(f"{col}__null_count") for col in feature_columns
        ],  # NULL COUNT applicable for all data types
        f.count("outlet_id").alias("row_count"),  # ROW COUNT
        *[
            f.expr(
                f"percentile_approx(`{col}`, array({','.join(map(str, percentiles))}), {percentiles_accuracy})"
            ).alias(f"{col}__percentiles")
            for col in numeric_columns  # PERCENTILES applicable only for numeric data type
        ],
    )

    qa_result_melted = melt_qa_result(qa_result, aggregate_colum)
    qa_result_melted = qa_result_melted.join(
        qa_result.select([aggregate_colum, "row_count"]),
        how="inner",
        on=[aggregate_colum],
    )
    qa_result_melted = qa_result_melted.withColumn(
        "null_percentage", f.col("null_count") / f.col("row_count")
    ).drop("null_count")

    if "percentiles" in qa_result_melted.columns:
        qa_result_melted = convert_percentiles_list_into_separate_columns(
            qa_result_melted, percentiles
        )

    return qa_result_melted.repartition(5)
