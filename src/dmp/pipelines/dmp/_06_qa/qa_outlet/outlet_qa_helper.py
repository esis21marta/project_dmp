import pyspark
import pyspark.sql.functions as f
from pyspark.sql.types import StringType

from utils import join_all, melt


def melt_qa_result(qa_result: pyspark.sql.DataFrame, aggregate_colum: str):
    """
    Convert QA result into multiple rows such that feature columns are rows and columns are metrics
    Args:
        qa_result:

    Returns:
        Melted dataframe where columns are metrics used for QA Check
    """
    metrics = set([col.rsplit("__")[-1] for col in qa_result.columns if "__" in col])

    qa_check_aggregates = []

    for metric in metrics:
        cols = [col for col in qa_result.columns if col.endswith(metric)]
        qa_result_aggregate = qa_result.select(
            [f.col(aggregate_colum)]
            + [f.col(c).alias(c.replace(f"__{metric}", "")) for c in cols]
        )
        result_melted = melt(
            df=qa_result_aggregate,
            id_vars=[aggregate_colum],
            value_vars=list(set(qa_result_aggregate.columns) - set((aggregate_colum,))),
            var_name="columns",
            value_name=metric,
        )
        qa_check_aggregates.append(result_melted)

    qa_result_melted = join_all(
        qa_check_aggregates, how="outer", on=[aggregate_colum, "columns"]
    )

    return qa_result_melted


def add_metadata_to_output_file(
    qa_df: pyspark.sql.DataFrame,
    pipeline_mode: str,
    table_name: str,
    file_path: str,
    old_file_path: str = None,
):
    qa_df = (
        qa_df.withColumn("layer", f.lit("master"))
        .withColumn("file_path", f.lit(file_path).cast(StringType()))
        .withColumn("old_file_path", f.lit(old_file_path).cast(StringType()))
        .withColumn("master_mode", f.lit(pipeline_mode).cast(StringType()))
        .withColumn("table_name", f.lit(table_name).cast(StringType()))
    )
    qa_df = qa_df.crossJoin(qa_df.select(f.max(f.col("weekstart")).alias("run_time")))

    return qa_df
