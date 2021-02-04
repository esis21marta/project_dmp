import logging
from itertools import chain
from typing import List

import pyspark
import pyspark.sql.functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

logger = logging.getLogger(__name__)


def get_run_time_df(df):
    return df.agg(f.max("run_time").alias("run_time")).withColumn(
        "run_time", f.to_date(f.col("run_time"), "yyyy-MM-dd")
    )


def add_dimension(score_df, dimension_metric_mapping):
    metric_doimension_mapping = {}
    for dimension, metrics in dimension_metric_mapping.items():
        for metric in metrics:
            metric_doimension_mapping[metric] = dimension
    mapping_expr = f.create_map(
        [f.lit(x) for x in chain(*metric_doimension_mapping.items())]
    )
    score_df = score_df.withColumn("dimension", mapping_expr.getItem(f.col("metric")))
    return score_df


def add_domain_name(metrics_df, score_df, feature_domain_mapping, table_domain_mapping):
    @udf(returnType=StringType())
    def extract_domain(layer, table_name, column):
        if layer == "master":
            for feature_prefix, domain in feature_domain_mapping.items():
                if column.startswith(feature_prefix):
                    return domain
            logger.warn(
                f"Domain not found! Column Name: {column} Table Name: {table_name} Layer: {layer}"
            )
            return None
        else:
            return table_domain_mapping.get(table_name)

    distinct_columns = metrics_df.select("layer", "table_name", "columns").distinct()
    distinct_columns_with_domain = distinct_columns.withColumn(
        "domain", extract_domain(f.col("layer"), f.col("table_name"), f.col("columns"))
    )
    score_df = score_df.join(
        distinct_columns_with_domain, on=["columns", "layer", "table_name"], how="inner"
    )
    return score_df


def get_table_domain_mapping(params_qa, layer):
    table_domain_mapping = {}
    if layer != "master":
        table_domain_mapping = {}
        for domain, tables in params_qa["catalog"].items():
            for table in tables:
                for table_name in table.keys():
                    table_domain_mapping[table_name] = domain
    return table_domain_mapping


def filter_data_for_scoring(
    metrics_df: pyspark.sql.DataFrame,
    master_mode: str,
    metrics: List[str],
    layer: str = None,
    table_name: str = None,
    dimension: str = None,
) -> pyspark.sql.DataFrame:
    # Filter by layer
    if layer:
        metrics_df = metrics_df.filter(f.col("layer") == layer)

    # Filter by table if required (in case of outlets)
    if table_name:
        metrics_df = metrics_df.filter(f.col("table_name") == table_name)

    # Filter by master mode - training/scoring
    metrics_df = metrics_df.filter(f.col("master_mode") == master_mode)

    if master_mode == "training" or layer == "feature" or dimension in ["Consistency"]:
        # Selected latest runtime
        max_run_time_df = metrics_df.groupBy("table_name").agg(
            f.max(f.col("run_time")).alias("run_time")
        )
        metrics_df = metrics_df.join(
            max_run_time_df, on=["table_name", "run_time"], how="inner"
        )

    metrics_df = metrics_df.select(
        ["weekstart", "columns", "layer", "table_name", "master_mode"] + metrics
    )

    return metrics_df
