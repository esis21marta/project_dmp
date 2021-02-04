import logging

import pyspark
import pyspark.sql.functions as f
from kedro.pipeline import node

from src.dmp.pipelines.dmp._06_qa.data_quality_helper.qa_score_helper import (
    add_dimension,
    add_domain_name,
    filter_data_for_scoring,
    get_run_time_df,
    get_table_domain_mapping,
)
from src.dmp.pipelines.dmp._06_qa.data_quality_helper.spark_df_to_oracle_db import (
    spark_df_to_oracle_db_wrapper,
)
from src.dmp.pipelines.dmp._06_qa.data_quality_helper.threshold_score import (
    get_threshold_score,
)
from utils import melt

logger = logging.getLogger(__name__)


def accuracy_completeness_score_wrapper(layer=None, table_name=None):
    def accuracy_completeness_score(
        metrics_df: pyspark.sql.DataFrame,
        master_mode: str,
        feature_domain_mapping,
        iqr_lookback_days: int,
        manual_threshold,
        dimension_mapping,
        params_qa,
    ):
        run_time_df = get_run_time_df(metrics_df)
        metrics = dimension_mapping["accuracy"] + dimension_mapping["completeness"]
        metrics_df = filter_data_for_scoring(
            metrics_df,
            master_mode=master_mode,
            metrics=metrics,
            layer=layer,
            table_name=table_name,
        )
        metrics_df_metled = melt(
            metrics_df,
            id_vars=["weekstart", "columns", "layer", "table_name", "master_mode",],
            value_vars=metrics,
            var_name="metric",
            value_name="value",
        )
        score_df = get_threshold_score(
            metrics_df_metled, iqr_lookback_days, manual_threshold
        )
        score_df = score_df.withColumn("unit", f.lit("percentage"))
        score_df = score_df.crossJoin(run_time_df)
        table_domain_mapping = get_table_domain_mapping(params_qa, layer)
        score_df = add_domain_name(
            metrics_df, score_df, feature_domain_mapping, table_domain_mapping
        )
        score_df = add_dimension(score_df, dimension_metric_mapping=dimension_mapping)
        score_df_aggregate = score_df.groupBy(
            "domain", "dimension", "layer", "run_time", "master_mode", "table_name"
        ).agg(f.mean(f.col("score")).alias("score"))
        score_df_aggregate = score_df_aggregate.withColumn("unit", f.lit("percentage"))

        return {
            "score_df": score_df,
            "score_df_aggregate": score_df_aggregate,
        }

    return accuracy_completeness_score


def get_accuracy_completeness_score_nodes():
    threshold_nodes = []
    for layer, table_name in [
        ("aggregation", None),
        ("feature", None),
        ("master", "master"),
        ("master", "outlet_geo"),
        ("master", "outlet_revenue"),
    ]:
        if table_name is None:
            layer_table_name = layer
        else:
            layer_table_name = f"{layer}_{table_name}"
        threshold_nodes += [
            node(
                func=accuracy_completeness_score_wrapper(layer, table_name),
                inputs={
                    "metrics_df": "l4_qa_metrics_db",
                    "master_mode": "params:pipeline",
                    "feature_domain_mapping": "params:feature_domain_mapping",
                    "iqr_lookback_days": "params:iqr_lookback_days",
                    "manual_threshold": "params:manual_threshold",
                    "dimension_mapping": "params:metrics.dimension_mapping",
                    "params_qa": f"params:qa_{layer}",
                },
                outputs={
                    "score_df": f"l4_qa_score_{layer_table_name}_accuracy_completeness",
                    "score_df_aggregate": f"l4_qa_score_{layer_table_name}_accuracy_completeness_aggregate",
                },
                name=f"de_qa_score_{layer_table_name}_accuracy_completeness",
                tags=[
                    "de_qa",
                    f"de_qa_{layer_table_name}",
                    f"de_qa_{layer_table_name}_threshold",
                    f"de_qa_{layer_table_name}_threshold_parquet",
                ],
            ),
            node(
                func=spark_df_to_oracle_db_wrapper(
                    f"l4_qa_score_{layer_table_name}_accuracy_completeness_db",
                    upsert=True,
                    primary_keys=["layer", "table_name", "run_time", "dimension"],
                ),
                inputs=f"l4_qa_score_{layer_table_name}_accuracy_completeness",
                outputs=f"l4_qa_score_{layer_table_name}_accuracy_completeness_db",
                name=f"de_qa_score_{layer_table_name}_accuracy_completeness_db",
                tags=[
                    "de_qa",
                    f"de_qa_{layer_table_name}",
                    f"de_qa_{layer_table_name}_threshold",
                    f"de_qa_{layer_table_name}_threshold_db",
                ],
            ),
            node(
                func=spark_df_to_oracle_db_wrapper(
                    f"l4_qa_score_{layer_table_name}_accuracy_completeness_aggregate_db",
                    upsert=True,
                    primary_keys=["layer", "table_name", "run_time", "dimension"],
                ),
                inputs=f"l4_qa_score_{layer_table_name}_accuracy_completeness_aggregate",
                outputs=f"l4_qa_score_{layer_table_name}_accuracy_completeness_aggregate_db",
                name=f"de_qa_score_{layer_table_name}_accuracy_completeness_aggregate_db",
                tags=[
                    "de_qa",
                    f"de_qa_{layer_table_name}",
                    f"de_qa_{layer_table_name}_threshold",
                    f"de_qa_{layer_table_name}_threshold_db",
                ],
            ),
        ]
    return threshold_nodes
