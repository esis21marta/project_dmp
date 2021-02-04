from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.external.common.python.utils import spark_filter_down
from src.dmp.pipelines.external.generate_bin_thresholds.nodes import (
    calculate_bin_threshold,
)


def create_pipeline():
    return Pipeline(
        [
            node(spark_filter_down, ["master", "params:filter_query"], "master_filter"),
            node(
                calculate_bin_threshold,
                [
                    "master_filter",
                    "params:score_column_name",
                    "params:score_num_of_bins",
                ],
                "bin_threshold",
                name="calculate_psi_baseline",
            ),
        ]
    )
