from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.external.generate_psi_report.nodes import (
    calculate_psi_all,
    filter_psi_columns,
)


def create_pipeline():
    return Pipeline(
        [
            node(
                calculate_psi_all,
                ["with_score", "params:features_binning"],
                "with_psi",
            ),
            node(
                filter_psi_columns,
                ["with_psi", "params:columns_to_include"],
                "with_psi_send",
            ),
        ]
    )
