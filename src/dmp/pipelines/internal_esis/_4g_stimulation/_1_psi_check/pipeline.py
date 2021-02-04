from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.internal_esis._4g_stimulation._1_psi_check.nodes import (
    calculate_psi_all,
    calculate_psi_baseline,
    filter_psi_columns,
    psi_baseline_bining,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    Adding segments and network data to the master table

    :param kwargs: Ignore any additional arguments added in the future.

    :return: A Pipeline object
    """
    return Pipeline(
        [
            node(
                func=psi_baseline_bining,
                inputs=["4g_num_cols", "dmp_baseline",],
                outputs=["dmp_baseline_bining"],
                name="bining_psi",
            ),  # in are all tables and out are all tables from a certain month
            node(
                func=calculate_psi_baseline,
                inputs=["dmp_baseline_bining", "params:4g_num_unique"],
                outputs="psi_baseline",
                name="bining_psi_baseline",
            ),  # in are all tables and out are all tables from a certain month
            node(
                func=psi_baseline_bining,
                inputs=["4g_num_cols", "dmp_master",],
                outputs=["dmp_master_bining"],
                name="bining_psi_master",
            ),  # in are all tables and out are all tables from a certain month
            node(
                func=calculate_psi_all,
                inputs=["dmp_master_bining", "params:features_binning"],
                outputs="with_psi_master",
                name="bining_psi_master_all",
            ),
            node(
                func=filter_psi_columns,
                inputs=["with_psi_master", "params:columns_to_include"],
                outputs="with_psi_master_send",
                name="bining_psi_master_all_send",
            ),
        ]
    )
