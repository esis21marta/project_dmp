from kedro.pipeline import Pipeline, node

from ..model_factory.preprocessing_nodes import remove_duplicate_msisdn_weekstart
from .nodes import (
    assign_bids_for_nuclear_pilot,
    assign_cluster_to_features,
    fit_clustering_pipeline,
    format_pilot_assignment,
    generate_target_calibration_cross_table,
    join_features_for_pilot_bid_assignment,
    prepare_data_for_clustering,
    summarize_clusters,
)


def create_target_calibration_pipeline():
    return Pipeline(
        [
            node(
                func=remove_duplicate_msisdn_weekstart,
                inputs=["master", "params:msisdn_column", "params:weekstart_column"],
                outputs="no_duplicates",
            ),
            node(
                func=generate_target_calibration_cross_table,
                inputs=[
                    "no_duplicates",
                    "params:target_calibration_column_sources",
                    "params:weekstart_column",
                    "params:msisdn_column",
                ],
                outputs=None,
            ),
        ]
    )


def prepare_data_for_nuclear_mapping():
    return Pipeline(
        [
            node(
                func=join_features_for_pilot_bid_assignment,
                inputs=[
                    "msisdn_scores",
                    "scoring_master",
                    "params:msisdn_column",
                    "params:scoring_date",
                    "params:weekstart_column",
                ],
                outputs="pilot_mapping_base",
            ),
        ]
    )


def create_nuclear_pilot_mapping():
    return Pipeline(
        [
            node(
                func=assign_bids_for_nuclear_pilot,
                inputs=[
                    "pilot_mapping_base",
                    "params:nuclear_pilot_conf",
                    "params:msisdn_column",
                ],
                outputs="pilot_bids",
            ),
            node(
                func=format_pilot_assignment,
                inputs=["pilot_bids", "bids_descriptions",],
                outputs="pilot_bids_full",
            ),
        ]
    )


def create_clustering_pipeline():
    return Pipeline(
        [
            node(
                func=prepare_data_for_clustering,
                inputs=[
                    "scoring_master",
                    "msisdn_scores",
                    "params:clustering_parameters",
                ],
                outputs="clustering_features",
            ),
            node(
                func=fit_clustering_pipeline,
                inputs=["clustering_features", "params:clustering_parameters",],
                outputs="clustering_pipeline",
            ),
            node(
                func=assign_cluster_to_features,
                inputs=["clustering_features", "clustering_pipeline",],
                outputs="clusters",
            ),
            node(
                func=summarize_clusters,
                inputs=["clusters", "params:clustering_parameters",],
                outputs="clusters_summary",
            ),
        ]
    )
