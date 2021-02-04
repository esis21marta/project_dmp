from kedro.pipeline import Pipeline, node

from src.dmp.pipelines.dmp._06_qa.reseller.inconsistent_value_checks import (
    get_additional_starting_outlet_numbers,
    track_outlet_filtering,
)
from src.dmp.pipelines.dmp._06_qa.reseller.missing_data_checks import (
    check_join_accuracies,
)
from src.dmp.pipelines.dmp._06_qa.reseller.time_series_continuity_checks import (
    check_digipos_time_series_continuity,
)


def create_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                func=check_join_accuracies,
                inputs=[
                    "l1_outlet_id_msisdn_mapping",
                    "l1_dfj_digipos_mkios",
                    "l1_dnkm",
                    "l1_urp_cleaned",
                ],
                outputs="qa_join_accuracies_results",
                name="check_join_accuracies",
            ),
            node(
                func=check_digipos_time_series_continuity,
                inputs="l1_digipos_outlet_reference_dd",
                outputs="qa_time_series_continuity_results",
                name="check_digipos_time_series_continuity",
            ),
            node(
                func=track_outlet_filtering,
                inputs="l1_all_filters",
                outputs="qa_de_reseller_outlet_filtering",
                name="track_outlet_filtering",
            ),
            node(
                func=get_additional_starting_outlet_numbers,
                inputs=["l1_outlet_digipos_daily_aggregated", "l1_dfj_digipos_mkios"],
                outputs="l1_additional_outlet_counts",
                name="qa_get_additional_starting_outlet_numbers",
            ),
        ],
        tags=["de_outlet_qa"],
    )
