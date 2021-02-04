from kedro.pipeline import Pipeline, node

from .prep_filtered_outlets import save_filtered_outlets_to_hive


def create_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                func=save_filtered_outlets_to_hive,
                inputs=[
                    "ra_filter_outlet_created_at_date",
                    "ra_filter_seven_consecutive_zero_sales_days",
                    "ra_filter_indo_coordinates",
                    "ra_filter_bottom_removed_outlets",
                    "ra_filter_top_removed_outlets",
                    "params:ra_master_table",
                ],
                outputs="ra_mck_int_filtered_outlets",
                name="ra_mck_int_save_filtered_outlets_to_hive",
            ),
        ],
    )
