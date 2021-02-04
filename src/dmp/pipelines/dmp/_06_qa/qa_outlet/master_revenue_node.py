import pyspark
import pyspark.sql.functions as f

from src.dmp.pipelines.dmp._06_qa.qa_outlet.outlet_accuracy_completeness import (
    get_accuracy_completeness_metrics,
)
from src.dmp.pipelines.dmp._06_qa.qa_outlet.outlet_outlier import get_outlier
from src.dmp.pipelines.dmp._06_qa.qa_outlet.outlet_qa_helper import (
    add_metadata_to_output_file,
)
from utils import get_config_parameters, next_week_start_day
from utils.spark_data_set_helper import get_catalog_file_path


def master_revenue_node_wrapper(
    project_context, revenue_catalog_name, old_revenue_catalog_name
):
    def master_revenue_node(
        df: pyspark.sql.DataFrame, df_old: pyspark.sql.DataFrame
    ) -> pyspark.sql.DataFrame:

        pipeline_mode = get_config_parameters(None, "parameters").get("pipeline")

        # Convert trx_date to weekstart
        df = df.withColumn("weekstart", next_week_start_day(f.col("trx_date")))

        df_old = df_old.withColumn("weekstart", next_week_start_day(f.col("trx_date")))

        accuracy_completeness_metrices = get_accuracy_completeness_metrics(
            df,
            aggregate_colum="weekstart",
            non_feature_columns=["outlet_id", "trx_date", "weekstart"],
            percentiles=["0.25", "0.75"],
            percentiles_accuracy=80,
        )

        outlier_metrics = get_outlier(
            df,
            aggregate_column="weekstart",
            df_metrics=accuracy_completeness_metrices,
            non_feature_columns=["outlet_id", "weekstart"],
        )

        file_path = get_catalog_file_path(revenue_catalog_name, project_context)
        old_file_path = get_catalog_file_path(old_revenue_catalog_name, project_context)
        table_name = "outlet_revenue"

        accuracy_completeness_metrices = add_metadata_to_output_file(
            accuracy_completeness_metrices,
            pipeline_mode,
            table_name,
            file_path,
            old_file_path,
        )
        outlier_metrics = add_metadata_to_output_file(
            outlier_metrics, pipeline_mode, table_name, file_path, old_file_path,
        )

        return {
            "accuracy_completeness_metrices": accuracy_completeness_metrices,
            "outlier_metrics": outlier_metrics,
        }

    return master_revenue_node
