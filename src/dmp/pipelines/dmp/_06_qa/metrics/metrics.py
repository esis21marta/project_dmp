# Copyright 2018-present QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Dict, List

import pyspark
import pyspark.sql.functions as f
from pyspark.sql.types import StringType

from src.dmp.pipelines.dmp._06_qa.data_quality_helper.features_list import (
    get_features_used_list,
)
from src.dmp.pipelines.dmp._06_qa.metrics.accuracy_completeness import (
    get_accuracy_completeness_metrics,
    get_monthly_unique_msisdn,
    get_outlier,
)
from src.dmp.pipelines.dmp._06_qa.metrics.consistency import get_consistency
from utils import fillna_with_null


def get_calculate_qa_metrics_node_wrapper(file_path, layer, old_file_path=None):
    def get_calculate_qa_metrics_node(
        df: pyspark.sql.DataFrame,
        mode: str,
        df_sample_msisdn: pyspark.sql.DataFrame,
        qa_params: dict,
        df_old: pyspark.sql.DataFrame = None,
    ) -> Dict[str, pyspark.sql.DataFrame]:
        """
        Returns a dictionary of string an Dataframes to be interpreted by Kedro node.s
        Args:
            df: Input dataframe.
            mode: training/scoring node.
            df_sample_msisdn: Sample MSISDN Dataframe.
            qa_params: Parameters for QA.
            df_old: Old master dataframe for master layer.

        Returns:
            Dataframe.
        """

        percentiles = qa_params["percentile"]["percentiles"]
        percentiles_accuracy = qa_params["percentile"]["accuracy"]
        columns = None

        if layer == "master" and mode == "training":
            features_mode = qa_params["features_mode"]
            columns = get_features_used_list(df, features_mode)

        # In case of scoring mode/pipeline, don't run consistency/same_percent check.
        if mode == "scoring":
            df_old = None

        qa_metrics: Dict[str, pyspark.sql.DataFrame] = calculate_qa_metrics(
            df=df,
            df_sample_msisdn=df_sample_msisdn,
            percentiles=percentiles,
            percentiles_accuracy=percentiles_accuracy,
            df_old=df_old,
            columns=columns,
        )

        for key, qa_df in qa_metrics.items():
            qa_df = (
                qa_df.withColumn("layer", f.lit(layer).cast(StringType()))
                .withColumn("file_path", f.lit(file_path).cast(StringType()))
                .withColumn("old_file_path", f.lit(old_file_path).cast(StringType()))
                .withColumn("master_mode", f.lit(mode).cast(StringType()))
                .withColumn(
                    "table_name", f.when(f.col("layer") == "master", f.lit("master"))
                )
            )

            # Add run_time with max weekstart of the df on which qa check is running
            qa_df = qa_df.crossJoin(
                df.select(f.max(f.col("weekstart")).alias("run_time"))
            )
            qa_metrics.update({key: qa_df})

        return qa_metrics

    return get_calculate_qa_metrics_node


def calculate_qa_metrics(
    df: pyspark.sql.DataFrame,
    df_sample_msisdn: pyspark.sql.DataFrame,
    percentiles: List[float],
    percentiles_accuracy: float,
    df_old: pyspark.sql.DataFrame = None,
    columns: List[str] = None,
) -> Dict[str, pyspark.sql.DataFrame]:
    """
    Calculates QA Metrics for all dimensions.
    Args:
        df: Input dataframe.
        df_sample_msisdn: Sample MSISDN Dataframe.
        percentiles: Percentiles which we want to calculate.
        percentiles_accuracy: Accuracy for percentiles.
        df_old: Old master dataframe for master layer.
        columns: Columns to run metrics computation on.

    Returns:
        Dictionary of name and dataframe.
    """

    # Get accuracy #1 dimension
    monthly_unique_msisdn_df = get_monthly_unique_msisdn(df)

    # Drop Extra columns
    for column in ["use_case"]:
        if column in df.columns:
            df = df.drop(column)
        if df_old and column in df_old.columns:
            df_old = df_old.drop(column)

    # Filter dataframe
    df = df.join(df_sample_msisdn, how="inner", on="msisdn")
    df.cache()
    df = fillna_with_null(df)

    # Get accuracy #2 and completeness dimension
    acc_com_metrics_df = get_accuracy_completeness_metrics(
        df, percentiles, percentiles_accuracy, columns
    ).repartition(1)
    outliers_df = get_outlier(df, acc_com_metrics_df, columns)

    # Get consistency dimensions + join with accuracy and completeness domain
    if df_old is not None:
        df_old = fillna_with_null(df_old)
        consistency_df = get_consistency(df, df_old)
        acc_com_metrics_df = acc_com_metrics_df.join(
            consistency_df, on=["weekstart", "columns"], how="outer"
        )

    output = {
        "metric_output": acc_com_metrics_df.repartition(1),
        "outlier_output": outliers_df.repartition(1),
        "monthly_unique_msisdn_output": monthly_unique_msisdn_df.repartition(1),
    }

    return output
