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

import pyspark
import pyspark.sql.functions as f

from utils import get_column_name_of_type, join_all, melt


def convert_array_type_column_to_array_length(df: pyspark.sql.DataFrame):
    """
    Convert Array Type Column to Array Length

    Arga:
        df (pyspark.sql.DataFrame)

    Returns:
        df (pyspark.sql.DataFrame)
    """

    array_columns = get_column_name_of_type(df, ["array"])
    for col in array_columns:
        df = df.withColumn(col, f.size(col))
    return df


def convert_percentiles_list_into_separate_columns(
    qa_result_melted: pyspark.sql.DataFrame, percentiles: list
):
    """
    Convert percentiles list column to separate column for each percentile
    Args:
        df (pyspark.sql.DataFrame):
        percentiles (list):

    Returns:
        Dataframe with percentiles column replaces with separate percentile column
    """
    # Create different columns for each percentiles
    percentiles_col_names = [
        "percentile_" + str(percentile) for percentile in percentiles
    ]

    # Split percentiles array to multiple percentiles columns
    qa_result_melted_columns_list = qa_result_melted.columns
    qa_result_melted_columns_list.remove("percentiles")
    qa_result_melted = qa_result_melted.select(
        qa_result_melted_columns_list
        + [qa_result_melted.percentiles[i] for i in range(len(percentiles_col_names))]
    )

    for index, percentile in enumerate(percentiles):
        qa_result_melted = qa_result_melted.withColumnRenamed(
            f"percentiles[{index}]", f"percentile_{str(percentile).replace('.', '_')}"
        )
    return qa_result_melted


def melt_qa_result(qa_result: pyspark.sql.DataFrame):
    """
    Convert QA result into multiple rows such that feature columns are rows and columns are metrics
    Args:
        qa_result:

    Returns:
        Melted dataframe where columns are metrics used for QA Check
    """
    metrics = set([col.rsplit("__")[-1] for col in qa_result.columns if "__" in col])

    qa_check_aggregates = []

    for metric in metrics:
        cols = [col for col in qa_result.columns if col.endswith(metric)]
        qa_result_aggregate = qa_result.select(
            [f.col("weekstart")]
            + [f.col(c).alias(c.replace(f"__{metric}", "")) for c in cols]
        )
        result_melted = melt(
            df=qa_result_aggregate,
            id_vars=["weekstart"],
            value_vars=list(set(qa_result_aggregate.columns) - set(("weekstart",))),
            var_name="columns",
            value_name=metric,
        )
        qa_check_aggregates.append(result_melted)

    qa_result_melted = join_all(
        qa_check_aggregates, how="outer", on=["weekstart", "columns"]
    )

    return qa_result_melted
