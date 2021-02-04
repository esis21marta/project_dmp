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

from typing import Any, Dict

import pyspark
from pyspark.sql.functions import col, date_format

from utils import previous_week_start_day


def unpack(
    de_output: pyspark.sql.DataFrame,
    raw_data: pyspark.sql.DataFrame,
    params_dict: Dict[str, Any],
    de_output_date_column: str,
    de_msisdn_column: str,
) -> pyspark.sql.DataFrame:
    """
    Finds the rows of de_output corresponding to the raw_data, selects them, adds the columns of interest and subselects on de_output_date_column when relevant.
    If date_column is specified in the params_dict, the output is subselected on msisdn and date, otherwise on msisdn only.

    :param de_output: table to unpack
    :param raw_data: raw data for a specific use case
    :param params_dict: dictionary containing information about columns in the raw_data
    :param de_output_date_column: date column in the table to unpack
    :param de_msisdn_column: msisdn column name expected by DE
    :return: data for a specific use case with features added in
    """

    output_partitions = params_dict["output_partitions"]
    msisdn_column_name = params_dict["msisdn_column"]
    other_columns_of_interest = params_dict.get("other_columns_of_interest", [])
    date_column_name = params_dict.get("date_column", {}).get("name", None)
    date_column_format = params_dict.get("date_column", {}).get("format", None)

    raw_data = raw_data.withColumnRenamed(msisdn_column_name, de_msisdn_column)

    if date_column_name is None:
        to_select = [de_msisdn_column] + other_columns_of_interest
        join_on = [de_msisdn_column]
    else:
        raw_data = raw_data.withColumn(
            de_output_date_column,
            previous_week_start_day(
                date_format(col(date_column_name), date_column_format)
            ),
        )
        to_select = (
            [de_msisdn_column] + other_columns_of_interest + [de_output_date_column]
        )
        join_on = [de_msisdn_column, de_output_date_column]
    return (
        raw_data.select(to_select)
        .join(de_output, on=join_on, how="inner")
        .repartition(output_partitions)
    )
