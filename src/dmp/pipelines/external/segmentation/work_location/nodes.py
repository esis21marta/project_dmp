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
import pyspark.sql.functions as F

from src.dmp.pipelines.external.segmentation.common.nodes import (
    get_cross_tabs,
    month_to_MM,
)


def clean_work_location(
    work_location: pyspark.sql.DataFrame, year: int, month: int
) -> pyspark.sql.DataFrame:
    """
    Selects relevant columns from the work location table and filters on the chosen year and month.

    :param work_location: Raw work location table
    :param year: Year for which all data is selected
    :param month: Month for which all data is selected
    :return: Cleaned work location table
    """

    month = month_to_MM(month)
    target_mo_id = f"{year}-{month}"

    work_location = (
        work_location.filter(F.col("mo_id") == target_mo_id)
        .withColumnRenamed("imsi", "msisdn")
        .drop_duplicates(subset=["msisdn"])
        .repartition(500)
    )

    columns_of_interest = [
        "msisdn",
        "work1_province_name",
        "work1_kabupaten_name",
    ]

    return work_location.select(columns_of_interest)


def get_work_location_cross_tabs(
    master_table: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Computes cross tabs grouping by work location data.

    :param master_table: Table containing kr and work location information
    :return: Cross tabs table
    """

    groups = [
        "work1_province_name",
        "work1_kabupaten_name",
    ]

    return get_cross_tabs(master_table, groups).coalesce(1)
