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

from typing import List

import pyspark
from pyspark.sql.functions import col, lit, lower, trim
from pyspark.sql.types import StringType


def filter_user_on_territory(
    home_location: pyspark.sql.DataFrame,
    home_location_month_filter: str,
    territory_filter: List[str],
    home_days_threshold: int,
    territory_name: str,
    max_home_count: int,
) -> pyspark.sql.DataFrame:

    """
    Returns the MSISDN from filtered territory

    :param home_location: Name of the table of home location
    :param home_location_month_filter: Month of the home location data to be filtered
    :param territory_filter: A list containing the territory filter in kabupaten
    :param home_days_threshold: Threshold how many days the user is in the home location
    :param home_days_threshold: Threshold how many days the user is in the home location
    :param territory_name: The territory type to be filtered
    :param max_home_count: Maximum count of the home location information in the table
    :return: The samples on the filtered territory
    """

    home_location = home_location.filter(col("mo_id") == home_location_month_filter)
    home_ids = range(1, max_home_count + 1)
    home_location = home_location.withColumn("territory_filter", lit(False))
    for home_id in home_ids:
        home_location = home_location.withColumn(
            "territory_filter",
            col("territory_filter")
            | (
                (
                    trim(lower(col(f"home{home_id}_{territory_name}_name"))).isin(
                        territory_filter
                    )
                )
                & (col(f"home{home_id}_days") > home_days_threshold)
            ),
        )
    filtered_user = home_location.filter(col("territory_filter")).drop(
        "territory_filter"
    )
    filtered_user = filtered_user.withColumnRenamed("imsi", "msisdn")

    select_columns = ["msisdn"] + [
        f"home{id}_{suffix}" for id in home_ids for suffix in ["lat", "lon"]
    ]
    filtered_user = filtered_user.select(select_columns)

    for column in select_columns:
        filtered_user = filtered_user.withColumn(column, col(column).cast(StringType()))

    return filtered_user
