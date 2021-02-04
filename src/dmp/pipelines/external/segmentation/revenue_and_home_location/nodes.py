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

from src.dmp.pipelines.external.segmentation.common.nodes import get_cross_tabs


def join_revenue_to_home_location(
    home_location: pyspark.sql.DataFrame, revenue: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    """
    Joins the cleaned home_location table to the revenue bins table.

    :param home_location: Cleaned home_location table
    :param revenue: Revenue bins table
    :return: Two tables joined
    """

    return home_location.join(revenue, on=["msisdn"], how="outer")


def get_revenue_and_home_location_cross_tabs(
    master_table: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Computes cross tabs grouping by revenue and home_location data.

    :param master_table: Table containing kr, home_location and revenue information
    :return: Cross tabs table
    """

    groups = [
        "home1_province_name",
        "home1_kabupaten_name",
        "revenue_binned",
    ]

    return get_cross_tabs(master_table, groups).coalesce(50)
