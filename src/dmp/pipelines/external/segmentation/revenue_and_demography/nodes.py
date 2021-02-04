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
from src.dmp.pipelines.external.segmentation.demography.nodes import clean_demography


def clean_filter_demography(
    demography: pyspark.sql.DataFrame, year: int, month: int
) -> pyspark.sql.DataFrame:
    """
    Selects relevant columns from the demography table, extracts month and year columns and filters on the chosen year and month.

    :param demography: Raw demography table
    :param year: Year for which data is selected
    :param month: Month for which data is selected
    :return: Cleaned demography table
    """

    columns_of_interest = [
        "msisdn",
        "brand_type",
        "age",
        "occupation",
        "income_category",
    ]

    return clean_demography(demography, year, month).select(columns_of_interest)


def join_revenue_to_demography(
    demography: pyspark.sql.DataFrame, revenue: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    """
    Joins the cleaned demography table to the revenue bins table.

    :param demography: Cleaned demography table
    :param revenue: Revenue bins table
    :return: Two tables joined
    """

    return demography.join(revenue, on=["msisdn"], how="outer")


def get_revenue_and_demography_cross_tabs(
    master_table: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Computes cross tabs grouping by revenue and demography data.

    :param master_table: Table containing kr, demography and revenue information
    :return: Cross tabs table
    """

    groups = [
        "brand_type",
        "age",
        "occupation",
        "income_category",
        "revenue_binned",
    ]

    return get_cross_tabs(master_table, groups).coalesce(50)
