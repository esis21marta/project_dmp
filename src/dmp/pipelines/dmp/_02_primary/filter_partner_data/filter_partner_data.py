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
import datetime

import pyspark
import pyspark.sql.functions as f


def filter_partner_data(
    df_partner_msisdn: pyspark.sql.DataFrame,
    df_data: pyspark.sql.DataFrame,
    filter_data: bool,
) -> pyspark.sql.DataFrame:
    """
    Filters out data of `df_data` for MSISDN in `df_partner_msisdn`
    It is controlled by the `filter_data` parameter.
        If filter_data = True, then data is filtered out for df_partner_msisdn
        Else: No filtering is done
    Args:
        df_partner_msisdn: Partner MSISDNs DataFrame
        df_data: Overall Data DataFrame
        filter_data: Flag for filtering Partner Data

    Returns:
        filtered `df_data`
    """

    if filter_data:
        # Get all MSISDNs
        df_partner_msisdn = df_partner_msisdn.select("msisdn")

        # Filter records only for partner MSISDNs
        df_data = df_data.join(df_partner_msisdn, ["msisdn"], how="inner")
    return df_data


def filter_partner_data_fixed_weekstart(
    df_partner_msisdn: pyspark.sql.DataFrame,
    df_data: pyspark.sql.DataFrame,
    filter_data: bool,
    first_weekstart: datetime.date,
    last_weekstart: datetime.date,
):
    """
    Filters out data of `df_data` for the fixed weekstart
    for MSISDN in `df_partner_msisdn`
    It is controlled by the `filter_data` parameter.
        If filter_data = True, then data is filtered out for df_partner_msisdn
        Else: No filtering is done
    Args:
        df_partner_msisdn: Partner MSISDNs DataFrame
        df_data: Overall Data DataFrame
        filter_data: Flag for filtering Partner Data
        first_weekstart: First weekstart
        last_weekstart: Last weekstart

    Returns:
        filtered `df_data`
    """
    first_weekstart = first_weekstart.strftime("%Y-%m-%d")
    last_weekstart = last_weekstart.strftime("%Y-%m-%d")

    df_data = df_data.filter(
        f.col("weekstart").between(first_weekstart, last_weekstart)
    )
    return filter_partner_data(df_partner_msisdn, df_data, filter_data)
