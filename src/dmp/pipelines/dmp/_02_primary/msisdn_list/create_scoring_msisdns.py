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

import logging

import pyspark
import pyspark.sql.functions as f

from utils import get_config_parameters, get_end_date, get_start_date

log = logging.getLogger(__name__)


def create_scoring_msisdns(
    df_prepaid_data: pyspark.sql.DataFrame, df_postpaid_data: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    """
    Creates Partner MSISDNs file for scoring master table
    Args:
        df_prepaid_data: Prepaid Customer Data
        df_postpaid_data: Postpaid Customer Data

    Returns:
        All the active msisdns
    """

    start_date = get_start_date()
    end_date = get_end_date()

    conf_catalog = get_config_parameters(config="catalog")
    prepaid_load_args = conf_catalog["l1_prepaid_customers_data"]["load_args"]
    postpaid_load_args = conf_catalog["l1_postpaid_customers_data"]["load_args"]

    df_prepaid_data = df_prepaid_data.filter(
        f.col(prepaid_load_args["partition_column"]).between(
            start_date.strftime(prepaid_load_args["partition_date_format"]),
            end_date.strftime(prepaid_load_args["partition_date_format"]),
        )
    )
    df_postpaid_data = df_postpaid_data.filter(
        f.col(postpaid_load_args["partition_column"]).between(
            start_date.strftime(postpaid_load_args["partition_date_format"]),
            end_date.strftime(postpaid_load_args["partition_date_format"]),
        )
    )

    df_prepaid_data = df_prepaid_data.select("msisdn").distinct()
    df_postpaid_data = df_postpaid_data.select("msisdn").distinct()

    df_msisdn = df_postpaid_data.union(df_prepaid_data).distinct()

    return df_msisdn
