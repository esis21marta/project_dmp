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

from functools import reduce

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit


def get_mondays_multidim(mck_t_cb_multidim, if_repartition=True):

    daterange = pd.date_range("2019-06-03", "2019-12-31", freq="7d", closed="left")
    mondays = [i.strftime("%Y-%m-%d") for i in daterange]

    mck_int_features_multidim_mondays = mck_t_cb_multidim.filter(
        col("trx_date").isin(mondays)
    )  # .filter(col("event_date").isin(['2019-08-13']))

    if if_repartition:
        mck_int_features_multidim_mondays = mck_int_features_multidim_mondays.repartition(
            1000
        )

    return mck_int_features_multidim_mondays


def unionAll(*dfs):
    return reduce(DataFrame.union, dfs)


def prepare_multidim(mck_int_features_multidim_mondays, params):

    df_out = mck_int_features_multidim_mondays.select(
        ["msisdn", "trx_date"] + params["cols_to_select"] + ["event_date"]
    ).withColumn(
        "yearmonth",
        F.concat(
            mck_int_features_multidim_mondays.trx_date.substr(0, 4).cast("string"),
            mck_int_features_multidim_mondays.trx_date.substr(6, 2).cast("string"),
        ),
    )

    return df_out


def union_tables(params, *tbls):

    tbls = dict(zip(params["table_names"], tbls))

    tbls_mod = []
    for key, value in tbls.items():
        tbls_mod.append(
            value.select(["msisdn"] + params["cols_to_select"]).withColumn(
                "yearmonth", lit(str(int(key[-6:]) + 1))
            )
        )

    tbls_unioned = unionAll(*tbls_mod)

    return tbls_unioned


def join_tables(
    mck_int_features_multidim_mondays_filtered,
    mck_int_features_mytsel_unioned,
    mck_int_features_digital_unioned,
    mck_int_features_smartphone_unioned,
    mck_int_features_lte_unioned,
):

    keys = ["msisdn", "yearmonth"]

    tbls_joined = (
        mck_int_features_multidim_mondays_filtered.join(
            mck_int_features_mytsel_unioned, keys, how="left"
        )
        .join(mck_int_features_digital_unioned, keys, how="left")
        .join(mck_int_features_smartphone_unioned, keys, how="left")
        .join(mck_int_features_lte_unioned, keys, how="left")
    )

    return tbls_joined
