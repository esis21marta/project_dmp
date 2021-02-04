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

import pyspark.sql.functions as f
from pyspark.sql.window import Window


def prepare_ds_features_table(mck_int_mck_features_ds, features_to_select):

    return mck_int_mck_features_ds.withColumn(
        "week_start",
        f.date_format(f.date_add(f.to_date(f.col("trx_date")), 7), "yyyy-MM-DD"),
    ).select(["msisdn", "week_start"] + features_to_select["features"])


def prepare_target_var_table(l3_int_mck_payu_pkg_rev_grid_window_functions):

    return l3_int_mck_payu_pkg_rev_grid_window_functions.filter(
        (f.col("rev_window_pre_21_cnt") == 4) & (f.col("rev_window_post_8_28_cnt") == 4)
    )


def prepare_dmp_features_table(sh_mck_int_features_dmp, params):

    return (
        sh_mck_int_features_dmp.withColumn("partner_con", f.concat_ws("", "partner"))
        .filter(f.col("partner_con").contains("internal"))
        .withColumn("week_start", f.col("weekstart"))
        .select(["msisdn", "week_start", "partner"] + params["features_to_select"])
    )


def join_inputs_to_master(
    cmps_to_master, target_to_master, ds_features_to_master, dmp_features_to_master
):

    master = (
        cmps_to_master.join(target_to_master, ["msisdn", "week_start"], "left")
        .join(ds_features_to_master, ["msisdn", "week_start"], "left")
        .join(dmp_features_to_master, ["msisdn", "week_start"], "left")
        .repartition(1000)
    )

    return master


def removing_duplicats(quickwins_master):

    return (
        quickwins_master.withColumn(
            "row_number",
            f.row_number().over(
                Window.partitionBy(["msisdn", "week_start"]).orderBy(f.col("msisdn"))
            ),
        )
        .where(f.col("row_number") == 1)
        .repartition(1000)
    )
