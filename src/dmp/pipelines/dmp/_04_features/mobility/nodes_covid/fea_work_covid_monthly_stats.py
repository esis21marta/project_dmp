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
from pyspark.sql import functions as f


def mobility_work_covid_monthly_stats(
    work_agg: pyspark.sql.DataFrame, covid_agg: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:

    df_work = (
        work_agg.where(
            (f.col("mo_id") >= "2020-01") & (f.col("duration_max_flag") == 1)
        )
        .groupBy("msisdn", "mo_id")
        .agg(f.first(f.col("geohash")).alias("geohash"))
    )

    df_work_covid_info = df_work.join(
        (
            covid_agg.selectExpr(
                "mo_id",
                "suspect_avg as fea_mobility_work_suspect_avg",
                "positive_avg as fea_mobility_work_positive_avg",
                "probable_avg as fea_mobility_work_probable_avg",
                "dead_avg as fea_mobility_work_dead_avg",
                "suspect_max as fea_mobility_work_suspect_max",
                "positive_max as fea_mobility_work_positive_max",
                "probable_max as fea_mobility_work_probable_max",
                "dead_max as fea_mobility_work_dead_max",
                "geohash",
                "covid_cluster_flag as fea_mobility_work_covid_cluster_flag",
            )
        ),
        on=["geohash", "mo_id"],
    ).drop("geohash")

    return df_work_covid_info
