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


def mobility_covid_home_work_not_same(
    home_agg: pyspark.sql.DataFrame, work_agg: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    df_home = (
        home_agg.where(
            (f.col("mo_id").isin("2020-03", "2020-04", "2020-05"))
            & (f.col("duration_max_flag") == 1)
        )
        .withColumnRenamed("geohash", "home_geohash")
        .select("msisdn", "home_geohash", "mo_id")
        .distinct()
    )

    df_work = (
        work_agg.where(
            (f.col("mo_id").isin("2020-03", "2020-04", "2020-05"))
            & (f.col("duration_max_flag") == 1)
        )
        .withColumnRenamed("geohash", "work_geohash")
        .select("msisdn", "work_geohash", "mo_id")
        .distinct()
    )

    df_join = (
        df_home.join(df_work, on=["msisdn", "mo_id"], how="full_outer")
        .withColumn(
            "home_work_not_same_flag",
            f.when(f.col("home_geohash") == f.col("work_geohash"), 0).otherwise(1),
        )
        .distinct()
    )

    df_join_final = (
        df_join.groupBy("msisdn")
        .pivot("mo_id")
        .agg(f.sum(f.col("home_work_not_same_flag")))
        .join(home_agg.select("msisdn", "mo_id").distinct(), on=["msisdn"])
        .withColumn(
            "fea_mobility_covid_home_work_not_same_count_2020_03",
            f.when(f.col("mo_id") > "2020-02", f.col("2020-03")),
        )
        .withColumn(
            "fea_mobility_covid_home_work_not_same_count_2020_04",
            f.when(f.col("mo_id") > "2020-03", f.col("2020-04")),
        )
        .withColumn(
            "fea_mobility_covid_home_work_not_same_count_2020_05",
            f.when(f.col("mo_id") > "2020-04", f.col("2020-05")),
        )
        .withColumn(
            "fea_mobility_covid_mar_apr_may_home_work_not_same_flag",
            f.when(
                f.coalesce(
                    f.col("fea_mobility_covid_home_work_not_same_count_2020_03"),
                    f.col("fea_mobility_covid_home_work_not_same_count_2020_04"),
                    f.col("fea_mobility_covid_home_work_not_same_count_2020_05"),
                )
                > 0,
                1,
            ),
        )
        .select(
            "msisdn",
            "mo_id",
            "fea_mobility_covid_home_work_not_same_count_2020_03",
            "fea_mobility_covid_home_work_not_same_count_2020_04",
            "fea_mobility_covid_home_work_not_same_count_2020_05",
            "fea_mobility_covid_mar_apr_may_home_work_not_same_flag",
        )
    )

    return df_join_final
