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

from utils import calc_distance_bw_2_lat_long, get_rolling_window


def mobility_covid_home_work_distance(
    home_agg: pyspark.sql.DataFrame, work_agg: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    # geo_encode_udf = f.udf(lambda x, y: encode(x, y, precision=5), t.StringType())

    df_home = (
        home_agg.where(
            (
                f.col("mo_id").isin(
                    "2020-01",
                    "2020-02",
                    "2020-03",
                    "2020-04",
                    "2020-05",
                    "2020-06",
                    "2020-07",
                    "2020-08",
                )
            )
            & (f.col("duration_max_flag") == 1)
        )
        .withColumnRenamed("geohash", "home_geohash")
        .selectExpr(
            "msisdn",
            "home_geohash",
            "mo_id",
            "home1_lat as lat1",
            "home1_lon as long1",
            "month_mapped_dt as home_dt",
        )
        .distinct()
    )

    df_work = (
        work_agg.where(
            (
                f.col("mo_id").isin(
                    "2020-01",
                    "2020-02",
                    "2020-03",
                    "2020-04",
                    "2020-05",
                    "2020-06",
                    "2020-07",
                    "2020-08",
                )
            )
            & (f.col("duration_max_flag") == 1)
        )
        .withColumnRenamed("geohash", "work_geohash")
        .selectExpr(
            "msisdn",
            "work_geohash",
            "mo_id",
            "work1_lat as lat2",
            "work1_lon as long2",
            "month_mapped_dt as work_dt",
        )
        .distinct()
    )

    df_join = df_home.join(
        df_work, on=["msisdn", "mo_id"], how="full_outer"
    ).withColumn("month_mapped_dt", f.coalesce(f.col("home_dt"), f.col("work_dt")))

    df_distance = calc_distance_bw_2_lat_long(df_join).selectExpr(
        "msisdn", "mo_id", "distance", "month_mapped_dt"
    )

    w_2month = get_rolling_window(2, key="msisdn", oby="month_mapped_dt")
    w_3month = get_rolling_window(3, key="msisdn", oby="month_mapped_dt")

    df_distance_updated = (
        df_distance.select("msisdn", "mo_id", "distance", "month_mapped_dt")
        .distinct()
        .withColumn(
            "pre_covid_distance",
            f.when(f.col("mo_id") == "2020-01", f.col("distance")).when(
                f.col("mo_id") == "2020-02", f.avg(f.col("distance")).over(w_2month)
            ),
        )
        .withColumn(
            "post_covid_distance_1",
            f.when(f.col("mo_id") == "2020-03", f.col("distance"))
            .when(f.col("mo_id") == "2020-04", f.avg(f.col("distance")).over(w_2month))
            .when(f.col("mo_id") == "2020-05", f.avg(f.col("distance")).over(w_3month)),
        )
        .withColumn(
            "post_covid_distance_2",
            f.when(f.col("mo_id") == "2020-06", f.col("distance"))
            .when(f.col("mo_id") == "2020-07", f.avg(f.col("distance")).over(w_2month))
            .when(f.col("mo_id") == "2020-08", f.avg(f.col("distance")).over(w_3month)),
        )
    )

    df_distance_pre_covid = df_distance_updated.where("mo_id = '2020-02'").selectExpr(
        "msisdn", "pre_covid_distance as pre_covid_distance_final"
    )

    df_distance_post_covid_1 = df_distance_updated.where(
        "mo_id = '2020-05'"
    ).selectExpr("msisdn", "post_covid_distance_1 as post_covid_distance_1_final")

    df_distance_post_covid_2 = df_distance_updated.where(
        "mo_id = '2020-08'"
    ).selectExpr("msisdn", "post_covid_distance_2 as post_covid_distance_2_final")

    df_final = (
        df_distance_updated.join(df_distance_pre_covid, on=["msisdn"], how="left")
        .join(df_distance_post_covid_1, on=["msisdn"], how="left")
        .join(df_distance_post_covid_2, on=["msisdn"], how="left")
        .withColumn(
            "fea_mobility_pre_covid_avg_home_work_distance",
            f.when(f.col("mo_id") < "2020-02", f.col("pre_covid_distance")).otherwise(
                f.col("pre_covid_distance_final")
            ),
        )
        .withColumn(
            "fea_mobility_covid_mar_apr_may_avg_home_work_distance",
            f.when(
                f.col("mo_id") < "2020-05", f.col("post_covid_distance_1")
            ).otherwise(f.col("post_covid_distance_1_final")),
        )
        .withColumn(
            "fea_mobility_covid_jun_jul_aug_avg_home_work_distance",
            f.when(
                f.col("mo_id") < "2020-08", f.col("post_covid_distance_2")
            ).otherwise(f.col("post_covid_distance_2_final")),
        )
        .withColumn(
            "fea_mobility_home_work_distance_diff_pre_post_covid_1",
            f.when(
                f.col("mo_id") > "2020-02",
                (
                    f.col("fea_mobility_covid_mar_apr_may_avg_home_work_distance")
                    - f.col("fea_mobility_pre_covid_avg_home_work_distance")
                ),
            ),
        )
        .withColumn(
            "fea_mobility_home_work_distance_diff_pre_post_covid_2",
            f.when(
                f.col("mo_id") > "2020-05",
                (
                    f.col("fea_mobility_covid_jun_jul_aug_avg_home_work_distance")
                    - f.col("fea_mobility_pre_covid_avg_home_work_distance")
                ),
            ),
        )
        .select(
            "msisdn",
            "mo_id",
            "fea_mobility_pre_covid_avg_home_work_distance",
            "fea_mobility_covid_mar_apr_may_avg_home_work_distance",
            "fea_mobility_covid_jun_jul_aug_avg_home_work_distance",
            "fea_mobility_home_work_distance_diff_pre_post_covid_1",
            "fea_mobility_home_work_distance_diff_pre_post_covid_2",
        )
    )

    return df_final
