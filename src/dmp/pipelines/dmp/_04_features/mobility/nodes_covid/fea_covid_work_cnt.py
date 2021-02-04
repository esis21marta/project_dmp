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

from utils import get_rolling_window


def mobility_covid_work_cnt_features(
    work_stay_point: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    df_select = work_stay_point.select(
        "msisdn", "geohash", "mo_id", "month_mapped_dt"
    ).distinct()

    w_1month = get_rolling_window(1, key="msisdn", oby="month_mapped_dt")
    w_2month = get_rolling_window(2, key="msisdn", oby="month_mapped_dt")
    w_3month = get_rolling_window(3, key="msisdn", oby="month_mapped_dt")

    # count data from each mont and time range (every 3 month)
    df_raw = df_select.where(
        (df_select.mo_id > "2020-02") & (df_select.mo_id < "2020-08")
    ).withColumn("fea_work_cnt", f.size(f.collect_set("geohash").over(w_1month)))

    df_max = (
        df_raw.groupBy("msisdn")
        .pivot("mo_id")
        .agg(f.max("fea_work_cnt").alias("max_work_cnt"))
    )

    month_raw = (
        df_select.withColumn(
            "fea_work_cnt_345",
            f.when(
                f.col("mo_id").isin("2020-03"),
                f.size(f.array_distinct(f.collect_list("geohash").over(w_1month))),
            )
            .when(
                f.col("mo_id").isin("2020-04"),
                f.size(f.array_distinct(f.collect_list("geohash").over(w_2month))),
            )
            .when(
                f.col("mo_id").isin("2020-05"),
                f.size(f.array_distinct(f.collect_list("geohash").over(w_3month))),
            ),
        )
        .withColumn(
            "fea_work_cnt_456",
            f.when(
                f.col("mo_id").isin("2020-04"),
                f.size(f.array_distinct(f.collect_list("geohash").over(w_1month))),
            )
            .when(
                f.col("mo_id").isin("2020-05"),
                f.size(f.array_distinct(f.collect_list("geohash").over(w_2month))),
            )
            .when(
                f.col("mo_id").isin("2020-06"),
                f.size(f.array_distinct(f.collect_list("geohash").over(w_3month))),
            ),
        )
        .withColumn(
            "fea_work_cnt_567",
            f.when(
                f.col("mo_id").isin("2020-05"),
                f.size(f.array_distinct(f.collect_list("geohash").over(w_1month))),
            )
            .when(
                f.col("mo_id").isin("2020-06"),
                f.size(f.array_distinct(f.collect_list("geohash").over(w_2month))),
            )
            .when(
                f.col("mo_id").isin("2020-07"),
                f.size(f.array_distinct(f.collect_list("geohash").over(w_3month))),
            ),
        )
    )

    month_max = month_raw.groupBy("msisdn").agg(
        f.max("fea_work_cnt_345").alias("fea_work_cnt_345_max"),
        f.max("fea_work_cnt_456").alias("fea_work_cnt_456_max"),
        f.max("fea_work_cnt_567").alias("fea_work_cnt_567_max"),
    )

    month_join = (
        month_raw.join(month_max, on=["msisdn"])
        .join(df_max, on=["msisdn"])
        .withColumn(
            "fea_mobility_covid_work_cnt_mar_apr_may",
            f.when(f.col("mo_id") < "2020-06", f.col("fea_work_cnt_345")).otherwise(
                f.col("fea_work_cnt_345_max")
            ),
        )
        .withColumn(
            "fea_mobility_covid_work_cnt_apr_may_jun",
            f.when(f.col("mo_id") < "2020-07", f.col("fea_work_cnt_456")).otherwise(
                f.col("fea_work_cnt_456_max")
            ),
        )
        .withColumn(
            "fea_mobility_covid_work_cnt_may_jun_jul",
            f.when(f.col("mo_id") < "2020-08", f.col("fea_work_cnt_567")).otherwise(
                f.col("fea_work_cnt_567_max")
            ),
        )
        .withColumn(
            "fea_mobility_covid_work_cnt_mar",
            f.when(f.col("mo_id") > "2020-02", f.col("2020-03")),
        )
        .withColumn(
            "fea_mobility_covid_work_cnt_apr",
            f.when(f.col("mo_id") > "2020-03", f.col("2020-04")),
        )
        .withColumn(
            "fea_mobility_covid_work_cnt_may",
            f.when(f.col("mo_id") > "2020-04", f.col("2020-05")),
        )
        .withColumn(
            "fea_mobility_covid_work_cnt_jun",
            f.when(f.col("mo_id") > "2020-05", f.col("2020-06")),
        )
        .withColumn(
            "fea_mobility_covid_work_cnt_jul",
            f.when(f.col("mo_id") > "2020-06", f.col("2020-07")),
        )
        .select(
            "msisdn",
            "mo_id",
            "fea_mobility_covid_work_cnt_mar_apr_may",
            "fea_mobility_covid_work_cnt_apr_may_jun",
            "fea_mobility_covid_work_cnt_may_jun_jul",
            "fea_mobility_covid_work_cnt_mar",
            "fea_mobility_covid_work_cnt_apr",
            "fea_mobility_covid_work_cnt_may",
            "fea_mobility_covid_work_cnt_jun",
            "fea_mobility_covid_work_cnt_jul",
        )
        .distinct()
    )

    return month_join
