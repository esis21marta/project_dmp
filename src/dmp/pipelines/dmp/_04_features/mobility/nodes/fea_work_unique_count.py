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


def mobility_work_stay_unique_count_features(
    work_stay_point: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:

    # work stay point processing

    col_list = [
        "msisdn",
        "mo_id",
        "work1_kelurahan_name",
        "work1_neighboring_kelurahan_name",
        "work1_kecamatan_name",
        "work1_kabupaten_name",
        "work1_province_name",
        "month_mapped_dt",
    ]

    df_work_udf = (
        work_stay_point.where(f.col("mo_id") >= "2019-07").select(*col_list).distinct()
    )

    w_1month = get_rolling_window(1, key="msisdn", oby="month_mapped_dt")
    # w_2month = get_rolling_window(2, key="msisdn", oby="month_mapped_dt")
    w_3month = get_rolling_window(3, key="msisdn", oby="month_mapped_dt")
    w_6month = get_rolling_window(6, key="msisdn", oby="month_mapped_dt")
    # w_9month = get_rolling_window(9, key="msisdn", oby="month_mapped_dt")

    df_work_count = (
        df_work_udf.withColumn(
            "fea_mobility_kelurahan_work_cnt_distinct_01m",
            f.size(
                f.array_distinct(f.collect_list("work1_kelurahan_name").over(w_1month))
            ),
        )
        .withColumn(
            "fea_mobility_kecamatan_work_cnt_distinct_01m",
            f.size(
                f.array_distinct(f.collect_list("work1_kecamatan_name").over(w_1month))
            ),
        )
        .withColumn(
            "fea_mobility_kabupaten_work_cnt_distinct_01m",
            f.size(
                f.array_distinct(f.collect_list("work1_kabupaten_name").over(w_1month))
            ),
        )
        .withColumn(
            "fea_mobility_province_work_cnt_distinct_01m",
            f.size(
                f.array_distinct(f.collect_list("work1_province_name").over(w_1month))
            ),
        )
        # .withColumn(
        #     "fea_mobility_kelurahan_work_cnt_distinct_02m",
        #     f.size(
        #         f.array_distinct(f.collect_list("work1_kelurahan_name").over(w_2month))
        #     ),
        # )
        # .withColumn(
        #     "fea_mobility_kecamatan_work_cnt_distinct_02m",
        #     f.size(
        #         f.array_distinct(f.collect_list("work1_kecamatan_name").over(w_2month))
        #     ),
        # )
        # .withColumn(
        #     "fea_mobility_kabupaten_work_cnt_distinct_02m",
        #     f.size(
        #         f.array_distinct(f.collect_list("work1_kabupaten_name").over(w_2month))
        #     ),
        # )
        # .withColumn(
        #     "fea_mobility_province_work_cnt_distinct_02m",
        #     f.size(
        #         f.array_distinct(f.collect_list("work1_province_name").over(w_2month))
        #     ),
        # )
        .withColumn(
            "fea_mobility_kelurahan_work_cnt_distinct_03m",
            f.size(
                f.array_distinct(f.collect_list("work1_kelurahan_name").over(w_3month))
            ),
        )
        .withColumn(
            "fea_mobility_kecamatan_work_cnt_distinct_03m",
            f.size(
                f.array_distinct(f.collect_list("work1_kecamatan_name").over(w_3month))
            ),
        )
        .withColumn(
            "fea_mobility_kabupaten_work_cnt_distinct_03m",
            f.size(
                f.array_distinct(f.collect_list("work1_kabupaten_name").over(w_3month))
            ),
        )
        .withColumn(
            "fea_mobility_province_work_cnt_distinct_03m",
            f.size(
                f.array_distinct(f.collect_list("work1_province_name").over(w_3month))
            ),
        )
        .withColumn(
            "fea_mobility_kelurahan_work_cnt_distinct_06m",
            f.size(
                f.array_distinct(f.collect_list("work1_kelurahan_name").over(w_6month))
            ),
        )
        .withColumn(
            "fea_mobility_kecamatan_work_cnt_distinct_06m",
            f.size(
                f.array_distinct(f.collect_list("work1_kecamatan_name").over(w_6month))
            ),
        )
        .withColumn(
            "fea_mobility_kabupaten_work_cnt_distinct_06m",
            f.size(
                f.array_distinct(f.collect_list("work1_kabupaten_name").over(w_6month))
            ),
        )
        .withColumn(
            "fea_mobility_province_work_cnt_distinct_06m",
            f.size(
                f.array_distinct(f.collect_list("work1_province_name").over(w_6month))
            ),
        )
        # .withColumn(
        #     "fea_mobility_kelurahan_work_cnt_distinct_09m",
        #     f.size(
        #         f.array_distinct(f.collect_list("work1_kelurahan_name").over(w_9month))
        #     ),
        # )
        # .withColumn(
        #     "fea_mobility_kecamatan_work_cnt_distinct_09m",
        #     f.size(
        #         f.array_distinct(f.collect_list("work1_kecamatan_name").over(w_9month))
        #     ),
        # )
        # .withColumn(
        #     "fea_mobility_kabupaten_work_cnt_distinct_09m",
        #     f.size(
        #         f.array_distinct(f.collect_list("work1_kabupaten_name").over(w_9month))
        #     ),
        # )
        # .withColumn(
        #     "fea_mobility_province_work_cnt_distinct_09m",
        #     f.size(
        #         f.array_distinct(f.collect_list("work1_province_name").over(w_9month))
        #     ),
        # )
        .selectExpr(
            "msisdn",
            "mo_id",
            "fea_mobility_kelurahan_work_cnt_distinct_01m",
            "fea_mobility_kecamatan_work_cnt_distinct_01m",
            "fea_mobility_kabupaten_work_cnt_distinct_01m",
            "fea_mobility_province_work_cnt_distinct_01m",
            # "fea_mobility_kelurahan_work_cnt_distinct_02m",
            # "fea_mobility_kecamatan_work_cnt_distinct_02m",
            # "fea_mobility_kabupaten_work_cnt_distinct_02m",
            # "fea_mobility_province_work_cnt_distinct_02m",
            "fea_mobility_kelurahan_work_cnt_distinct_03m",
            "fea_mobility_kecamatan_work_cnt_distinct_03m",
            "fea_mobility_kabupaten_work_cnt_distinct_03m",
            "fea_mobility_province_work_cnt_distinct_03m",
            "fea_mobility_kelurahan_work_cnt_distinct_06m",
            "fea_mobility_kecamatan_work_cnt_distinct_06m",
            "fea_mobility_kabupaten_work_cnt_distinct_06m",
            "fea_mobility_province_work_cnt_distinct_06m",
            # "fea_mobility_kelurahan_work_cnt_distinct_09m",
            # "fea_mobility_kecamatan_work_cnt_distinct_09m",
            # "fea_mobility_kabupaten_work_cnt_distinct_09m",
            # "fea_mobility_province_work_cnt_distinct_09m",
        )
    )

    return df_work_count
