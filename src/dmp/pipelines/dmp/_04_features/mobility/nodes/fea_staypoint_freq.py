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


def mobility_staypoint_freq_features(
    staypoint: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    staypoint_agg = staypoint.selectExpr(
        "msisdn",
        "kelurahan_name",
        "kecamatan_name",
        "kabupaten_name",
        "province_name",
        "mo_id",
        "month_mapped_dt",
        "village_cnt",
    )

    df_staypoint_agg = (
        staypoint_agg.withColumn(
            "kelurahan_count_1_mnth",
            f.sum("village_cnt").over(
                get_rolling_window(
                    1,
                    key="msisdn",
                    oby="month_mapped_dt",
                    optional_keys=["kelurahan_name"],
                )
            ),
        )
        #     .withColumn(
        #     "kelurahan_count_2_mnth",
        #     f.sum("village_cnt").over(
        #         get_rolling_window(
        #             2,
        #             key="msisdn",
        #             oby="month_mapped_dt",
        #             optional_keys=["kelurahan_name"],
        #         )
        #     ),
        # )
        .withColumn(
            "kelurahan_count_3_mnth",
            f.sum("village_cnt").over(
                get_rolling_window(
                    3,
                    key="msisdn",
                    oby="month_mapped_dt",
                    optional_keys=["kelurahan_name"],
                )
            ),
        )
        .withColumn(
            "kelurahan_count_6_mnth",
            f.sum("village_cnt").over(
                get_rolling_window(
                    6,
                    key="msisdn",
                    oby="month_mapped_dt",
                    optional_keys=["kelurahan_name"],
                )
            ),
        )
        #     .withColumn(
        #     "kelurahan_count_9_mnth",
        #     f.sum("village_cnt").over(
        #         get_rolling_window(
        #             9,
        #             key="msisdn",
        #             oby="month_mapped_dt",
        #             optional_keys=["kelurahan_name"],
        #         )
        #     ),
        # )
        .withColumn(
            "kecamatan_count_1_mnth",
            f.sum("village_cnt").over(
                get_rolling_window(
                    1,
                    key="msisdn",
                    oby="month_mapped_dt",
                    optional_keys=["kecamatan_name"],
                )
            ),
        )
        #     .withColumn(
        #     "kecamatan_count_2_mnth",
        #     f.sum("village_cnt").over(
        #         get_rolling_window(
        #             2,
        #             key="msisdn",
        #             oby="month_mapped_dt",
        #             optional_keys=["kecamatan_name"],
        #         )
        #     ),
        # )
        .withColumn(
            "kecamatan_count_3_mnth",
            f.sum("village_cnt").over(
                get_rolling_window(
                    3,
                    key="msisdn",
                    oby="month_mapped_dt",
                    optional_keys=["kecamatan_name"],
                )
            ),
        )
        .withColumn(
            "kecamatan_count_6_mnth",
            f.sum("village_cnt").over(
                get_rolling_window(
                    6,
                    key="msisdn",
                    oby="month_mapped_dt",
                    optional_keys=["kecamatan_name"],
                )
            ),
        )
        #     .withColumn(
        #     "kecamatan_count_9_mnth",
        #     f.sum("village_cnt").over(
        #         get_rolling_window(
        #             9,
        #             key="msisdn",
        #             oby="month_mapped_dt",
        #             optional_keys=["kecamatan_name"],
        #         )
        #     ),
        # )
        .withColumn(
            "kabupaten_count_1_mnth",
            f.sum("village_cnt").over(
                get_rolling_window(
                    1,
                    key="msisdn",
                    oby="month_mapped_dt",
                    optional_keys=["kabupaten_name"],
                )
            ),
        )
        #     .withColumn(
        #     "kabupaten_count_2_mnth",
        #     f.sum("village_cnt").over(
        #         get_rolling_window(
        #             2,
        #             key="msisdn",
        #             oby="month_mapped_dt",
        #             optional_keys=["kabupaten_name"],
        #         )
        #     ),
        # )
        .withColumn(
            "kabupaten_count_3_mnth",
            f.sum("village_cnt").over(
                get_rolling_window(
                    3,
                    key="msisdn",
                    oby="month_mapped_dt",
                    optional_keys=["kabupaten_name"],
                )
            ),
        )
        .withColumn(
            "kabupaten_count_6_mnth",
            f.sum("village_cnt").over(
                get_rolling_window(
                    6,
                    key="msisdn",
                    oby="month_mapped_dt",
                    optional_keys=["kabupaten_name"],
                )
            ),
        )
        #     .withColumn(
        #     "kabupaten_count_9_mnth",
        #     f.sum("village_cnt").over(
        #         get_rolling_window(
        #             9,
        #             key="msisdn",
        #             oby="month_mapped_dt",
        #             optional_keys=["kabupaten_name"],
        #         )
        #     ),
        # )
        .withColumn(
            "province_count_1_mnth",
            f.sum("village_cnt").over(
                get_rolling_window(
                    1,
                    key="msisdn",
                    oby="month_mapped_dt",
                    optional_keys=["province_name"],
                )
            ),
        )
        #     .withColumn(
        #     "province_count_2_mnth",
        #     f.sum("village_cnt").over(
        #         get_rolling_window(
        #             2,
        #             key="msisdn",
        #             oby="month_mapped_dt",
        #             optional_keys=["province_name"],
        #         )
        #     ),
        # )
        .withColumn(
            "province_count_3_mnth",
            f.sum("village_cnt").over(
                get_rolling_window(
                    3,
                    key="msisdn",
                    oby="month_mapped_dt",
                    optional_keys=["province_name"],
                )
            ),
        )
        .withColumn(
            "province_count_6_mnth",
            f.sum("village_cnt").over(
                get_rolling_window(
                    6,
                    key="msisdn",
                    oby="month_mapped_dt",
                    optional_keys=["province_name"],
                )
            ),
        )
        #     .withColumn(
        #     "province_count_9_mnth",
        #     f.sum("village_cnt").over(
        #         get_rolling_window(
        #             9,
        #             key="msisdn",
        #             oby="month_mapped_dt",
        #             optional_keys=["province_name"],
        #         )
        #     ),
        # )
    )

    df_staypoint_max = df_staypoint_agg.groupBy("msisdn", "mo_id").agg(
        f.max(f.col("kelurahan_count_1_mnth")).alias("kelurahan_count_1_mnth"),
        # f.max(f.col("kelurahan_count_2_mnth")).alias("kelurahan_count_2_mnth"),
        f.max(f.col("kelurahan_count_3_mnth")).alias("kelurahan_count_3_mnth"),
        f.max(f.col("kelurahan_count_6_mnth")).alias("kelurahan_count_6_mnth"),
        # f.max(f.col("kelurahan_count_9_mnth")).alias("kelurahan_count_9_mnth"),
        f.max(f.col("kecamatan_count_1_mnth")).alias("kecamatan_count_1_mnth"),
        # f.max(f.col("kecamatan_count_2_mnth")).alias("kecamatan_count_2_mnth"),
        f.max(f.col("kecamatan_count_3_mnth")).alias("kecamatan_count_3_mnth"),
        f.max(f.col("kecamatan_count_6_mnth")).alias("kecamatan_count_6_mnth"),
        # f.max(f.col("kecamatan_count_9_mnth")).alias("kecamatan_count_9_mnth"),
        f.max(f.col("kabupaten_count_1_mnth")).alias("kabupaten_count_1_mnth"),
        # f.max(f.col("kabupaten_count_2_mnth")).alias("kabupaten_count_2_mnth"),
        f.max(f.col("kabupaten_count_3_mnth")).alias("kabupaten_count_3_mnth"),
        f.max(f.col("kabupaten_count_6_mnth")).alias("kabupaten_count_6_mnth"),
        # f.max(f.col("kabupaten_count_9_mnth")).alias("kabupaten_count_9_mnth"),
        f.max(f.col("province_count_1_mnth")).alias("province_count_1_mnth"),
        # f.max(f.col("province_count_2_mnth")).alias("province_count_2_mnth"),
        f.max(f.col("province_count_3_mnth")).alias("province_count_3_mnth"),
        f.max(f.col("province_count_6_mnth")).alias("province_count_6_mnth"),
        # f.max(f.col("province_count_9_mnth")).alias("province_count_9_mnth"),
    )

    df_staypoint_kelurahan_1_mnth = (
        df_staypoint_agg.select(
            "msisdn", "kelurahan_name", "mo_id", "kelurahan_count_1_mnth"
        )
        .join(
            df_staypoint_max.select("msisdn", "mo_id", "kelurahan_count_1_mnth"),
            on=["msisdn", "mo_id", "kelurahan_count_1_mnth"],
        )
        .groupBy("msisdn", "mo_id", "kelurahan_count_1_mnth")
        .agg(f.first("kelurahan_name").alias("kelurahan_name_mnth_1"))
    )

    # df_staypoint_kelurahan_2_mnth = (
    #     df_staypoint_agg.select(
    #         "msisdn", "kelurahan_name", "mo_id", "kelurahan_count_2_mnth"
    #     )
    #         .join(
    #         df_staypoint_max.select("msisdn", "mo_id", "kelurahan_count_2_mnth"),
    #         on=["msisdn", "mo_id", "kelurahan_count_2_mnth"],
    #     )
    #         .groupBy("msisdn", "mo_id", "kelurahan_count_2_mnth")
    #         .agg(f.first("kelurahan_name").alias("kelurahan_name_mnth_2"))
    # )

    df_staypoint_kelurahan_3_mnth = (
        df_staypoint_agg.select(
            "msisdn", "kelurahan_name", "mo_id", "kelurahan_count_3_mnth"
        )
        .join(
            df_staypoint_max.select("msisdn", "mo_id", "kelurahan_count_3_mnth"),
            on=["msisdn", "mo_id", "kelurahan_count_3_mnth"],
        )
        .groupBy("msisdn", "mo_id", "kelurahan_count_3_mnth")
        .agg(f.first("kelurahan_name").alias("kelurahan_name_mnth_3"))
    )

    df_staypoint_kelurahan_6_mnth = (
        df_staypoint_agg.select(
            "msisdn", "kelurahan_name", "mo_id", "kelurahan_count_6_mnth"
        )
        .join(
            df_staypoint_max.select("msisdn", "mo_id", "kelurahan_count_6_mnth"),
            on=["msisdn", "mo_id", "kelurahan_count_6_mnth"],
        )
        .groupBy("msisdn", "mo_id", "kelurahan_count_6_mnth")
        .agg(f.first("kelurahan_name").alias("kelurahan_name_mnth_6"))
    )

    # df_staypoint_kelurahan_9_mnth = (
    #     df_staypoint_agg.select(
    #         "msisdn", "kelurahan_name", "mo_id", "kelurahan_count_9_mnth"
    #     )
    #         .join(
    #         df_staypoint_max.select("msisdn", "mo_id", "kelurahan_count_9_mnth"),
    #         on=["msisdn", "mo_id", "kelurahan_count_9_mnth"],
    #     )
    #         .groupBy("msisdn", "mo_id", "kelurahan_count_9_mnth")
    #         .agg(f.first("kelurahan_name").alias("kelurahan_name_mnth_9"))
    # )

    df_staypoint_kecamatan_1_mnth = (
        df_staypoint_agg.select(
            "msisdn", "kecamatan_name", "mo_id", "kecamatan_count_1_mnth"
        )
        .join(
            df_staypoint_max.select("msisdn", "mo_id", "kecamatan_count_1_mnth"),
            on=["msisdn", "mo_id", "kecamatan_count_1_mnth"],
        )
        .groupBy("msisdn", "mo_id", "kecamatan_count_1_mnth")
        .agg(f.first("kecamatan_name").alias("kecamatan_name_mnth_1"))
    )

    # df_staypoint_kecamatan_2_mnth = (
    #     df_staypoint_agg.select(
    #         "msisdn", "kecamatan_name", "mo_id", "kecamatan_count_2_mnth"
    #     )
    #         .join(
    #         df_staypoint_max.select("msisdn", "mo_id", "kecamatan_count_2_mnth"),
    #         on=["msisdn", "mo_id", "kecamatan_count_2_mnth"],
    #     )
    #         .groupBy("msisdn", "mo_id", "kecamatan_count_2_mnth")
    #         .agg(f.first("kecamatan_name").alias("kecamatan_name_mnth_2"))
    # )

    df_staypoint_kecamatan_3_mnth = (
        df_staypoint_agg.select(
            "msisdn", "kecamatan_name", "mo_id", "kecamatan_count_3_mnth"
        )
        .join(
            df_staypoint_max.select("msisdn", "mo_id", "kecamatan_count_3_mnth"),
            on=["msisdn", "mo_id", "kecamatan_count_3_mnth"],
        )
        .groupBy("msisdn", "mo_id", "kecamatan_count_3_mnth")
        .agg(f.first("kecamatan_name").alias("kecamatan_name_mnth_3"))
    )

    df_staypoint_kecamatan_6_mnth = (
        df_staypoint_agg.select(
            "msisdn", "kecamatan_name", "mo_id", "kecamatan_count_6_mnth"
        )
        .join(
            df_staypoint_max.select("msisdn", "mo_id", "kecamatan_count_6_mnth"),
            on=["msisdn", "mo_id", "kecamatan_count_6_mnth"],
        )
        .groupBy("msisdn", "mo_id", "kecamatan_count_6_mnth")
        .agg(f.first("kecamatan_name").alias("kecamatan_name_mnth_6"))
    )

    # df_staypoint_kecamatan_9_mnth = (
    #     df_staypoint_agg.select(
    #         "msisdn", "kecamatan_name", "mo_id", "kecamatan_count_9_mnth"
    #     )
    #         .join(
    #         df_staypoint_max.select("msisdn", "mo_id", "kecamatan_count_9_mnth"),
    #         on=["msisdn", "mo_id", "kecamatan_count_9_mnth"],
    #     )
    #         .groupBy("msisdn", "mo_id", "kecamatan_count_9_mnth")
    #         .agg(f.first("kecamatan_name").alias("kecamatan_name_mnth_9"))
    # )

    df_staypoint_kabupaten_1_mnth = (
        df_staypoint_agg.select(
            "msisdn", "kabupaten_name", "mo_id", "kabupaten_count_1_mnth"
        )
        .join(
            df_staypoint_max.select("msisdn", "mo_id", "kabupaten_count_1_mnth"),
            on=["msisdn", "mo_id", "kabupaten_count_1_mnth"],
        )
        .groupBy("msisdn", "mo_id", "kabupaten_count_1_mnth")
        .agg(f.first("kabupaten_name").alias("kabupaten_name_mnth_1"))
    )

    # df_staypoint_kabupaten_2_mnth = (
    #     df_staypoint_agg.select(
    #         "msisdn", "kabupaten_name", "mo_id", "kabupaten_count_2_mnth"
    #     )
    #         .join(
    #         df_staypoint_max.select("msisdn", "mo_id", "kabupaten_count_2_mnth"),
    #         on=["msisdn", "mo_id", "kabupaten_count_2_mnth"],
    #     )
    #         .groupBy("msisdn", "mo_id", "kabupaten_count_2_mnth")
    #         .agg(f.first("kabupaten_name").alias("kabupaten_name_mnth_2"))
    # )

    df_staypoint_kabupaten_3_mnth = (
        df_staypoint_agg.select(
            "msisdn", "kabupaten_name", "mo_id", "kabupaten_count_3_mnth"
        )
        .join(
            df_staypoint_max.select("msisdn", "mo_id", "kabupaten_count_3_mnth"),
            on=["msisdn", "mo_id", "kabupaten_count_3_mnth"],
        )
        .groupBy("msisdn", "mo_id", "kabupaten_count_3_mnth")
        .agg(f.first("kabupaten_name").alias("kabupaten_name_mnth_3"))
    )

    df_staypoint_kabupaten_6_mnth = (
        df_staypoint_agg.select(
            "msisdn", "kabupaten_name", "mo_id", "kabupaten_count_6_mnth"
        )
        .join(
            df_staypoint_max.select("msisdn", "mo_id", "kabupaten_count_6_mnth"),
            on=["msisdn", "mo_id", "kabupaten_count_6_mnth"],
        )
        .groupBy("msisdn", "mo_id", "kabupaten_count_6_mnth")
        .agg(f.first("kabupaten_name").alias("kabupaten_name_mnth_6"))
    )

    # df_staypoint_kabupaten_9_mnth = (
    #     df_staypoint_agg.select(
    #         "msisdn", "kabupaten_name", "mo_id", "kabupaten_count_9_mnth"
    #     )
    #         .join(
    #         df_staypoint_max.select("msisdn", "mo_id", "kabupaten_count_9_mnth"),
    #         on=["msisdn", "mo_id", "kabupaten_count_9_mnth"],
    #     )
    #         .groupBy("msisdn", "mo_id", "kabupaten_count_9_mnth")
    #         .agg(f.first("kabupaten_name").alias("kabupaten_name_mnth_9"))
    # )

    df_staypoint_province_1_mnth = (
        df_staypoint_agg.select(
            "msisdn", "province_name", "mo_id", "province_count_1_mnth"
        )
        .join(
            df_staypoint_max.select("msisdn", "mo_id", "province_count_1_mnth"),
            on=["msisdn", "mo_id", "province_count_1_mnth"],
        )
        .groupBy("msisdn", "mo_id", "province_count_1_mnth")
        .agg(f.first("province_name").alias("province_name_mnth_1"))
    )

    # df_staypoint_province_2_mnth = (
    #     df_staypoint_agg.select(
    #         "msisdn", "province_name", "mo_id", "province_count_2_mnth"
    #     )
    #         .join(
    #         df_staypoint_max.select("msisdn", "mo_id", "province_count_2_mnth"),
    #         on=["msisdn", "mo_id", "province_count_2_mnth"],
    #     )
    #         .groupBy("msisdn", "mo_id", "province_count_2_mnth")
    #         .agg(f.first("province_name").alias("province_name_mnth_2"))
    # )

    df_staypoint_province_3_mnth = (
        df_staypoint_agg.select(
            "msisdn", "province_name", "mo_id", "province_count_3_mnth"
        )
        .join(
            df_staypoint_max.select("msisdn", "mo_id", "province_count_3_mnth"),
            on=["msisdn", "mo_id", "province_count_3_mnth"],
        )
        .groupBy("msisdn", "mo_id", "province_count_3_mnth")
        .agg(f.first("province_name").alias("province_name_mnth_3"))
    )

    df_staypoint_province_6_mnth = (
        df_staypoint_agg.select(
            "msisdn", "province_name", "mo_id", "province_count_6_mnth"
        )
        .join(
            df_staypoint_max.select("msisdn", "mo_id", "province_count_6_mnth"),
            on=["msisdn", "mo_id", "province_count_6_mnth"],
        )
        .groupBy("msisdn", "mo_id", "province_count_6_mnth")
        .agg(f.first("province_name").alias("province_name_mnth_6"))
    )

    # df_staypoint_province_9_mnth = (
    #     df_staypoint_agg.select(
    #         "msisdn", "province_name", "mo_id", "province_count_9_mnth"
    #     )
    #         .join(
    #         df_staypoint_max.select("msisdn", "mo_id", "province_count_9_mnth"),
    #         on=["msisdn", "mo_id", "province_count_9_mnth"],
    #     )
    #         .groupBy("msisdn", "mo_id", "province_count_9_mnth")
    #         .agg(f.first("province_name").alias("province_name_mnth_9"))
    # )

    df_final = (
        df_staypoint_kelurahan_1_mnth
        # .join(df_staypoint_kelurahan_2_mnth, on=["msisdn", "mo_id"])
        .join(df_staypoint_kelurahan_3_mnth, on=["msisdn", "mo_id"])
        .join(df_staypoint_kelurahan_6_mnth, on=["msisdn", "mo_id"])
        # .join(df_staypoint_kelurahan_9_mnth, on=["msisdn", "mo_id"])
        .join(df_staypoint_kecamatan_1_mnth, on=["msisdn", "mo_id"])
        # .join(df_staypoint_kecamatan_2_mnth, on=["msisdn", "mo_id"])
        .join(df_staypoint_kecamatan_3_mnth, on=["msisdn", "mo_id"])
        .join(df_staypoint_kecamatan_6_mnth, on=["msisdn", "mo_id"])
        # .join(df_staypoint_kecamatan_9_mnth, on=["msisdn", "mo_id"])
        .join(df_staypoint_kabupaten_1_mnth, on=["msisdn", "mo_id"])
        # .join(df_staypoint_kabupaten_2_mnth, on=["msisdn", "mo_id"])
        .join(df_staypoint_kabupaten_3_mnth, on=["msisdn", "mo_id"])
        .join(df_staypoint_kabupaten_6_mnth, on=["msisdn", "mo_id"])
        # .join(df_staypoint_kabupaten_9_mnth, on=["msisdn", "mo_id"])
        .join(df_staypoint_province_1_mnth, on=["msisdn", "mo_id"])
        # .join(df_staypoint_province_2_mnth, on=["msisdn", "mo_id"])
        .join(df_staypoint_province_3_mnth, on=["msisdn", "mo_id"])
        .join(df_staypoint_province_6_mnth, on=["msisdn", "mo_id"])
        # .join(df_staypoint_province_9_mnth, on=["msisdn", "mo_id"])
        .selectExpr(
            "msisdn",
            "mo_id",
            "kelurahan_count_1_mnth as fea_mobility_staypoint_kelurahan_cnt_01m",
            "kelurahan_name_mnth_1 as fea_mobility_staypoint_kelurahan_name_01m",
            "kelurahan_count_3_mnth as fea_mobility_staypoint_kelurahan_cnt_03m",
            "kelurahan_name_mnth_3 as fea_mobility_staypoint_kelurahan_name_03m",
            "kelurahan_count_6_mnth as fea_mobility_staypoint_kelurahan_cnt_06m",
            "kelurahan_name_mnth_6 as fea_mobility_staypoint_kelurahan_name_06m",
            "kecamatan_count_1_mnth as fea_mobility_staypoint_kecamatan_cnt_01m",
            "kecamatan_name_mnth_1 as fea_mobility_staypoint_kecamatan_name_01m",
            "kecamatan_count_3_mnth as fea_mobility_staypoint_kecamatan_cnt_03m",
            "kecamatan_name_mnth_3 as fea_mobility_staypoint_kecamatan_name_03m",
            "kecamatan_count_6_mnth as fea_mobility_staypoint_kecamatan_cnt_06m",
            "kecamatan_name_mnth_6 as fea_mobility_staypoint_kecamatan_name_06m",
            "kabupaten_count_1_mnth as fea_mobility_staypoint_kabupaten_cnt_01m",
            "kabupaten_name_mnth_1 as fea_mobility_staypoint_kabupaten_name_01m",
            "kabupaten_count_3_mnth as fea_mobility_staypoint_kabupaten_cnt_03m",
            "kabupaten_name_mnth_3 as fea_mobility_staypoint_kabupaten_name_03m",
            "kabupaten_count_6_mnth as fea_mobility_staypoint_kabupaten_cnt_06m",
            "kabupaten_name_mnth_6 as fea_mobility_staypoint_kabupaten_name_06m",
            "province_count_1_mnth as fea_mobility_staypoint_province_cnt_01m",
            "province_name_mnth_1 as fea_mobility_staypoint_province_name_01m",
            "province_count_3_mnth as fea_mobility_staypoint_province_cnt_03m",
            "province_name_mnth_3 as fea_mobility_staypoint_province_name_03m",
            "province_count_6_mnth as fea_mobility_staypoint_province_cnt_06m",
            "province_name_mnth_6 as fea_mobility_staypoint_province_name_06m",
        )
    )

    return df_final
