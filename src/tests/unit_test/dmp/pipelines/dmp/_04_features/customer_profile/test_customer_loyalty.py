import pyspark.sql.functions as f
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import from_unixtime, unix_timestamp

from src.dmp.pipelines.dmp._04_features.customer_profile.customer_loyalty import (
    get_loyalty_points_usage_df,
    get_usage_prep_df,
)


class TestCustomerLoyalty:
    def test_usage_prep_sanity(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        usage_prep = sc.parallelize(
            [
                ("111", "2019-10-01", 100),
                ("111", "2019-10-02", 110),
                ("111", "2019-10-03", 100),
                ("111", "2019-10-04", 90),
                ("222", "2019-10-03", 10),
                ("222", "2019-10-04", 50),
            ]
        )
        usage_prep_df = spark_session.createDataFrame(
            usage_prep.map(
                lambda x: Row(msisdn=x[0], trx_date=x[1], tsel_poin=int(x[2]))
            )
        ).select(
            "msisdn",
            from_unixtime(unix_timestamp(f.col("trx_date"), "yyyy-MM-dd")).alias(
                "trx_date"
            ),
            "tsel_poin",
        )

        out = get_usage_prep_df(usage_prep_df)
        out_list = [
            [i[0], i[1].strftime("%Y-%m-%d"), i[2], i[3], i[4]] for i in out.collect()
        ]

        assert out.columns == [
            "msisdn",
            "trx_date",
            "tsel_poin",
            "prev_tsel_poin",
            "points_difference",
        ]
        assert len(out_list) == 6
        assert sorted(out_list) == [
            ["111", "2019-10-01", 100, None, None],
            ["111", "2019-10-02", 110, 100, 10],
            ["111", "2019-10-03", 100, 110, -10],
            ["111", "2019-10-04", 90, 100, -10],
            ["222", "2019-10-03", 10, None, None],
            ["222", "2019-10-04", 50, 10, 40],
        ]

    def test_loyalty_points_usage_sanity(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        loyalty_df = sc.parallelize(
            [
                ("111", "2019-09-01", None),
                ("111", "2019-09-30", 10),
                ("111", "2019-11-01", -10),
                ("111", "2019-11-30", -10),
                ("222", "2019-10-01", None),
                ("222", "2019-10-30", 40),
            ]
        )
        loyalty_df = spark_session.createDataFrame(
            loyalty_df.map(
                lambda x: Row(
                    msisdn=x[0],
                    trx_date=x[1],
                    points_difference=int(x[2]) if x[2] else None,
                )
            )
        ).select(
            "msisdn",
            from_unixtime(unix_timestamp(f.col("trx_date"), "yyyy-MM-dd")).alias(
                "trx_date"
            ),
            "points_difference",
        )

        out = get_loyalty_points_usage_df(loyalty_df)

        out_points = out.select(
            "msisdn",
            "trx_date",
            "points_difference",
            "points_earned",
            "points_used",
            "points_earned_01m",
            "points_earned_03m",
            "points_used_01m",
            "points_used_03m",
        )

        out_points_list = [
            [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8]]
            for i in out_points.collect()
        ]
        assert len(out_points_list) == 6
        assert sorted(out_points_list) == [
            ["111", "2019-09-01 00:00:00", None, 0, 0, 0, 0, 0, 0],
            ["111", "2019-09-30 00:00:00", 10, 10, 0, 10, 10, 0, 0],
            ["111", "2019-11-01 00:00:00", -10, 0, 10, 0, 10, 10, 10],
            ["111", "2019-11-30 00:00:00", -10, 0, 10, 0, 10, 20, 20],
            ["222", "2019-10-01 00:00:00", None, 0, 0, 0, 0, 0, 0],
            ["222", "2019-10-30 00:00:00", 40, 40, 0, 40, 40, 0, 0],
        ]

        out_points_avg_trd = out.select(
            "msisdn",
            "trx_date",
            "fea_avg_points_earned_03m",
            "fea_avg_points_used_03m",
            "fea_trd_point_earned_03m",
            "fea_trd_point_usage_03m",
        )
        out_points_avg_trd_list = [
            [i[0], i[1], i[2], i[3], i[4], i[5]] for i in out_points_avg_trd.collect()
        ]

        assert sorted(out_points_avg_trd_list) == [
            ["111", "2019-09-01 00:00:00", 0.0, 0.0, None, None],
            ["111", "2019-09-30 00:00:00", 0.1111111111111111, 0.0, 1.0, None],
            [
                "111",
                "2019-11-01 00:00:00",
                0.1111111111111111,
                0.1111111111111111,
                0.0,
                1.0,
            ],
            [
                "111",
                "2019-11-30 00:00:00",
                0.1111111111111111,
                0.2222222222222222,
                0.0,
                1.0,
            ],
            ["222", "2019-10-01 00:00:00", 0.0, 0.0, None, None],
            ["222", "2019-10-30 00:00:00", 0.4444444444444444, 0.0, 1.0, None],
        ]
