import pyspark.sql.functions as f
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import from_unixtime, unix_timestamp

from src.dmp.pipelines.dmp._04_features.customer_profile.customer_los import (
    feat_los_days,
)


class TestCustomerLos:
    def test_sanity(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        pre_paid = sc.parallelize(
            [
                ("111", "2019-10-01", 10, "2019-10-01"),
                ("111", "2019-10-02", 11, "2019-10-02"),
                ("111", "2019-10-03", 12, "2019-10-03"),
                ("111", "2019-10-04", 13, "2019-10-04"),
                ("111", "2019-10-05", 14, "2019-10-05"),
                ("111", "2019-10-06", 15, "2019-10-06"),
                ("111", "2019-10-07", 16, "2019-10-07"),
                ("111", "2019-10-08", 17, "2019-10-08"),
                ("111", "2019-10-09", 18, "2019-10-09"),
            ]
        )
        pre_paid_df = spark_session.createDataFrame(
            pre_paid.map(
                lambda x: Row(
                    msisdn=x[0], trx_date=x[1], los=int(x[2]), event_date=x[3]
                )
            )
        ).select(
            "msisdn",
            from_unixtime(unix_timestamp(f.col("trx_date"), "yyyy-MM-dd")).alias(
                "trx_date"
            ),
            "los",
            from_unixtime(unix_timestamp(f.col("event_date"), "yyyy-MM-dd")).alias(
                "event_date"
            ),
        )

        post_paid = sc.parallelize(
            [
                ("222", "2019-10-01", 130, "2019-10-01",),
                ("222", "2019-10-02", 131, "2019-10-02",),
                ("222", "2019-10-03", 132, "2019-10-03",),
                ("222", "2019-10-04", 133, "2019-10-04",),
                ("222", "2019-10-05", 134, "2019-10-05",),
                ("222", "2019-10-06", 135, "2019-10-06",),
                ("222", "2019-10-07", 136, "2019-10-07",),
                ("222", "2019-10-08", 137, "2019-10-08",),
                ("222", "2019-10-09", 138, "2019-10-09",),
            ]
        )
        post_paid_df = spark_session.createDataFrame(
            post_paid.map(
                lambda x: Row(
                    msisdn=x[0], trx_date=x[1], los=int(x[2]), event_date=x[3]
                )
            )
        ).select(
            "msisdn",
            from_unixtime(unix_timestamp(f.col("trx_date"), "yyyy-MM-dd")).alias(
                "trx_date"
            ),
            "los",
            from_unixtime(unix_timestamp(f.col("event_date"), "yyyy-MM-dd")).alias(
                "event_date"
            ),
        )

        out = feat_los_days(pre_paid_df, post_paid_df)
        out_list = [[i[0], i[1].strftime("%Y-%m-%d"), i[2]] for i in out.collect()]

        assert out.columns == ["msisdn", "weekstart", "los"]
        assert len(out_list) == 4
        assert sorted(out_list) == [
            ["111", "2019-10-07", 15],
            ["111", "2019-10-14", 18],
            ["222", "2019-10-07", 135],
            ["222", "2019-10-14", 138],
        ]

    def test_duplicate_msisdn(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        pre_paid = sc.parallelize(
            [
                ("111", "2019-10-01", 10, "2019-10-01"),
                ("222", "2019-10-01", 20, "2019-10-01"),
            ]
        )
        pre_paid_df = spark_session.createDataFrame(
            pre_paid.map(
                lambda x: Row(
                    msisdn=x[0], trx_date=x[1], los=int(x[2]), event_date=x[3]
                )
            )
        ).select(
            "msisdn",
            from_unixtime(unix_timestamp(f.col("trx_date"), "yyyy-MM-dd")).alias(
                "trx_date"
            ),
            "los",
            from_unixtime(unix_timestamp(f.col("event_date"), "yyyy-MM-dd")).alias(
                "event_date"
            ),
        )

        post_paid = sc.parallelize(
            [
                ("222", "2019-10-01", 10, "2019-10-01"),
                ("333", "2019-10-01", 30, "2019-10-01"),
            ]
        )
        post_paid_df = spark_session.createDataFrame(
            post_paid.map(
                lambda x: Row(
                    msisdn=x[0], trx_date=x[1], los=int(x[2]), event_date=x[3]
                )
            )
        ).select(
            "msisdn",
            from_unixtime(unix_timestamp(f.col("trx_date"), "yyyy-MM-dd")).alias(
                "trx_date"
            ),
            "los",
            from_unixtime(unix_timestamp(f.col("event_date"), "yyyy-MM-dd")).alias(
                "event_date"
            ),
        )

        out = feat_los_days(pre_paid_df, post_paid_df)
        out_list = [[i[0], i[1].strftime("%Y-%m-%d"), i[2]] for i in out.collect()]

        assert out.columns == ["msisdn", "weekstart", "los"]
        assert len(out_list) == 3
        assert sorted(out_list) == [
            ["111", "2019-10-07", 10],
            ["222", "2019-10-07", 20],
            ["333", "2019-10-07", 30],
        ]
