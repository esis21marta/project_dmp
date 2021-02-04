import pyspark.sql.functions as f
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import from_unixtime, unix_timestamp

from src.dmp.pipelines.dmp._01_aggregation.airtime_loan.agg_airtime_loan_to_weekly import (
    compute_dpd_per_weeekstart,
    fea_calc_airtime_loan_count_features,
)


class TestAirtimeLoad:
    def test_compute_dpd_per_weeekstart(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        takers = sc.parallelize(
            [
                # Case A) Taker count == Payer count
                (
                    "111",
                    "2019-10-07",
                    "2019-10-05 10:00:00",
                    "----------------------1111",
                    "1111",
                    1,
                ),
                (
                    "111",
                    "2019-10-14",
                    "2019-10-13 10:00:00",
                    "----------------------1112",
                    "1112",
                    1,
                ),
                # Case B) Taker count > Payer count
                (
                    "222",
                    "2019-10-07",
                    "2019-10-07 10:00:00",
                    "----------------------2221",
                    "2221",
                    2,
                ),
                (
                    "222",
                    "2019-10-14",
                    "2019-10-13 10:00:00",
                    "----------------------2222",
                    "2222",
                    2,
                ),
                # Case C) Payer count > Taker count
                (
                    "333",
                    "2019-10-14",
                    "2019-10-13 10:00:00",
                    "----------------------3331",
                    "3331",
                    3,
                ),
                # Case  D) Taker count == 0, Payment count > 0
                # Case  E) Taker count > 0, Payment count == 0
                (
                    "555",
                    "2019-10-14",
                    "2019-10-13 10:00:00",
                    "----------------------5551",
                    "5551",
                    5,
                ),
            ]
        )

        payment = sc.parallelize(
            [
                # Case A) Taker count == Payer count
                ("111", "2019-10-15 10:00:00", "------------------------1111", "1111"),
                ("111", "2019-10-13 12:00:00", "------------------------1112", "1112"),
                # Case B) Taker count > Payer count
                ("222", "2019-10-08 12:00:00", "------------------------2221", "2221"),
                # Case C) Payer count > Taker count
                ("333", "2019-10-14 08:00:00", "------------------------3331", "3331"),
                ("333", "2019-10-19 08:00:00", "------------------------3332", "3332"),
                # Case  D) Taker count == 0, Payment count > 0
                ("444", "2019-10-19 08:00:00", "------------------------4441", "4441"),
                # Case  E) Taker count > 0, Payment count == 0
            ]
        )

        takers = spark_session.createDataFrame(
            takers.map(
                lambda x: Row(
                    msisdn=x[0],
                    weekstart=x[1],
                    timestamp=x[2],
                    campaignid=x[3],
                    month_campaign=x[4],
                    tot_airtime_loan_count=x[5],
                )
            )
        ).select(
            "msisdn",
            "weekstart",
            from_unixtime(unix_timestamp(f.col("timestamp"), "yyyy-MM-dd")).alias(
                "timestamp"
            ),
            from_unixtime(
                unix_timestamp(f.col("timestamp"), "yyyy-MM-dd HH:mm:ss")
            ).alias("trx_date"),
            "campaignid",
            "month_campaign",
            "tot_airtime_loan_count",
        )

        payment = spark_session.createDataFrame(
            payment.map(
                lambda x: Row(
                    msisdn=x[0], timestamp=x[1], campaignid=x[2], month_campaign=x[3]
                )
            )
        ).select(
            "msisdn",
            from_unixtime(unix_timestamp(f.col("timestamp"), "yyyy-MM-dd")).alias(
                "timestamp"
            ),
            from_unixtime(
                unix_timestamp(f.col("timestamp"), "yyyy-MM-dd HH:mm:ss")
            ).alias("trx_date"),
            "campaignid",
            "month_campaign",
        )

        out = compute_dpd_per_weeekstart(takers, payment)

        out_cols = [
            "msisdn",
            "weekstart",
            "tot_airtime_loan_count",
            "repayment_duration",
            "2_dpd_count",
            "10_dpd_count",
            "15_dpd_count",
            "20_dpd_count",
            "25_dpd_count",
        ]

        out_list = [
            [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8]]
            for i in out.select(out_cols).collect()
        ]

        assert sorted(out_list) == [
            # Case A) Regular output expected
            ["111", "2019-10-07", 1, 10, 1, 0, 0, 0, 0],
            ["111", "2019-10-14", 1, 0, 0, 0, 0, 0, 0],
            # Case B) Expected 1 repayment able to calculate DPD, the other not
            ["222", "2019-10-07", 2, 1, 0, 0, 0, 0, 0],
            ["222", "2019-10-14", 2, None, None, None, None, None, None],
            # Case C) Expected only 1 repayment able to calculate
            ["333", "2019-10-14", 3, 1, 0, 0, 0, 0, 0],
            # Case D) No output expected
            # Case E) Output with no DPD values
            ["555", "2019-10-14", 5, None, None, None, None, None, None],
        ]

    def test_fea_calc_airtime_loan_count_features(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        takers = sc.parallelize(
            [
                ("111", "2019-10-01", "abc"),
                ("111", "2019-10-04", "abc"),
                ("111", "2019-10-06", "abc"),
                ("111", "2019-10-10", "abc"),
                ("111", "2019-10-12", "abc"),
            ]
        )
        takers = spark_session.createDataFrame(
            takers.map(lambda x: Row(msisdn=x[0], timestamp=x[1], campaignid=x[2]))
        ).select(
            "msisdn",
            from_unixtime(unix_timestamp(f.col("timestamp"), "yyyy-MM-dd")).alias(
                "timestamp"
            ),
            "campaignid",
        )

        out = fea_calc_airtime_loan_count_features(takers)
        assert "msisdn" in out.columns

        out_list = [
            [i[0], i[1].strftime("%Y-%m-%d"), i[2]]
            for i in out.select(
                "msisdn", "weekstart", "tot_airtime_loan_count"
            ).collect()
        ]
        assert sorted(out_list) == [
            ["111", "2019-10-07", 3],
            ["111", "2019-10-07", 3],
            ["111", "2019-10-07", 3],
            ["111", "2019-10-14", 2],
            ["111", "2019-10-14", 2],
        ]
