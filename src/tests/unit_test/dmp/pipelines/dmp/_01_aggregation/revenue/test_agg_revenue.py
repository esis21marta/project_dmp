from pyspark.sql import Row, SparkSession

from src.dmp.pipelines.dmp._01_aggregation.revenue.create_revenue_weekly import (
    _weekly_aggregation as create_weekly_revenue_table,
)


class TestRevenueAggregate:
    def test_create_weekly_revenue_table(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        df_rev_payu_dd_df = sc.parallelize(
            [
                # Case A.1) Sanity test - only weekdays
                # Case A.2) Sanity test - only payu
                # Case A.3) Sanity test - filling 1st to 5th dayofmonth
                # Case A.4) Sanity test - Threshold >= 100R
                (
                    "111",
                    "2019-10-01",
                    500,
                    100,
                    100,
                    100,
                    100,
                    100,
                    10,
                    10,
                    10,
                    10,
                    10,
                    10,
                    10,
                    10,
                    10,
                    10,
                ),
                (
                    "111",
                    "2019-10-02",
                    500,
                    100,
                    100,
                    100,
                    100,
                    100,
                    10,
                    10,
                    10,
                    10,
                    10,
                    10,
                    10,
                    10,
                    10,
                    10,
                ),
                (
                    "111",
                    "2019-10-03",
                    500,
                    100,
                    100,
                    100,
                    100,
                    100,
                    10,
                    10,
                    10,
                    10,
                    10,
                    10,
                    10,
                    10,
                    10,
                    10,
                ),
                # Case B) Sanity test - Threshold < 100R
                (
                    "222",
                    "2019-10-03",
                    100,
                    20,
                    20,
                    20,
                    20,
                    20,
                    2,
                    2,
                    2,
                    2,
                    2,
                    2,
                    2,
                    2,
                    2,
                    2,
                ),
            ]
        )

        df_rev_payu_dd_df = spark_session.createDataFrame(
            df_rev_payu_dd_df.map(
                lambda x: Row(
                    msisdn=x[0],
                    event_date=x[1],
                    rev_total=x[1],
                    rev_voice=x[3],
                    rev_data=x[4],
                    rev_sms=x[5],
                    rev_roaming=x[6],
                    rev_dls=x[7],
                    rev_dls_mms_cnt=x[8],
                    rev_dls_mms_p2p=x[9],
                    rev_dls_music=x[10],
                    rev_dls_other_vas=x[11],
                    rev_dls_rbt=x[12],
                    rev_dls_sms_nonp2p=x[13],
                    rev_dls_tp=x[14],
                    rev_dls_ussd=x[15],
                    rev_dls_videocall=x[16],
                    rev_dls_voice_nonp2p=x[17],
                )
            )
        )

        df_rev_pkg_dd_df = sc.parallelize(
            [
                # Case C.1) Sanity test - only weekends
                # Case C.2) Sanity test - onlyl package
                # Case C.3) Sanity test - missing 1st to 5th dayofmonth
                ("333", "2019-10-12", 800, 200, 200, 200, 200),
                ("333", "2019-10-13", 400, 100, 100, 100, 100),
                # Case D) Edge case - row with 0 revenue
                ("444", "2019-10-12", 0, 0, 0, 0, 0),
            ]
        )

        df_rev_pkg_dd_df = spark_session.createDataFrame(
            df_rev_pkg_dd_df.map(
                lambda x: Row(
                    msisdn=x[0],
                    event_date=x[1],
                    rev_pkg_prchse=x[2],
                    rev_voice_pkg_prchs=x[3],
                    rev_data_pkg_prchs=x[4],
                    rev_sms_pkg_prchs=x[5],
                    rev_roam_pkg_prchs=x[6],
                )
            )
        )

        out_cols = [
            "msisdn",  # 1
            "weekstart",  # 2
            "rev_voice",  # 3
            "rev_data",  # 4
            "rev_sms",  # 5
            "rev_voice_weekend",  # 6
            "rev_data_weekend",  # 7
            "rev_sms_weekend",  # 8
            "rev_payu_tot",  # 9
            "rev_payu_voice",  # 10
            "rev_payu_sms",  # 11
            "rev_payu_data",  # 12
            "rev_payu_roam",  # 13
            "rev_payu_dls",  # 14
            "rev_payu_dls_mms_cnt",  # 15
            "rev_payu_dls_mms_p2p",  # 16
            "rev_payu_dls_music",  # 17
            "rev_payu_dls_other_vas",  # 18
            "rev_payu_dls_rbt",  # 19
            "rev_payu_dls_sms_nonp2p",  # 20
            "rev_payu_dls_tp",  # 21
            "rev_payu_dls_ussd",  # 22
            "rev_payu_dls_videocall",  # 23
            "rev_payu_dls_voice_nonp2p",  # 24
            "rev_pkg_tot",  # 25
            "rev_pkg_voice",  # 26
            "rev_pkg_sms",  # 27
            "rev_pkg_data",  # 28
            "rev_pkg_roam",  # 29
            "rev_voice_tot_1_5d",  # 30
            "rev_data_tot_1_5d",  # 31
            "rev_sms_tot_1_5d",  # 32
            "rev_roam_tot_1_5d",  # 33
            "days_with_rev_payu_voice_above_99_below_1000",  # 34
            "days_with_rev_payu_data_above_99_below_1000",  # 35
            "days_with_rev_payu_sms_above_99_below_1000",  # 36
            "days_with_rev_payu_dls_above_99_below_1000",  # 37
            "days_with_rev_payu_roam_above_99_below_1000",  # 38
            "rev_alt_sms",  # 39
            "rev_alt_sms_pkg",  # 40
            "rev_alt_digital",  # 41
            "rev_alt_voice",  # 42
            "rev_alt_voice_pkg",  # 43
            "rev_alt_roam",  # 44
            "rev_alt_roam_pkg",  # 45
            "rev_alt_data_weekend",  # 46
            "rev_alt_sms_weekend",  # 47
            "rev_alt_voice_weekend",  # 48
            "rev_alt_days_with_data_above_999_below_5000",  # 49
            "rev_alt_days_with_voice_above_999_below_5000",  # 50
            "rev_alt_days_with_digital_above_999_below_5000",  # 51
        ]

        df_merge_revenue_dd_df = sc.parallelize(
            [
                # Case A.1) Sanity test - only weekdays
                # Case A.2) Sanity test - only sms, voice and roaming
                # Case A.3) Sanity test - filling 1st to 5th dayofmonth
                (
                    "111",
                    "2019-10-01",
                    "SMS P2P",
                    "SMS Regular",
                    "Domestik SMS MO Internal",
                    80.0,
                ),
                ("111", "2019-10-02", "SMS P2P", "SMS Package", "Microcampaign", 50.0),
                ("111", "2019-10-02", "SMS P2P", "SMS Package", "Microcampaign", 60.0),
                ("111", "2019-10-03", "Voice P2P", "NQ", "Other voice package", 5000.0),
                (
                    "111",
                    "2019-10-04",
                    "International Roaming",
                    "Broadband",
                    "Data Roaming Package",
                    70.0,
                ),
                # Case B.1) Sanity test - only voice, data and roaming
                # Case B.2) Sanity test - weekdays + weekends
                (
                    "222",
                    "2019-10-01",
                    "Voice P2P",
                    "Voice Package",
                    "Other voice package",
                    70.0,
                ),
                (
                    "222",
                    "2019-10-02",
                    "International Roaming",
                    "Broadband",
                    "Data Roaming Package",
                    70.0,
                ),
                (
                    "222",
                    "2019-10-03",
                    "Broadband",
                    "APN/PAYU",
                    "Voice MT Roaming",
                    1000.0,
                ),
                (
                    "222",
                    "2019-10-05",
                    "Broadband",
                    "APN/PAYU",
                    "Voice MT Roaming",
                    1000.0,
                ),
                # Case C.1) Sanity test - only weekends
                # Case C.2) Sanity test - missing 1st to 5th dayofmonth
                ("333", "2019-10-12", "Broadband", "NQ", "Voice MT Roaming", 2000.0),
                ("333", "2019-10-13", "Digital Services", "USSD", "DLS", 2000.0),
                # Case D.1) Sanity test - has 0 revenue
                ("444", "2019-10-12", "Voice P2P", "NQ", "Other voice package", 0.0),
                # Case E.1) Sanity test - weekdays + weekends
                # Case E.2) Sanity test - Present only in merge_revenue_dd table
                ("555", "2019-10-09", "SMS P2P", "NQ", "NQ", 100.0),
                ("555", "2019-10-12", "Voice P2P", "NQ", "NQ", 0.0),
                ("555", "2019-10-12", "NQ", "NQ", "NQ", 0.0),
            ]
        )

        df_merge_revenue_dd_df = spark_session.createDataFrame(
            df_merge_revenue_dd_df.map(
                lambda x: Row(
                    msisdn=x[0],
                    event_date=x[1],
                    l1_name=x[2],
                    l2_name=x[3],
                    l3_name=x[4],
                    rev=x[5],
                )
            )
        )

        out = create_weekly_revenue_table(
            df_rev_payu_dd_df, df_rev_pkg_dd_df, df_merge_revenue_dd_df
        ).select(out_cols)
        out_list = [
            [
                i[0],
                i[1].strftime("%Y-%m-%d"),
                i[2],
                i[3],
                i[4],
                i[5],
                i[6],
                i[7],
                i[8],
                i[9],
                i[10],
                i[11],
                i[12],
                i[13],
                i[14],
                i[15],
                i[16],
                i[17],
                i[18],
                i[19],
                i[20],
                i[21],
                i[22],
                i[23],
                i[24],
                i[25],
                i[26],
                i[27],
                i[28],
                i[29],
                i[30],
                i[31],
                i[32],
                i[33],
                i[34],
                i[35],
                i[36],
                i[37],
                i[38],
                i[39],
                i[40],
                i[41],
                i[42],
                i[43],
                i[44],
                i[45],
                i[46],
                i[47],
                i[48],
                i[49],
                i[50],
            ]
            for i in out.collect()
        ]

        assert sorted(out_list) == [
            # Case A.1) Assert correct results (weekend columns will be null)
            # Case A.2) Assert correct results (package columns will be 0 while payu columns are filled)
            # Case A.3) Assert correct results (1st to 5th will be filled)
            # Case A.4) Assert correct results (threshold will be filled)
            [
                "111",
                "2019-10-07",
                300,
                300,
                300,
                None,
                None,
                None,
                None,
                300,
                300,
                300,
                300,
                300,
                30,
                30,
                30,
                30,
                30,
                30,
                30,
                30,
                30,
                30,
                0,
                0,
                0,
                0,
                0,
                300,
                300,
                300,
                300,  # --> 1st to 5th dayofmonth filled
                3,
                3,
                3,
                3,
                3,  # --> threshold filled
                190,
                110,
                None,
                5000,
                None,
                70,
                70,
                None,
                None,
                None,
                0,
                0,
                0,
            ],
            # Case A.1 - A.3) Same result as above except..
            # Case B) Assert correct results (threshold will be 0)
            [
                "222",
                "2019-10-07",
                20,
                20,
                20,
                None,
                None,
                None,
                None,
                20,
                20,
                20,
                20,
                20,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                0,
                0,
                0,
                0,
                0,
                20,
                20,
                20,
                20,  # --> 1st to 5th dayofmonth filled
                0,
                0,
                0,
                0,
                0,  # --> threshold 0
                None,
                None,
                None,
                70,
                70,
                70,
                70,
                1000,
                None,
                None,
                2,
                0,
                0,
            ],
            # Case C.1) Assert correct results (weekend columns will be filled)
            # Case C.2) Assert correct results (payu columns will bi 0 while package columns are filled)
            # Case C.3) Assert correct results (1st to 5th will be 0)
            [
                "333",
                "2019-10-14",
                300,
                300,
                300,
                300,
                300,
                300,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                1200,
                300,
                300,
                300,
                300,
                0,
                0,
                0,
                0,  # --> 1st to 5th dayofmonth 0
                0,
                0,
                0,
                0,
                0,
                None,
                None,
                2000,
                None,
                None,
                None,
                None,
                2000,
                None,
                None,
                1,
                0,
                1,
            ],
            # Case D) Should be all 0's/nulls
            [
                "444",
                "2019-10-14",
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                None,
                None,
                None,
                0,
                None,
                None,
                None,
                None,
                None,
                0,
                0,
                0,
                0,
            ],
            # Case E) Should have data only for alt features
            [
                "555",
                "2019-10-14",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                100,
                None,
                None,
                0,
                None,
                None,
                None,
                None,
                None,
                0,
                0,
                0,
                0,
            ],
        ]
