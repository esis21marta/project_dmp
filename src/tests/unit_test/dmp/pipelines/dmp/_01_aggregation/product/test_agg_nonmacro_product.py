from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from src.dmp.pipelines.dmp._01_aggregation.product.create_non_macro_product_weekly import (
    _weekly_aggregation as create_non_macro_product_weekly,
)

SKU_BUCKET_PIVOT_SCHEMA = StructType(
    [
        StructField("bid", StringType()),
        StructField("sku", StringType()),
        StructField("tcash_balance", StringType()),
        StructField("voice_offnet", StringType()),
        StructField("voice_allnet", StringType()),
        StructField("voice_allopr", StringType()),
        StructField("voice_onnet_malam", StringType()),
        StructField("voice_onnet_siang", StringType()),
        StructField("voice_roaming_mo", StringType()),
        StructField("voice_onnet", StringType()),
        StructField("voice_idd", StringType()),
        StructField("voice_roaming", StringType()),
        StructField("data_games", StringType()),
        StructField("data_video", StringType()),
        StructField("data_dpi", StringType()),
        StructField("data_4g_omg", StringType()),
        StructField("data_wifi", StringType()),
        StructField("data_roaming", StringType()),
        StructField("data_allnet", StringType()),
        StructField("data_4g_mds", StringType()),
        StructField("data_allnet_local", StringType()),
        StructField("unlimited_data", StringType()),
        StructField("data_allnet_siang", StringType()),
        StructField("data_music", StringType()),
        StructField("4g_data_pool", StringType()),
        StructField("data_onnet", StringType()),
        StructField("data_4g", StringType()),
        StructField("data_mds", StringType()),
        StructField("sms_allnet", StringType()),
        StructField("sms_allopr", StringType()),
        StructField("sms_offnet", StringType()),
        StructField("sms_roaming", StringType()),
        StructField("sms_onnet", StringType()),
        StructField("sms_onnet_siang", StringType()),
        StructField("subscription_tribe", StringType()),
        StructField("subscription_hooq", StringType()),
        StructField("subscription_viu", StringType()),
        StructField("subscription_bein", StringType()),
        StructField("subscription_music", StringType()),
        StructField("monbal_monbal", StringType()),
        StructField("monbal_siang", StringType()),
        StructField("monbal_onnet", StringType()),
    ]
)


class TestNonMacroProductAggregate:
    def test_create_non_macro_product_weekly(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        df_usage_ocs_chg_dd = sc.parallelize(
            [
                # Case A.1) Sanity test - cp_name IS reflex or digicore
                # Case A.2) Sanity test - data > 1023
                ("111", "2019-10-01", "abc", "abc123", "digicore", "vas", 100),
                ("111", "2019-10-08", "def", "def123", "digicore", "vas", 100),
                # Case B) Sanity test - cp_name NOT reflex or digicore
                ("222", "2019-10-01", "hij", "hij123", "REFLEX1", "vas", 100),
                ("222", "2019-10-15", "klm", "klm123", "OTHER", "vas", 100),
                # Case C) Edge case - null first week
                ("333", "2019-10-01", None, None, None, None, None),
                ("333", "2019-10-08", "abc", "abc123", "digicore", "vas", 100),
                ("333", "2019-10-15", "def", "def123", "digicore", "vas", 100),
            ]
        )

        df_usage_ocs_chg_dd = spark_session.createDataFrame(
            df_usage_ocs_chg_dd.map(
                lambda x: Row(
                    msisdn=x[0],
                    event_date=x[1],
                    content_id=x[2],
                    pack_id=x[3],
                    cp_name=x[4],
                    service_filter=x[5],
                    rev=x[6],
                )
            )
        )

        df_sku_bucket_pivot_dd = sc.parallelize(
            [
                # Case A) Sanity test - digicore
                [
                    "abc",
                    "abc123",
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
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    55,
                    55,
                    55,
                    10,
                    10,
                    10,
                    10,
                    10,
                    10,
                    0,
                    0,
                    0,
                    0,
                    0,
                    10,
                    10,
                    10,
                ],
                [
                    "def",
                    "def123",
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    55,
                    55,
                    55,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    0,
                    0,
                    0,
                    0,
                    0,
                    20,
                    20,
                    20,
                ],
                # Case B) Sanity test - digicore not exist
                [
                    "opq",
                    "opq123",
                    15,
                    15,
                    15,
                    15,
                    15,
                    15,
                    15,
                    15,
                    15,
                    15,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    1024,
                    55,
                    55,
                    55,
                    15,
                    15,
                    15,
                    15,
                    15,
                    15,
                    0,
                    0,
                    0,
                    0,
                    0,
                    15,
                    15,
                    15,
                ],
            ]
        )

        df_sku_bucket_pivot_dd = spark_session.createDataFrame(
            df_sku_bucket_pivot_dd, schema=SKU_BUCKET_PIVOT_SCHEMA
        )

        out_cols = [
            "msisdn",  # 1
            "weekstart",  # 2
            "revenue",  # 3
            "tcash_balance",  # 4
            "voice_offnet",  # 5
            "voice_allnet",  # 6
            "voice_allopr",  # 7
            "voice_onnet_malam",  # 8
            "voice_onnet_siang",  # 9
            "voice_roaming_mo",  # 10
            "voice_onnet",  # 11
            "voice_idd",  # 12
            "voice_roaming",  # 13
            "data_games",  # 14
            "data_video",  # 15
            "data_dpi",  # 16
            "data_4g_omg",  # 17
            "data_wifi",  # 18
            "data_roaming",  # 19
            "data_allnet",  # 20
            "data_4g_mds",  # 21
            "data_allnet_local",  # 22
            "unlimited_data",  # 23
            "data_allnet_siang",  # 24
            "data_music",  # 25
            "fg_data_pool",  # 26
            "data_onnet",  # 27
            "data_4g",  # 28
            "data_mds",  # 29
            "sms_allnet",  # 30
            "sms_allopr",  # 31
            "sms_offnet",  # 32
            "sms_roaming",  # 33
            "sms_onnet",  # 34
            "sms_onnet_siang",  # 35
            "subscription_tribe",  # 36
            "subscription_hooq",  # 37
            "subscription_viu",  # 38
            "subscription_bein",  # 39
            "subscription_music",  # 40
            "monbal_monbal",  # 41
            "monbal_siang",  # 42
            "monbal_onnet",  # 43
        ]

        out = create_non_macro_product_weekly(
            df_usage_ocs_chg_dd, df_sku_bucket_pivot_dd
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
            ]
            for i in out.collect()
        ]

        assert sorted(out_list) == [
            # Case A) Assert correct results value will be filled
            [
                "111",
                "2019-10-07",
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
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                55,
                55,
                55,
                10,
                10,
                10,
                10,
                10,
                10,
                1,
                1,
                1,
                1,
                1,
                10,
                10,
                10,
            ],
            [
                "111",
                "2019-10-14",
                100,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                55,
                55,
                55,
                20,
                20,
                20,
                20,
                20,
                20,
                1,
                1,
                1,
                1,
                1,
                20,
                20,
                20,
            ],
            # Case B.1) Assert correct results cp_name NOT reflex or digicore
            # Case B.2) Assert correct results digicore not exist
            [
                "222",
                "2019-10-07",
                100,
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
                None,
                None,
                None,
                None,
            ],
            # Case C) Assert correct results (weekend columns will be filled)
            [
                "333",
                "2019-10-14",
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
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                55,
                55,
                55,
                10,
                10,
                10,
                10,
                10,
                10,
                1,
                1,
                1,
                1,
                1,
                10,
                10,
                10,
            ],
            [
                "333",
                "2019-10-21",
                100,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                55,
                55,
                55,
                20,
                20,
                20,
                20,
                20,
                20,
                1,
                1,
                1,
                1,
                1,
                20,
                20,
                20,
            ],
        ]
