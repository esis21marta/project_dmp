import datetime
from unittest import mock

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._04_features.internet_app_usage.fea_internet_app_usage import (
    fea_internet_app_usage,
    pivot_features,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


@mock.patch(
    "src.dmp.pipelines.dmp._04_features.internet_app_usage.fea_internet_app_usage.get_start_date",
    return_value=datetime.date(2019, 3, 9),
    autospec=True,
)
@mock.patch(
    "src.dmp.pipelines.dmp._04_features.internet_app_usage.fea_internet_app_usage.get_end_date",
    return_value=datetime.date(2020, 10, 25),
    autospec=True,
)
class TestInternetAppUsage:
    def test_fea_internet_app_usage(
        self, mock_get_end_date, mock_get_start_date, spark_session: SparkSession
    ):
        l4_internet_app_usage_weekly_final = spark_session.createDataFrame(
            # fmt: off
            data = [
              # ['msisdn', 'weekstart'                , 'category'    , 'volume_in', 'volume_out', 'accessed_app'             , '4g_volume_in_weekday', '4g_volume_in_weekend', '4g_volume_out_weekday', '4g_volume_out_weekend', '4g_duration_weekday', '4g_duration_weekend', '4g_internal_latency_weekday', '4g_internal_latency_count_weekday', '4g_internal_latency_weekend', '4g_internal_latency_count_weekend', '4g_external_latency_weekday', '4g_external_latency_count_weekday', '4g_external_latency_weekend', '4g_external_latency_count_weekend', 'min_trx_date'             , 'max_trx_date'             ],
                ['1'     , datetime.date(2019, 12, 16), 'instagram'   , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['1'     , datetime.date(2019, 12, 16), 'youtube'     , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['1'     , datetime.date(2019, 12, 16), 'budget_hobby', 124        , 1000        , ['Netflix', 'Amazon Prime'], 0                     , 0                     , 0                      , 0                      , 0                    , 0                    , 0.0                          , 0                                  , 0.0                          , 0                                  , 0.0                          , 0                                  , 0.0                          , 0                                  , datetime.date(2019, 12, 11), datetime.date(2019, 12, 14)],
                ['1'     , datetime.date(2019, 12, 16), 'linkedin'    , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['1'     , datetime.date(2019, 12, 16), 'facebook'    , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['1'     , datetime.date(2020, 3, 9)  , 'instagram'   , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['1'     , datetime.date(2020, 3, 9)  , 'youtube'     , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['1'     , datetime.date(2020, 3, 9)  , 'budget_hobby', 94794      , 72754       , ['Netflix']                , 0                     , 0                     , 0                      , 0                      , 0                    , 0                    , 0.0                          , 0                                  , 0.0                          , 0                                  , 0.0                          , 0                                  , 0.0                          , 0                                  , datetime.date(2020, 3, 8)  , datetime.date(2020, 3, 8)  ],
                ['1'     , datetime.date(2020, 3, 9)  , 'linkedin'    , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['1'     , datetime.date(2020, 3, 9)  , 'facebook'    , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['1'     , datetime.date(2020, 3, 16) , 'instagram'   , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['1'     , datetime.date(2020, 3, 16) , 'youtube'     , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['1'     , datetime.date(2020, 3, 16) , 'budget_hobby', 121946     , 156743      , ['Netflix']                , 70471                 , 0                     , 77193                  , 0                      , 390                  , 0                    , 0.1                          , 1                                  , 0.0                          , 0                                  , 0.0                          , 1                                  , 0.0                          , 0                                  , datetime.date(2020, 3, 9)  , datetime.date(2020, 3, 10) ],
                ['1'     , datetime.date(2020, 3, 16) , 'linkedin'    , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['1'     , datetime.date(2020, 3, 16) , 'facebook'    , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['2'     , datetime.date(2020, 3, 9)  , 'instagram'   , 39505      , 30478       , ['Instagram']              , 39505                 , 0                     , 30478                  , 0                      , 266                  , 0                    , 0.4                          , 1                                  , 0.0                          , 0                                  , 0.1                          , 1                                  , 0.0                          , 0                                  , datetime.date(2020, 3, 6)  , datetime.date(2020, 3, 6)  ],
                ['2'     , datetime.date(2020, 3, 9)  , 'youtube'     , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['2'     , datetime.date(2020, 3, 9)  , 'budget_hobby', 159505     , 54371       , ['Netflix']                , 0                     , 0                     , 0                      , 0                      , 0                    , 0                    , 0.0                          , 0                                  , 0.0                          , 0                                  , 0.0                          , 0                                  , 0.0                          , 0                                  , datetime.date(2020, 3, 5)  , datetime.date(2020, 3, 7)  ],
                ['2'     , datetime.date(2020, 3, 9)  , 'linkedin'    , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['2'     , datetime.date(2020, 3, 9)  , 'facebook'    , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['2'     , datetime.date(2020, 3, 16) , 'instagram'   , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['2'     , datetime.date(2020, 3, 16) , 'youtube'     , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['2'     , datetime.date(2020, 3, 16) , 'budget_hobby', None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['2'     , datetime.date(2020, 3, 16) , 'linkedin'    , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['2'     , datetime.date(2020, 3, 16) , 'facebook'    , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['3'     , datetime.date(2019, 12, 16), 'instagram'   , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['3'     , datetime.date(2019, 12, 16), 'youtube'     , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['3'     , datetime.date(2019, 12, 16), 'budget_hobby', 131        , 10000       , ['Netflix', 'Amazon Prime'], 0                     , 0                     , 0                      , 0                      , 0                    , 0                    , 0.0                          , 0                                  , 0.0                          , 0                                  , 0.0                          , 0                                  , 0.0                          , 0                                  , datetime.date(2019, 12, 11), datetime.date(2019, 12, 14)],
                ['3'     , datetime.date(2019, 12, 16), 'linkedin'    , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['3'     , datetime.date(2019, 12, 16), 'facebook'    , 121        , 241         , ['Facebook']               , 121                   , None                  , 241                    , None                   , 12                   , None                 , 12.1                         , 2                                  , None                         , None                               , None                         , None                               , None                         , None                               , datetime.date(2019, 12, 11), datetime.date(2019, 12, 12)],
                ['3'     , datetime.date(2020, 3, 9)  , 'instagram'   , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['3'     , datetime.date(2020, 3, 9)  , 'youtube'     , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['3'     , datetime.date(2020, 3, 9)  , 'budget_hobby', 72503      , 129133      , ['Netflix']                , 0                     , 0                     , 0                      , 0                      , 0                    , 0                    , 0.0                          , 0                                  , 0.0                          , 0                                  , 0.0                          , 0                                  , 0.0                          , 0                                  , datetime.date(2020, 3, 2)  , datetime.date(2020, 3, 4)  ],
                ['3'     , datetime.date(2020, 3, 9)  , 'linkedin'    , 16818      , 17590       , ['LinkedIn']               , 16818                 , 0                     , 17590                  , 0                      , 485                  , 0                    , 0.0                          , 1                                  , 0.0                          , 0                                  , 0.4                          , 1                                  , 0.0                          , 0                                  , datetime.date(2020, 3, 3)  , datetime.date(2020, 3, 3)  ],
                ['3'     , datetime.date(2020, 3, 9)  , 'facebook'    , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['3'     , datetime.date(2020, 3, 16) , 'instagram'   , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['3'     , datetime.date(2020, 3, 16) , 'youtube'     , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['3'     , datetime.date(2020, 3, 16) , 'budget_hobby', None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['3'     , datetime.date(2020, 3, 16) , 'linkedin'    , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
                ['3'     , datetime.date(2020, 3, 16) , 'facebook'    , None       , None        , None                       , None                  , None                  , None                   , None                   , None                 , None                 , None                         , None                               , None                         , None                               , None                         , None                               , None                         , None                               , None                       , None                       ],
            ],
            # fmt: on
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField("weekstart", DateType(), True),
                    StructField("category", StringType(), True),
                    StructField("volume_in", LongType(), True),
                    StructField("volume_out", LongType(), True),
                    StructField("accessed_app", ArrayType(StringType(), True), True),
                    StructField("4g_volume_in_weekday", LongType(), True),
                    StructField("4g_volume_in_weekend", LongType(), True),
                    StructField("4g_volume_out_weekday", LongType(), True),
                    StructField("4g_volume_out_weekend", LongType(), True),
                    StructField("4g_duration_weekday", LongType(), True),
                    StructField("4g_duration_weekend", LongType(), True),
                    StructField("4g_internal_latency_weekday", DoubleType(), True),
                    StructField("4g_internal_latency_count_weekday", LongType(), True),
                    StructField("4g_internal_latency_weekend", DoubleType(), True),
                    StructField("4g_internal_latency_count_weekend", LongType(), True),
                    StructField("4g_external_latency_weekday", DoubleType(), True),
                    StructField("4g_external_latency_count_weekday", LongType(), True),
                    StructField("4g_external_latency_weekend", DoubleType(), True),
                    StructField("4g_external_latency_count_weekend", LongType(), True),
                    StructField("min_trx_date", DateType(), True),
                    StructField("max_trx_date", DateType(), True),
                ]
            ),
        )

        actual_fea_internet_app_usage = fea_internet_app_usage(
            df_app_usage_weekly=l4_internet_app_usage_weekly_final,
            feature_mode="all",
            required_output_features=[],
            shuffle_partitions=1,
        )

        expected_fea_internet_app_usage = spark_session.createDataFrame(
            # fmt: off
            data = [
              # ['msisdn', 'weekstart'                , 'category'    , 'data_vol_weekday_weekend_ratio_01m', 'data_vol_weekday_weekend_ratio_01w', 'data_vol_weekday_weekend_ratio_02m', 'data_vol_weekday_weekend_ratio_03m', 'latency_4g_weekdays_01m', 'latency_4g_weekdays_01w', 'latency_4g_weekdays_02m', 'latency_4g_weekends_01m', 'latency_4g_weekends_01w', 'latency_4g_weekends_02m', 'speed_4g_weekdays_01m', 'speed_4g_weekdays_01w', 'speed_4g_weekdays_02m', 'speed_4g_weekends_01m', 'speed_4g_weekends_01w', 'speed_4g_weekends_02m', 'accessed_apps_01m', 'accessed_apps_01w', 'accessed_apps_02m', 'accessed_apps_02w', 'accessed_apps_03m', 'accessed_apps_03w', 'days_since_first_usage', 'days_since_last_usage', 'accessed_apps_num_weeks_01m', 'accessed_apps_num_weeks_01w', 'accessed_apps_num_weeks_02m', 'accessed_apps_num_weeks_03m', 'data_vol_01m', 'data_vol_01w', 'data_vol_02m', 'data_vol_02w', 'data_vol_03m', 'data_vol_03w', 'data_vol_weekday_03m', 'duration_01m', 'duration_01w', 'duration_02m', 'duration_03m'],
                ['1'     , datetime.date(2019, 12, 16), 'budget_hobby', None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 2                  , 2                  , 2                  , 2                  , 2                  , 2                  , 5                       , 2                      , 1                            , 1                            , 1                            , 1                            , 1124          , 1124          , 1124          , 1124          , 1124          , 1124          , 0                     , 0             , 0             , 0             , 0             ],
                ['1'     , datetime.date(2020, 3, 9)  , 'budget_hobby', None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 1                  , 1                  , 1                  , 1                  , 2                  , 1                  , 89                      , 1                      , 1                            , 1                            , 1                            , 2                            , 167548        , 167548        , 167548        , 167548        , 168672        , 167548        , 0                     , 0             , 0             , 0             , 0             ],
                ['1'     , datetime.date(2020, 3, 16) , 'budget_hobby', 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , 0.05                     , 0.05                     , 0.05                     , None                     , None                     , None                     , 378.625641025641       , 378.625641025641       , 378.625641025641       , None                   , None                   , None                   , 1                  , 1                  , 1                  , 1                  , 1                  , 1                  , 91                      , 6                      , 2                            , 1                            , 2                            , 2                            , 446237        , 278689        , 446237        , 446237        , 446237        , 446237        , 147664                , 390           , 390           , 390           , 390           ],
                ['1'     , datetime.date(2019, 12, 16), 'facebook'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2020, 3, 9)  , 'facebook'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2020, 3, 16) , 'facebook'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2019, 12, 16), 'instagram'   , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2020, 3, 9)  , 'instagram'   , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2020, 3, 16) , 'instagram'   , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2019, 12, 16), 'linkedin'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2020, 3, 9)  , 'linkedin'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2020, 3, 16) , 'linkedin'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2019, 12, 16), 'youtube'     , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2020, 3, 9)  , 'youtube'     , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2020, 3, 16) , 'youtube'     , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['2'     , datetime.date(2020, 3, 9)  , 'budget_hobby', None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 1                  , 1                  , 1                  , 1                  , 1                  , 1                  , 4                       , 2                      , 1                            , 1                            , 1                            , 1                            , 213876        , 213876        , 213876        , 213876        , 213876        , 213876        , 0                     , 0             , 0             , 0             , 0             ],
                ['2'     , datetime.date(2020, 3, 16) , 'budget_hobby', None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 1                  , 0                  , 1                  , 1                  , 1                  , 1                  , 11                      , 9                      , 1                            , 0                            , 1                            , 1                            , 213876        , None          , 213876        , 213876        , 213876        , 213876        , 0                     , 0             , None          , 0             , 0             ],
                ['2'     , datetime.date(2020, 3, 9)  , 'facebook'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['2'     , datetime.date(2020, 3, 16) , 'facebook'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['2'     , datetime.date(2020, 3, 9)  , 'instagram'   , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , 0.25                     , 0.25                     , 0.25                     , None                     , None                     , None                     , 263.093984962406       , 263.093984962406       , 263.093984962406       , None                   , None                   , None                   , 1                  , 1                  , 1                  , 1                  , 1                  , 1                  , 3                       , 3                      , 1                            , 1                            , 1                            , 1                            , 69983         , 69983         , 69983         , 69983         , 69983         , 69983         , 69983                 , 266           , 266           , 266           , 266           ],
                ['2'     , datetime.date(2020, 3, 16) , 'instagram'   , 0.0                                 , None                                , 0.0                                 , 0.0                                 , 0.25                     , None                     , 0.25                     , None                     , None                     , None                     , 263.093984962406       , None                   , 263.093984962406       , None                   , None                   , None                   , 1                  , 0                  , 1                  , 1                  , 1                  , 1                  , 10                      , 10                     , 1                            , 0                            , 1                            , 1                            , 69983         , None          , 69983         , 69983         , 69983         , 69983         , 69983                 , 266           , None          , 266           , 266           ],
                ['2'     , datetime.date(2020, 3, 9)  , 'linkedin'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['2'     , datetime.date(2020, 3, 16) , 'linkedin'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['2'     , datetime.date(2020, 3, 9)  , 'youtube'     , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['2'     , datetime.date(2020, 3, 16) , 'youtube'     , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['3'     , datetime.date(2019, 12, 16), 'budget_hobby', None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 2                  , 2                  , 2                  , 2                  , 2                  , 2                  , 5                       , 2                      , 1                            , 1                            , 1                            , 1                            , 10131         , 10131         , 10131         , 10131         , 10131         , 10131         , 0                     , 0             , 0             , 0             , 0             ],
                ['3'     , datetime.date(2020, 3, 9)  , 'budget_hobby', None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 1                  , 1                  , 1                  , 1                  , 2                  , 1                  , 89                      , 5                      , 1                            , 1                            , 1                            , 2                            , 201636        , 201636        , 201636        , 201636        , 211767        , 201636        , 0                     , 0             , 0             , 0             , 0             ],
                ['3'     , datetime.date(2020, 3, 16) , 'budget_hobby', None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 1                  , 0                  , 1                  , 1                  , 1                  , 1                  , 91                      , 12                     , 1                            , 0                            , 1                            , 1                            , 201636        , None          , 201636        , 201636        , 201636        , 201636        , 0                     , 0             , None          , 0             , 0             ],
                ['3'     , datetime.date(2019, 12, 16), 'facebook'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , 30.166666666666668     , 30.166666666666668     , 30.166666666666668     , None                   , None                   , None                   , 1                  , 1                  , 1                  , 1                  , 1                  , 1                  , 5                       , 4                      , 1                            , 1                            , 1                            , 1                            , 362           , 362           , 362           , 362           , 362           , 362           , 362                   , None          , None          , None          , None          ],
                ['3'     , datetime.date(2020, 3, 9)  , 'facebook'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 1                  , 0                  , 89                      , 88                     , 0                            , 0                            , 0                            , 1                            , None          , None          , None          , None          , 362           , None          , 362                   , None          , None          , None          , None          ],
                ['3'     , datetime.date(2020, 3, 16) , 'facebook'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , 91                      , 91                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['3'     , datetime.date(2019, 12, 16), 'instagram'   , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['3'     , datetime.date(2020, 3, 9)  , 'instagram'   , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['3'     , datetime.date(2020, 3, 16) , 'instagram'   , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['3'     , datetime.date(2019, 12, 16), 'linkedin'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['3'     , datetime.date(2020, 3, 9)  , 'linkedin'    , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , 0.2                      , 0.2                      , 0.2                      , None                     , None                     , None                     , 70.94432989690722      , 70.94432989690722      , 70.94432989690722      , None                   , None                   , None                   , 1                  , 1                  , 1                  , 1                  , 1                  , 1                  , 6                       , 6                      , 1                            , 1                            , 1                            , 1                            , 34408         , 34408         , 34408         , 34408         , 34408         , 34408         , 34408                 , 485           , 485           , 485           , 485           ],
                ['3'     , datetime.date(2020, 3, 16) , 'linkedin'    , 0.0                                 , None                                , 0.0                                 , 0.0                                 , 0.2                      , None                     , 0.2                      , None                     , None                     , None                     , 70.94432989690722      , None                   , 70.94432989690722      , None                   , None                   , None                   , 1                  , 0                  , 1                  , 1                  , 1                  , 1                  , 13                      , 13                     , 1                            , 0                            , 1                            , 1                            , 34408         , None          , 34408         , 34408         , 34408         , 34408         , 34408                 , 485           , None          , 485           , 485           ],
                ['3'     , datetime.date(2019, 12, 16), 'youtube'     , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['3'     , datetime.date(2020, 3, 9)  , 'youtube'     , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['3'     , datetime.date(2020, 3, 16) , 'youtube'     , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
            ],
            # fmt: on
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField("weekstart", DateType(), True),
                    StructField("category", StringType(), True),
                    StructField(
                        "data_vol_weekday_weekend_ratio_01m", DoubleType(), True
                    ),
                    StructField(
                        "data_vol_weekday_weekend_ratio_01w", DoubleType(), True
                    ),
                    StructField(
                        "data_vol_weekday_weekend_ratio_02m", DoubleType(), True
                    ),
                    StructField(
                        "data_vol_weekday_weekend_ratio_03m", DoubleType(), True
                    ),
                    StructField("latency_4g_weekdays_01m", DoubleType(), True),
                    StructField("latency_4g_weekdays_01w", DoubleType(), True),
                    StructField("latency_4g_weekdays_02m", DoubleType(), True),
                    StructField("latency_4g_weekends_01m", DoubleType(), True),
                    StructField("latency_4g_weekends_01w", DoubleType(), True),
                    StructField("latency_4g_weekends_02m", DoubleType(), True),
                    StructField("speed_4g_weekdays_01m", DoubleType(), True),
                    StructField("speed_4g_weekdays_01w", DoubleType(), True),
                    StructField("speed_4g_weekdays_02m", DoubleType(), True),
                    StructField("speed_4g_weekends_01m", DoubleType(), True),
                    StructField("speed_4g_weekends_01w", DoubleType(), True),
                    StructField("speed_4g_weekends_02m", DoubleType(), True),
                    StructField("accessed_apps_01m", IntegerType(), True),
                    StructField("accessed_apps_01w", IntegerType(), True),
                    StructField("accessed_apps_02m", IntegerType(), True),
                    StructField("accessed_apps_02w", IntegerType(), True),
                    StructField("accessed_apps_03m", IntegerType(), True),
                    StructField("accessed_apps_03w", IntegerType(), True),
                    StructField("days_since_first_usage", IntegerType(), True),
                    StructField("days_since_last_usage", IntegerType(), True),
                    StructField("accessed_apps_num_weeks_01m", LongType(), True),
                    StructField("accessed_apps_num_weeks_01w", LongType(), True),
                    StructField("accessed_apps_num_weeks_02m", LongType(), True),
                    StructField("accessed_apps_num_weeks_03m", LongType(), True),
                    StructField("data_vol_01m", LongType(), True),
                    StructField("data_vol_01w", LongType(), True),
                    StructField("data_vol_02m", LongType(), True),
                    StructField("data_vol_02w", LongType(), True),
                    StructField("data_vol_03m", LongType(), True),
                    StructField("data_vol_03w", LongType(), True),
                    StructField("data_vol_weekday_03m", LongType(), True),
                    StructField("duration_01m", LongType(), True),
                    StructField("duration_01w", LongType(), True),
                    StructField("duration_02m", LongType(), True),
                    StructField("duration_03m", LongType(), True),
                ]
            ),
        )

        assert_df_frame_equal(
            actual_fea_internet_app_usage, expected_fea_internet_app_usage
        )

    def test_pivot_features(
        self, mock_get_end_date, mock_get_start_date, spark_session: SparkSession
    ):
        l5_internet_app_usage = spark_session.createDataFrame(
            # fmt: off
            data = [
              # ['msisdn', 'weekstart'                , 'category'    , 'data_vol_weekday_weekend_ratio_01m', 'data_vol_weekday_weekend_ratio_01w', 'data_vol_weekday_weekend_ratio_02m', 'data_vol_weekday_weekend_ratio_03m', 'latency_4g_weekdays_01m', 'latency_4g_weekdays_01w', 'latency_4g_weekdays_02m', 'latency_4g_weekends_01m', 'latency_4g_weekends_01w', 'latency_4g_weekends_02m', 'speed_4g_weekdays_01m', 'speed_4g_weekdays_01w', 'speed_4g_weekdays_02m', 'speed_4g_weekends_01m', 'speed_4g_weekends_01w', 'speed_4g_weekends_02m', 'accessed_apps_01m', 'accessed_apps_01w', 'accessed_apps_02m', 'accessed_apps_02w', 'accessed_apps_03m', 'accessed_apps_03w', 'days_since_first_usage', 'days_since_last_usage', 'accessed_apps_num_weeks_01m', 'accessed_apps_num_weeks_01w', 'accessed_apps_num_weeks_02m', 'accessed_apps_num_weeks_03m', 'data_vol_01m', 'data_vol_01w', 'data_vol_02m', 'data_vol_02w', 'data_vol_03m', 'data_vol_03w', 'data_vol_weekday_03m', 'duration_01m', 'duration_01w', 'duration_02m', 'duration_03m'],
                ['1'     , datetime.date(2019, 12, 16), 'budget_hobby', None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 2                  , 2                  , 2                  , 2                  , 2                  , 2                  , 5                       , 2                      , 1                            , 1                            , 1                            , 1                            , 1124          , 1124          , 1124          , 1124          , 1124          , 1124          , 0                     , 0             , 0             , 0             , 0             ],
                ['1'     , datetime.date(2020, 3, 9)  , 'budget_hobby', None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 1                  , 1                  , 1                  , 1                  , 2                  , 1                  , 89                      , 1                      , 1                            , 1                            , 1                            , 2                            , 167548        , 167548        , 167548        , 167548        , 168672        , 167548        , 0                     , 0             , 0             , 0             , 0             ],
                ['1'     , datetime.date(2020, 3, 16) , 'budget_hobby', 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , 0.05                     , 0.05                     , 0.05                     , None                     , None                     , None                     , 378.625641025641       , 378.625641025641       , 378.625641025641       , None                   , None                   , None                   , 1                  , 1                  , 1                  , 1                  , 1                  , 1                  , 91                      , 6                      , 2                            , 1                            , 2                            , 2                            , 446237        , 278689        , 446237        , 446237        , 446237        , 446237        , 147664                , 390           , 390           , 390           , 390           ],
                ['1'     , datetime.date(2019, 12, 16), 'facebook'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2020, 3, 9)  , 'facebook'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2020, 3, 16) , 'facebook'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2019, 12, 16), 'instagram'   , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2020, 3, 9)  , 'instagram'   , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2020, 3, 16) , 'instagram'   , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2019, 12, 16), 'linkedin'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2020, 3, 9)  , 'linkedin'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2020, 3, 16) , 'linkedin'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2019, 12, 16), 'youtube'     , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2020, 3, 9)  , 'youtube'     , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['1'     , datetime.date(2020, 3, 16) , 'youtube'     , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['2'     , datetime.date(2020, 3, 9)  , 'budget_hobby', None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 1                  , 1                  , 1                  , 1                  , 1                  , 1                  , 4                       , 2                      , 1                            , 1                            , 1                            , 1                            , 213876        , 213876        , 213876        , 213876        , 213876        , 213876        , 0                     , 0             , 0             , 0             , 0             ],
                ['2'     , datetime.date(2020, 3, 16) , 'budget_hobby', None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 1                  , 0                  , 1                  , 1                  , 1                  , 1                  , 11                      , 9                      , 1                            , 0                            , 1                            , 1                            , 213876        , None          , 213876        , 213876        , 213876        , 213876        , 0                     , 0             , None          , 0             , 0             ],
                ['2'     , datetime.date(2020, 3, 9)  , 'facebook'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['2'     , datetime.date(2020, 3, 16) , 'facebook'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['2'     , datetime.date(2020, 3, 9)  , 'instagram'   , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , 0.25                     , 0.25                     , 0.25                     , None                     , None                     , None                     , 263.093984962406       , 263.093984962406       , 263.093984962406       , None                   , None                   , None                   , 1                  , 1                  , 1                  , 1                  , 1                  , 1                  , 3                       , 3                      , 1                            , 1                            , 1                            , 1                            , 69983         , 69983         , 69983         , 69983         , 69983         , 69983         , 69983                 , 266           , 266           , 266           , 266           ],
                ['2'     , datetime.date(2020, 3, 16) , 'instagram'   , 0.0                                 , None                                , 0.0                                 , 0.0                                 , 0.25                     , None                     , 0.25                     , None                     , None                     , None                     , 263.093984962406       , None                   , 263.093984962406       , None                   , None                   , None                   , 1                  , 0                  , 1                  , 1                  , 1                  , 1                  , 10                      , 10                     , 1                            , 0                            , 1                            , 1                            , 69983         , None          , 69983         , 69983         , 69983         , 69983         , 69983                 , 266           , None          , 266           , 266           ],
                ['2'     , datetime.date(2020, 3, 9)  , 'linkedin'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['2'     , datetime.date(2020, 3, 16) , 'linkedin'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['2'     , datetime.date(2020, 3, 9)  , 'youtube'     , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['2'     , datetime.date(2020, 3, 16) , 'youtube'     , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['3'     , datetime.date(2019, 12, 16), 'budget_hobby', None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 2                  , 2                  , 2                  , 2                  , 2                  , 2                  , 5                       , 2                      , 1                            , 1                            , 1                            , 1                            , 10131         , 10131         , 10131         , 10131         , 10131         , 10131         , 0                     , 0             , 0             , 0             , 0             ],
                ['3'     , datetime.date(2020, 3, 9)  , 'budget_hobby', None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 1                  , 1                  , 1                  , 1                  , 2                  , 1                  , 89                      , 5                      , 1                            , 1                            , 1                            , 2                            , 201636        , 201636        , 201636        , 201636        , 211767        , 201636        , 0                     , 0             , 0             , 0             , 0             ],
                ['3'     , datetime.date(2020, 3, 16) , 'budget_hobby', None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 1                  , 0                  , 1                  , 1                  , 1                  , 1                  , 91                      , 12                     , 1                            , 0                            , 1                            , 1                            , 201636        , None          , 201636        , 201636        , 201636        , 201636        , 0                     , 0             , None          , 0             , 0             ],
                ['3'     , datetime.date(2019, 12, 16), 'facebook'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , 30.166666666666668     , 30.166666666666668     , 30.166666666666668     , None                   , None                   , None                   , 1                  , 1                  , 1                  , 1                  , 1                  , 1                  , 5                       , 4                      , 1                            , 1                            , 1                            , 1                            , 362           , 362           , 362           , 362           , 362           , 362           , 362                   , None          , None          , None          , None          ],
                ['3'     , datetime.date(2020, 3, 9)  , 'facebook'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 1                  , 0                  , 89                      , 88                     , 0                            , 0                            , 0                            , 1                            , None          , None          , None          , None          , 362           , None          , 362                   , None          , None          , None          , None          ],
                ['3'     , datetime.date(2020, 3, 16) , 'facebook'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , 91                      , 91                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['3'     , datetime.date(2019, 12, 16), 'instagram'   , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['3'     , datetime.date(2020, 3, 9)  , 'instagram'   , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['3'     , datetime.date(2020, 3, 16) , 'instagram'   , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['3'     , datetime.date(2019, 12, 16), 'linkedin'    , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['3'     , datetime.date(2020, 3, 9)  , 'linkedin'    , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , 0.2                      , 0.2                      , 0.2                      , None                     , None                     , None                     , 70.94432989690722      , 70.94432989690722      , 70.94432989690722      , None                   , None                   , None                   , 1                  , 1                  , 1                  , 1                  , 1                  , 1                  , 6                       , 6                      , 1                            , 1                            , 1                            , 1                            , 34408         , 34408         , 34408         , 34408         , 34408         , 34408         , 34408                 , 485           , 485           , 485           , 485           ],
                ['3'     , datetime.date(2020, 3, 16) , 'linkedin'    , 0.0                                 , None                                , 0.0                                 , 0.0                                 , 0.2                      , None                     , 0.2                      , None                     , None                     , None                     , 70.94432989690722      , None                   , 70.94432989690722      , None                   , None                   , None                   , 1                  , 0                  , 1                  , 1                  , 1                  , 1                  , 13                      , 13                     , 1                            , 0                            , 1                            , 1                            , 34408         , None          , 34408         , 34408         , 34408         , 34408         , 34408                 , 485           , None          , 485           , 485           ],
                ['3'     , datetime.date(2019, 12, 16), 'youtube'     , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['3'     , datetime.date(2020, 3, 9)  , 'youtube'     , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
                ['3'     , datetime.date(2020, 3, 16) , 'youtube'     , None                                , None                                , None                                , None                                , None                     , None                     , None                     , None                     , None                     , None                     , None                   , None                   , None                   , None                   , None                   , None                   , 0                  , 0                  , 0                  , 0                  , 0                  , 0                  , -1                      , -1                     , 0                            , 0                            , 0                            , 0                            , None          , None          , None          , None          , None          , None          , None                  , None          , None          , None          , None          ],
            ],
            # fmt: on
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField("weekstart", DateType(), True),
                    StructField("category", StringType(), True),
                    StructField(
                        "data_vol_weekday_weekend_ratio_01m", DoubleType(), True
                    ),
                    StructField(
                        "data_vol_weekday_weekend_ratio_01w", DoubleType(), True
                    ),
                    StructField(
                        "data_vol_weekday_weekend_ratio_02m", DoubleType(), True
                    ),
                    StructField(
                        "data_vol_weekday_weekend_ratio_03m", DoubleType(), True
                    ),
                    StructField("latency_4g_weekdays_01m", DoubleType(), True),
                    StructField("latency_4g_weekdays_01w", DoubleType(), True),
                    StructField("latency_4g_weekdays_02m", DoubleType(), True),
                    StructField("latency_4g_weekends_01m", DoubleType(), True),
                    StructField("latency_4g_weekends_01w", DoubleType(), True),
                    StructField("latency_4g_weekends_02m", DoubleType(), True),
                    StructField("speed_4g_weekdays_01m", DoubleType(), True),
                    StructField("speed_4g_weekdays_01w", DoubleType(), True),
                    StructField("speed_4g_weekdays_02m", DoubleType(), True),
                    StructField("speed_4g_weekends_01m", DoubleType(), True),
                    StructField("speed_4g_weekends_01w", DoubleType(), True),
                    StructField("speed_4g_weekends_02m", DoubleType(), True),
                    StructField("accessed_apps_01m", IntegerType(), True),
                    StructField("accessed_apps_01w", IntegerType(), True),
                    StructField("accessed_apps_02m", IntegerType(), True),
                    StructField("accessed_apps_02w", IntegerType(), True),
                    StructField("accessed_apps_03m", IntegerType(), True),
                    StructField("accessed_apps_03w", IntegerType(), True),
                    StructField("days_since_first_usage", IntegerType(), True),
                    StructField("days_since_last_usage", IntegerType(), True),
                    StructField("accessed_apps_num_weeks_01m", LongType(), True),
                    StructField("accessed_apps_num_weeks_01w", LongType(), True),
                    StructField("accessed_apps_num_weeks_02m", LongType(), True),
                    StructField("accessed_apps_num_weeks_03m", LongType(), True),
                    StructField("data_vol_01m", LongType(), True),
                    StructField("data_vol_01w", LongType(), True),
                    StructField("data_vol_02m", LongType(), True),
                    StructField("data_vol_02w", LongType(), True),
                    StructField("data_vol_03m", LongType(), True),
                    StructField("data_vol_03w", LongType(), True),
                    StructField("data_vol_weekday_03m", LongType(), True),
                    StructField("duration_01m", LongType(), True),
                    StructField("duration_01w", LongType(), True),
                    StructField("duration_02m", LongType(), True),
                    StructField("duration_03m", LongType(), True),
                ]
            ),
        )

        actual_fea_internet_app_usage_pivot = pivot_features(
            fea_df=l5_internet_app_usage,
            feature_mode="all",
            required_output_features=[],
            shuffle_partitions=1,
        )

        expected_fea_internet_app_usage_pivot = spark_session.createDataFrame(
            # fmt: off
            data = [
              # ['msisdn', 'weekstart'                , 'budget_hobby_data_vol_weekday_weekend_ratio_01m', 'budget_hobby_data_vol_weekday_weekend_ratio_01w', 'budget_hobby_data_vol_weekday_weekend_ratio_02m', 'budget_hobby_data_vol_weekday_weekend_ratio_03m', 'budget_hobby_latency_4g_weekdays_01m', 'budget_hobby_latency_4g_weekdays_01w', 'budget_hobby_latency_4g_weekdays_02m', 'budget_hobby_latency_4g_weekends_01m', 'budget_hobby_latency_4g_weekends_01w', 'budget_hobby_latency_4g_weekends_02m', 'budget_hobby_speed_4g_weekdays_01m', 'budget_hobby_speed_4g_weekdays_01w', 'budget_hobby_speed_4g_weekdays_02m', 'budget_hobby_speed_4g_weekends_01m', 'budget_hobby_speed_4g_weekends_01w', 'budget_hobby_speed_4g_weekends_02m', 'facebook_data_vol_weekday_weekend_ratio_01m', 'facebook_data_vol_weekday_weekend_ratio_01w', 'facebook_data_vol_weekday_weekend_ratio_02m', 'facebook_data_vol_weekday_weekend_ratio_03m', 'facebook_latency_4g_weekdays_01m', 'facebook_latency_4g_weekdays_01w', 'facebook_latency_4g_weekdays_02m', 'facebook_latency_4g_weekends_01m', 'facebook_latency_4g_weekends_01w', 'facebook_latency_4g_weekends_02m', 'facebook_speed_4g_weekdays_01m', 'facebook_speed_4g_weekdays_01w', 'facebook_speed_4g_weekdays_02m', 'facebook_speed_4g_weekends_01m', 'facebook_speed_4g_weekends_01w', 'facebook_speed_4g_weekends_02m', 'instagram_data_vol_weekday_weekend_ratio_01m', 'instagram_data_vol_weekday_weekend_ratio_01w', 'instagram_data_vol_weekday_weekend_ratio_02m', 'instagram_data_vol_weekday_weekend_ratio_03m', 'instagram_latency_4g_weekdays_01m', 'instagram_latency_4g_weekdays_01w', 'instagram_latency_4g_weekdays_02m', 'instagram_latency_4g_weekends_01m', 'instagram_latency_4g_weekends_01w', 'instagram_latency_4g_weekends_02m', 'instagram_speed_4g_weekdays_01m', 'instagram_speed_4g_weekdays_01w', 'instagram_speed_4g_weekdays_02m', 'instagram_speed_4g_weekends_01m', 'instagram_speed_4g_weekends_01w', 'instagram_speed_4g_weekends_02m', 'linkedin_data_vol_weekday_weekend_ratio_01m', 'linkedin_data_vol_weekday_weekend_ratio_01w', 'linkedin_data_vol_weekday_weekend_ratio_02m', 'linkedin_data_vol_weekday_weekend_ratio_03m', 'linkedin_latency_4g_weekdays_01m', 'linkedin_latency_4g_weekdays_01w', 'linkedin_latency_4g_weekdays_02m', 'linkedin_latency_4g_weekends_01m', 'linkedin_latency_4g_weekends_01w', 'linkedin_latency_4g_weekends_02m', 'linkedin_speed_4g_weekdays_01m', 'linkedin_speed_4g_weekdays_01w', 'linkedin_speed_4g_weekdays_02m', 'linkedin_speed_4g_weekends_01m', 'linkedin_speed_4g_weekends_01w', 'linkedin_speed_4g_weekends_02m', 'youtube_data_vol_weekday_weekend_ratio_01m', 'youtube_data_vol_weekday_weekend_ratio_01w', 'youtube_data_vol_weekday_weekend_ratio_02m', 'youtube_data_vol_weekday_weekend_ratio_03m', 'youtube_latency_4g_weekdays_01m', 'youtube_latency_4g_weekdays_01w', 'youtube_latency_4g_weekdays_02m', 'youtube_latency_4g_weekends_01m', 'youtube_latency_4g_weekends_01w', 'youtube_latency_4g_weekends_02m', 'youtube_speed_4g_weekdays_01m', 'youtube_speed_4g_weekdays_01w', 'youtube_speed_4g_weekdays_02m', 'youtube_speed_4g_weekends_01m', 'youtube_speed_4g_weekends_01w', 'youtube_speed_4g_weekends_02m', 'budget_hobby_days_since_first_usage', 'budget_hobby_days_since_last_usage', 'facebook_days_since_first_usage', 'facebook_days_since_last_usage', 'instagram_days_since_first_usage', 'instagram_days_since_last_usage', 'linkedin_days_since_first_usage', 'linkedin_days_since_last_usage', 'youtube_days_since_first_usage', 'youtube_days_since_last_usage', 'budget_hobby_accessed_apps_01m', 'budget_hobby_accessed_apps_01w', 'budget_hobby_accessed_apps_02m', 'budget_hobby_accessed_apps_02w', 'budget_hobby_accessed_apps_03m', 'budget_hobby_accessed_apps_03w', 'budget_hobby_accessed_apps_num_weeks_01m', 'budget_hobby_accessed_apps_num_weeks_01w', 'budget_hobby_accessed_apps_num_weeks_02m', 'budget_hobby_accessed_apps_num_weeks_03m', 'budget_hobby_data_vol_01m', 'budget_hobby_data_vol_01w', 'budget_hobby_data_vol_02m', 'budget_hobby_data_vol_02w', 'budget_hobby_data_vol_03m', 'budget_hobby_data_vol_03w', 'budget_hobby_data_vol_weekday_03m', 'budget_hobby_duration_01m', 'budget_hobby_duration_01w', 'budget_hobby_duration_02m', 'budget_hobby_duration_03m', 'facebook_accessed_apps_01m', 'facebook_accessed_apps_01w', 'facebook_accessed_apps_02m', 'facebook_accessed_apps_02w', 'facebook_accessed_apps_03m', 'facebook_accessed_apps_03w', 'facebook_accessed_apps_num_weeks_01m', 'facebook_accessed_apps_num_weeks_01w', 'facebook_accessed_apps_num_weeks_02m', 'facebook_accessed_apps_num_weeks_03m', 'facebook_data_vol_01m', 'facebook_data_vol_01w', 'facebook_data_vol_02m', 'facebook_data_vol_02w', 'facebook_data_vol_03m', 'facebook_data_vol_03w', 'facebook_data_vol_weekday_03m', 'facebook_duration_01m', 'facebook_duration_01w', 'facebook_duration_02m', 'facebook_duration_03m', 'instagram_accessed_apps_01m', 'instagram_accessed_apps_01w', 'instagram_accessed_apps_02m', 'instagram_accessed_apps_02w', 'instagram_accessed_apps_03m', 'instagram_accessed_apps_03w', 'instagram_accessed_apps_num_weeks_01m', 'instagram_accessed_apps_num_weeks_01w', 'instagram_accessed_apps_num_weeks_02m', 'instagram_accessed_apps_num_weeks_03m', 'instagram_data_vol_01m', 'instagram_data_vol_01w', 'instagram_data_vol_02m', 'instagram_data_vol_02w', 'instagram_data_vol_03m', 'instagram_data_vol_03w', 'instagram_data_vol_weekday_03m', 'instagram_duration_01m', 'instagram_duration_01w', 'instagram_duration_02m', 'instagram_duration_03m', 'linkedin_accessed_apps_01m', 'linkedin_accessed_apps_01w', 'linkedin_accessed_apps_02m', 'linkedin_accessed_apps_02w', 'linkedin_accessed_apps_03m', 'linkedin_accessed_apps_03w', 'linkedin_accessed_apps_num_weeks_01m', 'linkedin_accessed_apps_num_weeks_01w', 'linkedin_accessed_apps_num_weeks_02m', 'linkedin_accessed_apps_num_weeks_03m', 'linkedin_data_vol_01m', 'linkedin_data_vol_01w', 'linkedin_data_vol_02m', 'linkedin_data_vol_02w', 'linkedin_data_vol_03m', 'linkedin_data_vol_03w', 'linkedin_data_vol_weekday_03m', 'linkedin_duration_01m', 'linkedin_duration_01w', 'linkedin_duration_02m', 'linkedin_duration_03m', 'youtube_accessed_apps_01m', 'youtube_accessed_apps_01w', 'youtube_accessed_apps_02m', 'youtube_accessed_apps_02w', 'youtube_accessed_apps_03m', 'youtube_accessed_apps_03w', 'youtube_accessed_apps_num_weeks_01m', 'youtube_accessed_apps_num_weeks_01w', 'youtube_accessed_apps_num_weeks_02m', 'youtube_accessed_apps_num_weeks_03m', 'youtube_data_vol_01m', 'youtube_data_vol_01w', 'youtube_data_vol_02m', 'youtube_data_vol_02w', 'youtube_data_vol_03m', 'youtube_data_vol_03w', 'youtube_data_vol_weekday_03m', 'youtube_duration_01m', 'youtube_duration_01w', 'youtube_duration_02m', 'youtube_duration_03m'],
                ['1'     , datetime.date(2019, 12, 16), None                                             , None                                             , None                                             , None                                             , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , None                                         , None                                         , None                                         , None                                         , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , None                                          , None                                          , None                                          , None                                          , 0.0                                , 0.0                                , 0.0                                , 0.0                                , 0.0                                , 0.0                                , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , None                                         , None                                         , None                                         , None                                         , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , None                                        , None                                        , None                                        , None                                        , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 5                                    , 2                                   , -1                               , -1                              , -1                                , -1                               , -1                               , -1                              , -1                              , -1                             , 2                               , 2                               , 2                               , 2                               , 2                               , 2                               , 1                                         , 1                                         , 1                                         , 1                                         , 1124                       , 1124                       , 1124                       , 1124                       , 1124                       , 1124                       , 0                                  , 0                          , 0                          , 0                          , 0                          , 0                           , 0                           , 0                           , 0                           , 0                           , 0                           , 0                                     , 0                                     , 0                                     , 0                                     , 0                      , 0                      , 0                      , 0                      , 0                      , 0                      , 0                              , 0                      , 0                      , 0                      , 0                      , 0                            , 0                            , 0                            , 0                            , 0                            , 0                            , 0                                      , 0                                      , 0                                      , 0                                      , 0                       , 0                       , 0                       , 0                       , 0                       , 0                       , 0                               , 0                       , 0                       , 0                       , 0                       , 0                           , 0                           , 0                           , 0                           , 0                           , 0                           , 0                                     , 0                                     , 0                                     , 0                                     , 0                      , 0                      , 0                      , 0                      , 0                      , 0                      , 0                              , 0                      , 0                      , 0                      , 0                      , 0                          , 0                          , 0                          , 0                          , 0                          , 0                          , 0                                    , 0                                    , 0                                    , 0                                    , 0                     , 0                     , 0                     , 0                     , 0                     , 0                     , 0                             , 0                     , 0                     , 0                     , 0                     ],
                ['1'     , datetime.date(2020, 3, 9)  , None                                             , None                                             , None                                             , None                                             , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , None                                         , None                                         , None                                         , None                                         , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , None                                          , None                                          , None                                          , None                                          , 0.0                                , 0.0                                , 0.0                                , 0.0                                , 0.0                                , 0.0                                , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , None                                         , None                                         , None                                         , None                                         , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , None                                        , None                                        , None                                        , None                                        , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 89                                   , 1                                   , -1                               , -1                              , -1                                , -1                               , -1                               , -1                              , -1                              , -1                             , 1                               , 1                               , 1                               , 1                               , 2                               , 1                               , 1                                         , 1                                         , 1                                         , 2                                         , 167548                     , 167548                     , 167548                     , 167548                     , 168672                     , 167548                     , 0                                  , 0                          , 0                          , 0                          , 0                          , 0                           , 0                           , 0                           , 0                           , 0                           , 0                           , 0                                     , 0                                     , 0                                     , 0                                     , 0                      , 0                      , 0                      , 0                      , 0                      , 0                      , 0                              , 0                      , 0                      , 0                      , 0                      , 0                            , 0                            , 0                            , 0                            , 0                            , 0                            , 0                                      , 0                                      , 0                                      , 0                                      , 0                       , 0                       , 0                       , 0                       , 0                       , 0                       , 0                               , 0                       , 0                       , 0                       , 0                       , 0                           , 0                           , 0                           , 0                           , 0                           , 0                           , 0                                     , 0                                     , 0                                     , 0                                     , 0                      , 0                      , 0                      , 0                      , 0                      , 0                      , 0                              , 0                      , 0                      , 0                      , 0                      , 0                          , 0                          , 0                          , 0                          , 0                          , 0                          , 0                                    , 0                                    , 0                                    , 0                                    , 0                     , 0                     , 0                     , 0                     , 0                     , 0                     , 0                             , 0                     , 0                     , 0                     , 0                     ],
                ['1'     , datetime.date(2020, 3, 16) , 0.0                                              , 0.0                                              , 0.0                                              , 0.0                                              , 0.05                                  , 0.05                                  , 0.05                                  , 0.0                                   , 0.0                                   , 0.0                                   , 378.625641025641                    , 378.625641025641                    , 378.625641025641                    , 0.0                                 , 0.0                                 , 0.0                                 , None                                         , None                                         , None                                         , None                                         , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , None                                          , None                                          , None                                          , None                                          , 0.0                                , 0.0                                , 0.0                                , 0.0                                , 0.0                                , 0.0                                , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , None                                         , None                                         , None                                         , None                                         , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , None                                        , None                                        , None                                        , None                                        , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 91                                   , 6                                   , -1                               , -1                              , -1                                , -1                               , -1                               , -1                              , -1                              , -1                             , 1                               , 1                               , 1                               , 1                               , 1                               , 1                               , 2                                         , 1                                         , 2                                         , 2                                         , 446237                     , 278689                     , 446237                     , 446237                     , 446237                     , 446237                     , 147664                             , 390                        , 390                        , 390                        , 390                        , 0                           , 0                           , 0                           , 0                           , 0                           , 0                           , 0                                     , 0                                     , 0                                     , 0                                     , 0                      , 0                      , 0                      , 0                      , 0                      , 0                      , 0                              , 0                      , 0                      , 0                      , 0                      , 0                            , 0                            , 0                            , 0                            , 0                            , 0                            , 0                                      , 0                                      , 0                                      , 0                                      , 0                       , 0                       , 0                       , 0                       , 0                       , 0                       , 0                               , 0                       , 0                       , 0                       , 0                       , 0                           , 0                           , 0                           , 0                           , 0                           , 0                           , 0                                     , 0                                     , 0                                     , 0                                     , 0                      , 0                      , 0                      , 0                      , 0                      , 0                      , 0                              , 0                      , 0                      , 0                      , 0                      , 0                          , 0                          , 0                          , 0                          , 0                          , 0                          , 0                                    , 0                                    , 0                                    , 0                                    , 0                     , 0                     , 0                     , 0                     , 0                     , 0                     , 0                             , 0                     , 0                     , 0                     , 0                     ],
                ['2'     , datetime.date(2020, 3, 9)  , None                                             , None                                             , None                                             , None                                             , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , None                                         , None                                         , None                                         , None                                         , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                                           , 0.0                                           , 0.0                                           , 0.0                                           , 0.25                               , 0.25                               , 0.25                               , 0.0                                , 0.0                                , 0.0                                , 263.093984962406                 , 263.093984962406                 , 263.093984962406                 , 0.0                              , 0.0                              , 0.0                              , None                                         , None                                         , None                                         , None                                         , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , None                                        , None                                        , None                                        , None                                        , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 4                                    , 2                                   , -1                               , -1                              , 3                                 , 3                                , -1                               , -1                              , -1                              , -1                             , 1                               , 1                               , 1                               , 1                               , 1                               , 1                               , 1                                         , 1                                         , 1                                         , 1                                         , 213876                     , 213876                     , 213876                     , 213876                     , 213876                     , 213876                     , 0                                  , 0                          , 0                          , 0                          , 0                          , 0                           , 0                           , 0                           , 0                           , 0                           , 0                           , 0                                     , 0                                     , 0                                     , 0                                     , 0                      , 0                      , 0                      , 0                      , 0                      , 0                      , 0                              , 0                      , 0                      , 0                      , 0                      , 1                            , 1                            , 1                            , 1                            , 1                            , 1                            , 1                                      , 1                                      , 1                                      , 1                                      , 69983                   , 69983                   , 69983                   , 69983                   , 69983                   , 69983                   , 69983                           , 266                     , 266                     , 266                     , 266                     , 0                           , 0                           , 0                           , 0                           , 0                           , 0                           , 0                                     , 0                                     , 0                                     , 0                                     , 0                      , 0                      , 0                      , 0                      , 0                      , 0                      , 0                              , 0                      , 0                      , 0                      , 0                      , 0                          , 0                          , 0                          , 0                          , 0                          , 0                          , 0                                    , 0                                    , 0                                    , 0                                    , 0                     , 0                     , 0                     , 0                     , 0                     , 0                     , 0                             , 0                     , 0                     , 0                     , 0                     ],
                ['2'     , datetime.date(2020, 3, 16) , None                                             , None                                             , None                                             , None                                             , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , None                                         , None                                         , None                                         , None                                         , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                                           , None                                          , 0.0                                           , 0.0                                           , 0.25                               , 0.0                                , 0.25                               , 0.0                                , 0.0                                , 0.0                                , 263.093984962406                 , 0.0                              , 263.093984962406                 , 0.0                              , 0.0                              , 0.0                              , None                                         , None                                         , None                                         , None                                         , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , None                                        , None                                        , None                                        , None                                        , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 11                                   , 9                                   , -1                               , -1                              , 10                                , 10                               , -1                               , -1                              , -1                              , -1                             , 1                               , 0                               , 1                               , 1                               , 1                               , 1                               , 1                                         , 0                                         , 1                                         , 1                                         , 213876                     , 0                          , 213876                     , 213876                     , 213876                     , 213876                     , 0                                  , 0                          , 0                          , 0                          , 0                          , 0                           , 0                           , 0                           , 0                           , 0                           , 0                           , 0                                     , 0                                     , 0                                     , 0                                     , 0                      , 0                      , 0                      , 0                      , 0                      , 0                      , 0                              , 0                      , 0                      , 0                      , 0                      , 1                            , 0                            , 1                            , 1                            , 1                            , 1                            , 1                                      , 0                                      , 1                                      , 1                                      , 69983                   , 0                       , 69983                   , 69983                   , 69983                   , 69983                   , 69983                           , 266                     , 0                       , 266                     , 266                     , 0                           , 0                           , 0                           , 0                           , 0                           , 0                           , 0                                     , 0                                     , 0                                     , 0                                     , 0                      , 0                      , 0                      , 0                      , 0                      , 0                      , 0                              , 0                      , 0                      , 0                      , 0                      , 0                          , 0                          , 0                          , 0                          , 0                          , 0                          , 0                                    , 0                                    , 0                                    , 0                                    , 0                     , 0                     , 0                     , 0                     , 0                     , 0                     , 0                             , 0                     , 0                     , 0                     , 0                     ],
                ['3'     , datetime.date(2019, 12, 16), None                                             , None                                             , None                                             , None                                             , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , None                                         , None                                         , None                                         , None                                         , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 30.166666666666668              , 30.166666666666668              , 30.166666666666668              , 0.0                             , 0.0                             , 0.0                             , None                                          , None                                          , None                                          , None                                          , 0.0                                , 0.0                                , 0.0                                , 0.0                                , 0.0                                , 0.0                                , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , None                                         , None                                         , None                                         , None                                         , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , None                                        , None                                        , None                                        , None                                        , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 5                                    , 2                                   , 5                                , 4                               , -1                                , -1                               , -1                               , -1                              , -1                              , -1                             , 2                               , 2                               , 2                               , 2                               , 2                               , 2                               , 1                                         , 1                                         , 1                                         , 1                                         , 10131                      , 10131                      , 10131                      , 10131                      , 10131                      , 10131                      , 0                                  , 0                          , 0                          , 0                          , 0                          , 1                           , 1                           , 1                           , 1                           , 1                           , 1                           , 1                                     , 1                                     , 1                                     , 1                                     , 362                    , 362                    , 362                    , 362                    , 362                    , 362                    , 362                            , 0                      , 0                      , 0                      , 0                      , 0                            , 0                            , 0                            , 0                            , 0                            , 0                            , 0                                      , 0                                      , 0                                      , 0                                      , 0                       , 0                       , 0                       , 0                       , 0                       , 0                       , 0                               , 0                       , 0                       , 0                       , 0                       , 0                           , 0                           , 0                           , 0                           , 0                           , 0                           , 0                                     , 0                                     , 0                                     , 0                                     , 0                      , 0                      , 0                      , 0                      , 0                      , 0                      , 0                              , 0                      , 0                      , 0                      , 0                      , 0                          , 0                          , 0                          , 0                          , 0                          , 0                          , 0                                    , 0                                    , 0                                    , 0                                    , 0                     , 0                     , 0                     , 0                     , 0                     , 0                     , 0                             , 0                     , 0                     , 0                     , 0                     ],
                ['3'     , datetime.date(2020, 3, 9)  , None                                             , None                                             , None                                             , None                                             , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , None                                         , None                                         , None                                         , None                                         , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , None                                          , None                                          , None                                          , None                                          , 0.0                                , 0.0                                , 0.0                                , 0.0                                , 0.0                                , 0.0                                , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                                          , 0.0                                          , 0.0                                          , 0.0                                          , 0.2                               , 0.2                               , 0.2                               , 0.0                               , 0.0                               , 0.0                               , 70.94432989690722               , 70.94432989690722               , 70.94432989690722               , 0.0                             , 0.0                             , 0.0                             , None                                        , None                                        , None                                        , None                                        , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 89                                   , 5                                   , 89                               , 88                              , -1                                , -1                               , 6                                , 6                               , -1                              , -1                             , 1                               , 1                               , 1                               , 1                               , 2                               , 1                               , 1                                         , 1                                         , 1                                         , 2                                         , 201636                     , 201636                     , 201636                     , 201636                     , 211767                     , 201636                     , 0                                  , 0                          , 0                          , 0                          , 0                          , 0                           , 0                           , 0                           , 0                           , 1                           , 0                           , 0                                     , 0                                     , 0                                     , 1                                     , 0                      , 0                      , 0                      , 0                      , 362                    , 0                      , 362                            , 0                      , 0                      , 0                      , 0                      , 0                            , 0                            , 0                            , 0                            , 0                            , 0                            , 0                                      , 0                                      , 0                                      , 0                                      , 0                       , 0                       , 0                       , 0                       , 0                       , 0                       , 0                               , 0                       , 0                       , 0                       , 0                       , 1                           , 1                           , 1                           , 1                           , 1                           , 1                           , 1                                     , 1                                     , 1                                     , 1                                     , 34408                  , 34408                  , 34408                  , 34408                  , 34408                  , 34408                  , 34408                          , 485                    , 485                    , 485                    , 485                    , 0                          , 0                          , 0                          , 0                          , 0                          , 0                          , 0                                    , 0                                    , 0                                    , 0                                    , 0                     , 0                     , 0                     , 0                     , 0                     , 0                     , 0                             , 0                     , 0                     , 0                     , 0                     ],
                ['3'     , datetime.date(2020, 3, 16) , None                                             , None                                             , None                                             , None                                             , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                   , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , 0.0                                 , None                                         , None                                         , None                                         , None                                         , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                               , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , 0.0                             , None                                          , None                                          , None                                          , None                                          , 0.0                                , 0.0                                , 0.0                                , 0.0                                , 0.0                                , 0.0                                , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                                          , None                                         , 0.0                                          , 0.0                                          , 0.2                               , 0.0                               , 0.2                               , 0.0                               , 0.0                               , 0.0                               , 70.94432989690722               , 0.0                             , 70.94432989690722               , 0.0                             , 0.0                             , 0.0                             , None                                        , None                                        , None                                        , None                                        , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                              , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 0.0                            , 91                                   , 12                                  , 91                               , 91                              , -1                                , -1                               , 13                               , 13                              , -1                              , -1                             , 1                               , 0                               , 1                               , 1                               , 1                               , 1                               , 1                                         , 0                                         , 1                                         , 1                                         , 201636                     , 0                          , 201636                     , 201636                     , 201636                     , 201636                     , 0                                  , 0                          , 0                          , 0                          , 0                          , 0                           , 0                           , 0                           , 0                           , 0                           , 0                           , 0                                     , 0                                     , 0                                     , 0                                     , 0                      , 0                      , 0                      , 0                      , 0                      , 0                      , 0                              , 0                      , 0                      , 0                      , 0                      , 0                            , 0                            , 0                            , 0                            , 0                            , 0                            , 0                                      , 0                                      , 0                                      , 0                                      , 0                       , 0                       , 0                       , 0                       , 0                       , 0                       , 0                               , 0                       , 0                       , 0                       , 0                       , 1                           , 0                           , 1                           , 1                           , 1                           , 1                           , 1                                     , 0                                     , 1                                     , 1                                     , 34408                  , 0                      , 34408                  , 34408                  , 34408                  , 34408                  , 34408                          , 485                    , 0                      , 485                    , 485                    , 0                          , 0                          , 0                          , 0                          , 0                          , 0                          , 0                                    , 0                                    , 0                                    , 0                                    , 0                     , 0                     , 0                     , 0                     , 0                     , 0                     , 0                             , 0                     , 0                     , 0                     , 0                     ],
            ],
            # fmt: on
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField("weekstart", DateType(), True),
                    StructField(
                        "fea_int_app_usage_budget_hobby_data_vol_weekday_weekend_ratio_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_data_vol_weekday_weekend_ratio_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_data_vol_weekday_weekend_ratio_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_data_vol_weekday_weekend_ratio_03m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_latency_4g_weekdays_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_latency_4g_weekdays_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_latency_4g_weekdays_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_latency_4g_weekends_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_latency_4g_weekends_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_latency_4g_weekends_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_speed_4g_weekdays_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_speed_4g_weekdays_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_speed_4g_weekdays_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_speed_4g_weekends_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_speed_4g_weekends_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_speed_4g_weekends_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_data_vol_weekday_weekend_ratio_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_data_vol_weekday_weekend_ratio_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_data_vol_weekday_weekend_ratio_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_data_vol_weekday_weekend_ratio_03m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_latency_4g_weekdays_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_latency_4g_weekdays_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_latency_4g_weekdays_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_latency_4g_weekends_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_latency_4g_weekends_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_latency_4g_weekends_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_speed_4g_weekdays_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_speed_4g_weekdays_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_speed_4g_weekdays_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_speed_4g_weekends_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_speed_4g_weekends_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_speed_4g_weekends_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_data_vol_weekday_weekend_ratio_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_data_vol_weekday_weekend_ratio_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_data_vol_weekday_weekend_ratio_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_data_vol_weekday_weekend_ratio_03m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_latency_4g_weekdays_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_latency_4g_weekdays_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_latency_4g_weekdays_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_latency_4g_weekends_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_latency_4g_weekends_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_latency_4g_weekends_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_speed_4g_weekdays_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_speed_4g_weekdays_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_speed_4g_weekdays_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_speed_4g_weekends_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_speed_4g_weekends_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_speed_4g_weekends_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_data_vol_weekday_weekend_ratio_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_data_vol_weekday_weekend_ratio_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_data_vol_weekday_weekend_ratio_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_data_vol_weekday_weekend_ratio_03m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_latency_4g_weekdays_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_latency_4g_weekdays_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_latency_4g_weekdays_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_latency_4g_weekends_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_latency_4g_weekends_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_latency_4g_weekends_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_speed_4g_weekdays_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_speed_4g_weekdays_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_speed_4g_weekdays_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_speed_4g_weekends_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_speed_4g_weekends_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_speed_4g_weekends_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_data_vol_weekday_weekend_ratio_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_data_vol_weekday_weekend_ratio_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_data_vol_weekday_weekend_ratio_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_data_vol_weekday_weekend_ratio_03m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_latency_4g_weekdays_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_latency_4g_weekdays_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_latency_4g_weekdays_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_latency_4g_weekends_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_latency_4g_weekends_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_latency_4g_weekends_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_speed_4g_weekdays_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_speed_4g_weekdays_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_speed_4g_weekdays_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_speed_4g_weekends_01m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_speed_4g_weekends_01w",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_speed_4g_weekends_02m",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_days_since_first_usage",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_days_since_last_usage",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_days_since_first_usage",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_days_since_last_usage",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_days_since_first_usage",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_days_since_last_usage",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_days_since_first_usage",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_days_since_last_usage",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_days_since_first_usage",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_days_since_last_usage",
                        IntegerType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_accessed_apps_01m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_accessed_apps_01w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_accessed_apps_02m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_accessed_apps_02w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_accessed_apps_03m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_accessed_apps_03w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_accessed_apps_num_weeks_01m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_accessed_apps_num_weeks_01w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_accessed_apps_num_weeks_02m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_accessed_apps_num_weeks_03m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_data_vol_01m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_data_vol_01w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_data_vol_02m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_data_vol_02w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_data_vol_03m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_data_vol_03w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_data_vol_weekday_03m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_duration_01m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_duration_01w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_duration_02m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_budget_hobby_duration_03m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_accessed_apps_01m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_accessed_apps_01w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_accessed_apps_02m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_accessed_apps_02w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_accessed_apps_03m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_accessed_apps_03w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_accessed_apps_num_weeks_01m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_accessed_apps_num_weeks_01w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_accessed_apps_num_weeks_02m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_accessed_apps_num_weeks_03m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_data_vol_01m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_data_vol_01w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_data_vol_02m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_data_vol_02w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_data_vol_03m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_data_vol_03w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_data_vol_weekday_03m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_duration_01m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_duration_01w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_duration_02m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_facebook_duration_03m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_accessed_apps_01m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_accessed_apps_01w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_accessed_apps_02m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_accessed_apps_02w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_accessed_apps_03m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_accessed_apps_03w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_accessed_apps_num_weeks_01m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_accessed_apps_num_weeks_01w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_accessed_apps_num_weeks_02m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_accessed_apps_num_weeks_03m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_data_vol_01m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_data_vol_01w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_data_vol_02m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_data_vol_02w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_data_vol_03m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_data_vol_03w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_data_vol_weekday_03m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_duration_01m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_duration_01w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_duration_02m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_instagram_duration_03m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_accessed_apps_01m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_accessed_apps_01w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_accessed_apps_02m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_accessed_apps_02w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_accessed_apps_03m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_accessed_apps_03w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_accessed_apps_num_weeks_01m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_accessed_apps_num_weeks_01w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_accessed_apps_num_weeks_02m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_accessed_apps_num_weeks_03m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_data_vol_01m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_data_vol_01w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_data_vol_02m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_data_vol_02w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_data_vol_03m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_data_vol_03w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_data_vol_weekday_03m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_duration_01m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_duration_01w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_duration_02m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_linkedin_duration_03m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_accessed_apps_01m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_accessed_apps_01w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_accessed_apps_02m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_accessed_apps_02w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_accessed_apps_03m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_accessed_apps_03w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_accessed_apps_num_weeks_01m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_accessed_apps_num_weeks_01w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_accessed_apps_num_weeks_02m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_accessed_apps_num_weeks_03m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_data_vol_01m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_data_vol_01w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_data_vol_02m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_data_vol_02w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_data_vol_03m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_data_vol_03w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_data_vol_weekday_03m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_duration_01m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_duration_01w", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_duration_02m", LongType(), True
                    ),
                    StructField(
                        "fea_int_app_usage_youtube_duration_03m", LongType(), True
                    ),
                ]
            )
        )

        assert_df_frame_equal(
            actual_fea_internet_app_usage_pivot, expected_fea_internet_app_usage_pivot
        )
