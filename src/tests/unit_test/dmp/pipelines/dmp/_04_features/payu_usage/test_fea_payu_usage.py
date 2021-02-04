import datetime
from unittest import mock

import yaml
from pyspark.sql import Row, SparkSession

from src.dmp.pipelines.dmp._04_features.payu_usage.fea_payu_usage import fea_payu_usage


@mock.patch(
    "src.dmp.pipelines.dmp._04_features.payu_usage.fea_payu_usage.get_start_date",
    return_value=datetime.date(2018, 10, 10),
    autospec=True,
)
@mock.patch(
    "src.dmp.pipelines.dmp._04_features.payu_usage.fea_payu_usage.get_end_date",
    return_value=datetime.date(2020, 10, 10),
    autospec=True,
)
class TestFeaturePayuUsage:
    config_feature = yaml.load(open("conf/dmp/training/parameters_feature.yml"))[
        "config_feature"
    ]

    def test_fea_payu_usage(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        sc = spark_session.sparkContext

        df = sc.parallelize(
            [
                # Case A) Sanity test - 1 row for 1 weekstart
                ("111", "2019-10-07", 1000, 700, 300, 500, 500, 300, 400, 150, 150),
                ("111", "2019-10-14", 2000, 1000, 1000, 1000, 1000, 500, 500, 500, 500),
                ("111", "2019-10-21", 100, 50, 50, 50, 50, 30, 20, 20, 30),
                ("111", "2019-10-28", 200, 100, 100, 100, 100, 60, 40, 30, 70),
                ("111", "2019-11-04", 300, 150, 150, 150, 150, 100, 50, 75, 75),
                # Case B) Sanity test - single row only with no usage
                ("222", "2019-10-07", 0, 0, 0, 0, 0, 0, 0, 0, 0),
            ]
        )

        df = spark_session.createDataFrame(
            df.map(
                lambda x: Row(
                    msisdn=x[0],
                    weekstart=x[1],
                    vol_data_kb=x[2],
                    vol_data_day_kb=x[3],
                    vol_data_night_kb=x[4],
                    vol_data_kb_weekends=x[5],
                    vol_data_kb_weekdays=x[6],
                    vol_data_day_kb_weekends=x[7],
                    vol_data_day_kb_weekdays=x[8],
                    vol_data_night_kb_weekends=x[9],
                    vol_data_night_kb_weekdays=x[10],
                )
            )
        )

        out_cols = [
            "msisdn",
            "weekstart",
            "fea_payu_usage_weekend_kb_usage_01w",
            "fea_payu_usage_kb_usage_01w",
            "fea_payu_usage_weekend_night_time_kb_usage_01w",
            "fea_payu_usage_weekday_day_time_kb_usage_01w",
            "fea_payu_usage_weekend_kb_usage_01m",
            "fea_payu_usage_day_time_kb_usage_01m",
        ]

        out = fea_payu_usage(df, self.config_feature, "all", []).select(out_cols)
        out_list = [
            [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7]] for i in out.collect()
        ]

        assert sorted(out_list) == [
            # Case A) Sanity test - all features calculated correctly
            ["111", "2019-10-07", 500, 1000, 150, 400, 500, 700],
            ["111", "2019-10-14", 1000, 2000, 500, 500, 1500, 1700],
            ["111", "2019-10-21", 50, 100, 20, 20, 1550, 1750],
            ["111", "2019-10-28", 100, 200, 30, 40, 1650, 1850],
            ["111", "2019-11-04", 150, 300, 75, 50, 1300, 1300],
            # Case B) Sanity test - no usage
            ["222", "2019-10-07", 0, 0, 0, 0, 0, 0],
        ]
