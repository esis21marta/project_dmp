from pyspark.sql import Row, SparkSession

from src.dmp.pipelines.dmp._01_aggregation.handset.create_handset_lookup_data import (
    create_handset_lookup_data,
)


class TestCreateHandsetLookupData:
    def test_create_handset_lookup_data(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        df_device_dim = sc.parallelize(
            [
                ("tac-1000", "manufacturer-1", "marketname-1", "devicetype-1"),
                ("tac-1000", "manufacturer-1", "marketname-1", "devicetype-2"),
                ("tac-2000", "manufacturer-2", "marketname-2", "devicetype-3"),
            ]
        )

        df_device_dim = spark_session.createDataFrame(
            df_device_dim.map(
                lambda x: Row(
                    tac=x[0], manufacturer=x[1], market_name=x[2], device_type=x[3]
                )
            )
        )

        out = create_handset_lookup_data(df_handset_dim=df_device_dim)
        out = [[i[0], i[1], i[2], i[3]] for i in out.collect()]

        assert sorted(out) == [
            ["tac-1000", "manufacturer-1", "marketname-1", "devicetype-1"],
            ["tac-2000", "manufacturer-2", "marketname-2", "devicetype-3"],
        ]
