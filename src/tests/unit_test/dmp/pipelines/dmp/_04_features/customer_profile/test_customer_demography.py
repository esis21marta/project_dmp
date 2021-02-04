import pyspark.sql.functions as f
from pyspark import Row
from pyspark.sql import SparkSession

from src.dmp.pipelines.dmp._04_features.customer_profile.customer_demography import (
    feat_customer_demographics,
)


class TestCustomerProfile:
    def test_sanity(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        lynx_rdd = sc.parallelize(
            [
                (
                    "A",
                    "201905",
                    31,
                    10,
                    "female",
                    10.0,
                    "married with children",
                    10,
                    "Partner",
                    5,
                    "ISLAM",
                    10,
                    "Housewife",
                    4,
                    "bachelors degree",
                    10,
                    "1-3 million",
                ),
                (
                    "A",
                    "201906",
                    31,
                    10,
                    "male",
                    10.0,
                    "single",
                    7,
                    "Partner",
                    5,
                    "ISLAM",
                    10,
                    "Househusband",
                    4,
                    "bachelors degree",
                    10,
                    "3-5 million",
                ),
                (
                    "A",
                    "201907",
                    31,
                    10,
                    "male",
                    10.0,
                    "married without children",
                    9,
                    "Partner",
                    5,
                    "ISLAM",
                    10,
                    "Househusband",
                    4,
                    "bachelors degree",
                    10,
                    "3-5 million",
                ),
            ]
        )

        lynx_df = spark_session.createDataFrame(
            lynx_rdd.map(
                lambda x: Row(
                    msisdn=x[0],
                    month=x[1],
                    age=int(x[2]),
                    age_prob=float(x[3]),
                    gender=x[4],
                    gender_prob=float(x[5]),
                    marital_status=x[6],
                    marital_status_prob=int(x[7]),
                    household_status=x[8],
                    household_status_prob=int(x[9]),
                    religion=x[10],
                    religion_prob=int(x[11]),
                    occupation=x[12],
                    occupation_prob=int(x[13]),
                    education=x[14],
                    education_prob=int(x[15]),
                    income_category=x[16],
                )
            )
        ).withColumn("month", f.to_date(f.col("month"), "yyyyMM"))

        out = feat_customer_demographics(lynx_df)

        out_list = [
            [i[0], i[1].strftime("%Y-%m-%d"), i[2], i[3], i[4]]
            for i in out.select(
                "msisdn",
                "weekstart",
                "gender",
                "household_status",
                "household_status_prob",
            ).collect()
        ]

        assert out.columns == [
            "msisdn",
            "weekstart",
            "age",
            "age_prob",
            "gender",
            "gender_prob",
            "marital_status",
            "marital_status_prob",
            "household_status",
            "household_status_prob",
            "religion",
            "religion_prob",
            "occupation",
            "occupation_prob",
            "education",
            "education_prob",
            "income_category",
        ]
        assert len(out_list) == 3
        assert sorted(out_list) == [
            ["A", "2019-05-06", "female", "Partner", 5],
            ["A", "2019-06-03", "male", "Partner", 5],
            ["A", "2019-07-08", "male", "Partner", 5],
        ]
