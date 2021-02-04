from pyspark.sql import Row, SparkSession
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from utils import append_different_windows, get_df_with_mode_cols


class TestListFunctions:
    def test_get_df_with_mode_cols(self, spark_session: SparkSession):
        schema = StructType(
            [
                StructField("msisdn", StringType(), True),
                StructField("weekstart", StringType(), True),
                StructField("col_00w_to_01w", ArrayType(StringType()), True),
                StructField("col_01w_to_02w", ArrayType(StringType()), True),
            ]
        )

        data = spark_session.createDataFrame(
            [
                # Case A) One column null and one column empty list
                ("a", "2019-01-01 10:00:00", None, []),
                # Case B) One column filled with numbers and one column empty list
                ("b", "2019-01-01 10:00:00", [30, 30, 100, 100, 2, 30], []),
                # Case C) One column filled with strings and one column empty list
                (
                    "c",
                    "2019-01-01 10:00:00",
                    ["test", "test", "test", "test1", "test1", "test2", "test2"],
                    [],
                ),
                # Case D) One column filled with numbers and one column empty list
                ("d", "2019-01-01 10:00:00", [], [10, 10, 10, 100]),
                # Case E) One column filled with numbers and one with strings (more nulls than values)
                (
                    "e",
                    "2019-01-01 10:00:00",
                    [None, None, None, 1, 1, 2, 2],
                    ["a", "a", None, None, None],
                ),
                # Case F) One column filled with numbers and one with strings (more '' than values)
                (
                    "f",
                    "2019-01-01 10:00:00",
                    ["", "", "", 1, 1, 2, 2],
                    ["a", "a", "", "", ""],
                ),
            ],
            schema,
        )

        out_dict = get_df_with_mode_cols(
            data, ["col_00w_to_01w", "col_01w_to_02w"], "msisdn"
        )
        out = out_dict["dataframe"].select(
            ["msisdn", "weekstart"] + out_dict["mode_columns"]
        )
        mode_columns = out_dict["mode_columns"]

        assert mode_columns == ["mode_col_00w_to_01w", "mode_col_01w_to_02w"]

        out_list = [[str(i[0]), str(i[1]), str(i[2]), str(i[3])] for i in out.collect()]

        assert sorted(out_list) == [
            # Case A) Both nulls
            ["a", "2019-01-01 10:00:00", "None", "None"],
            # Case B) One mode calculated correctly (int) and one null
            ["b", "2019-01-01 10:00:00", "30", "None"],
            # Case C) One mode calculated correctly (string) and one null
            ["c", "2019-01-01 10:00:00", "test", "None"],
            # Case D) One mode calculated correctly (string) and one null
            ["d", "2019-01-01 10:00:00", "None", "10"],
            # Case E) Two mode calculated correctly (string and int)
            ["e", "2019-01-01 10:00:00", "1", "a"],
            # Case F) Empty string chosen correctly
            ["f", "2019-01-01 10:00:00", "", ""],
        ]

    def test_append_different_windows(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        data = sc.parallelize(
            [
                # Case A) Sanity test - rolling window over 4 weekstarts
                ("111", "2019-10-07", "a"),
                ("111", "2019-10-14", "d"),
                ("111", "2019-10-21", "g"),
                ("111", "2019-10-28", "j"),
                # Case B) Edge case - rolling window over 3 weekstarts but one of them empty list and one of them null
                ("222", "2019-10-07", "a"),
                ("222", "2019-10-14", ""),
                ("222", "2019-10-21", None),
                # Case C) Sanity case - rolling window over 2 weekstarts with numbers
                ("333", "2019-10-10", 3),
                ("333", "2019-10-17", 1),
            ]
        )

        data = spark_session.createDataFrame(
            data.map(lambda x: Row(msisdn=x[0], weekstart=x[1], col=x[2]))
        )

        out_dict = append_different_windows(data, ["col"], [[7, 0], [14, 7], [28, 14]])
        out_window_columns = out_dict["window_columns"]
        out_df = out_dict["dataframe"].select(
            ["msisdn", "weekstart"] + out_window_columns
        )

        out_list_multiple_col = [
            [
                i[0],
                i[1],
                sorted(map(str, i[2])),
                sorted(map(str, i[3])),
                sorted(map(str, i[4])),
            ]
            for i in out_df.collect()
        ]

        assert sorted(out_df.columns) == [
            "col_00w_to_01w",
            "col_01w_to_02w",
            "col_02w_to_01m",
            "msisdn",
            "weekstart",
        ]
        assert out_window_columns == [
            "col_00w_to_01w",
            "col_01w_to_02w",
            "col_02w_to_01m",
        ]
        assert sorted(out_list_multiple_col) == [
            # Case A) Sanity test - rolling windows collected correctly
            ["111", "2019-10-07", ["a"], [], []],
            ["111", "2019-10-14", ["d"], ["a"], []],
            ["111", "2019-10-21", ["g"], ["d"], ["a"]],
            ["111", "2019-10-28", ["j"], ["g"], ["a", "d"]],
            # Case B) Edge case - rolling windows collected correctly
            ["222", "2019-10-07", ["a"], [], []],
            ["222", "2019-10-14", [""], ["a"], []],
            ["222", "2019-10-21", [], [""], ["a"]],
            # Case C) Sanity case  - rolling windows collected correctly
            ["333", "2019-10-10", ["3"], [], []],
            ["333", "2019-10-17", ["1"], ["3"], []],
        ]
