from decimal import Decimal
from typing import Dict

import pyspark
import pyspark.sql.functions as F

from src.dmp.run import ProjectContext


def assert_parquet_equivalence(
    project_context: ProjectContext,
    catalog_name: str,
    smoke_path: str,
    ground_truth_path: str,
    msisdns_df: pyspark.sql.DataFrame,
    laccis_df: pyspark.sql.DataFrame,
):
    """
    Assert parquet content equivalence.
    Args:
        project_context: Project Context.
        smoke_path: Smoke parquet path.
        ground_truth_path: Ground truth parquet path.
        msisdns_df: Dataframe of msisdns.
    """

    spark_session = project_context._spark_session
    ignored_parquet_catalogs: Dict[str, str] = project_context.params.get(
        "custom_start_date"
    )

    if catalog_name in list(ignored_parquet_catalogs.keys()):
        start_date = ignored_parquet_catalogs.get(catalog_name)
    else:
        start_date = "2019-01-01"

    try:
        smoke_path_df = spark_session.read.parquet(smoke_path)
        ground_truth_path_df = spark_session.read.parquet(ground_truth_path)

        if (
            ("msisdn" in smoke_path_df.columns)
            & ("msisdn" in ground_truth_path_df.columns)
            & ("weekstart" in smoke_path_df.columns)
            & ("weekstart" in ground_truth_path_df.columns)
        ):

            smoke_path_df = smoke_path_df.join(msisdns_df, on=["msisdn"])
            ground_truth_path_df = ground_truth_path_df.join(msisdns_df, on=["msisdn"])

            cols_sel = ["weekstart", "msisdn"]
            cols_sel = cols_sel + [
                ele for ele in smoke_path_df.columns if ele not in cols_sel
            ]

            smoke_path_df_filtered = smoke_path_df.filter(
                (F.col("weekstart") >= start_date)
                & (F.col("weekstart") <= "2019-12-31")
            ).orderBy(cols_sel)
            ground_truth_path_df_filtered = ground_truth_path_df.filter(
                (F.col("weekstart") >= start_date)
                & (F.col("weekstart") <= "2019-12-31")
            ).orderBy(cols_sel)

        elif (
            ("lac_ci" in smoke_path_df.columns)
            & ("lac_ci" in ground_truth_path_df.columns)
            & ("weekstart" in smoke_path_df.columns)
            & ("weekstart" in ground_truth_path_df.columns)
        ):

            smoke_path_df = smoke_path_df.join(laccis_df, on=["lac_ci"])
            ground_truth_path_df = ground_truth_path_df.join(laccis_df, on=["lac_ci"])

            cols_sel = ["weekstart", "lac_ci"]
            cols_sel = cols_sel + [
                ele for ele in smoke_path_df.columns if ele not in cols_sel
            ]

            smoke_path_df_filtered = smoke_path_df.filter(
                (F.col("weekstart") >= start_date)
                & (F.col("weekstart") <= "2019-12-31")
            ).orderBy(cols_sel)
            ground_truth_path_df_filtered = ground_truth_path_df.filter(
                (F.col("weekstart") >= start_date)
                & (F.col("weekstart") <= "2019-12-31")
            ).orderBy(cols_sel)

        elif (
            ("lac" in smoke_path_df.columns)
            & ("lac" in ground_truth_path_df.columns)
            & ("weekstart" in smoke_path_df.columns)
            & ("weekstart" in ground_truth_path_df.columns)
        ):

            lacs_df = laccis_df.withColumn(
                "lac", F.split(F.col("lac_ci"), "-").getItem(0)
            ).select("lac")

            smoke_path_df = smoke_path_df.join(lacs_df, on=["lac"])
            ground_truth_path_df = ground_truth_path_df.join(lacs_df, on=["lac"])

            cols_sel = ["weekstart", "lac"]
            cols_sel = cols_sel + [
                ele for ele in smoke_path_df.columns if ele not in cols_sel
            ]

            smoke_path_df_filtered = smoke_path_df.filter(
                (F.col("weekstart") >= start_date)
                & (F.col("weekstart") <= "2019-12-31")
            ).orderBy(cols_sel)
            ground_truth_path_df_filtered = ground_truth_path_df.filter(
                (F.col("weekstart") >= start_date)
                & (F.col("weekstart") <= "2019-12-31")
            ).orderBy(cols_sel)

        else:
            smoke_path_df_filtered = smoke_path_df.orderBy(smoke_path_df.columns)
            ground_truth_path_df_filtered = ground_truth_path_df.orderBy(
                ground_truth_path_df.columns
            )

        smoke_path_cols = smoke_path_df_filtered.columns
        ground_truth_cols = ground_truth_path_df_filtered.columns

        # Check for columns equivalence
        if sorted(smoke_path_cols) != sorted(ground_truth_cols):
            return (
                False,
                f"Columns don't match: {sorted(smoke_path_cols)} != {sorted(ground_truth_cols)}",
            )

        cols_of_interest = smoke_path_df_filtered.columns

        smoke_path_list = [
            [i[idx] for idx in range(len(cols_of_interest))]
            for i in smoke_path_df_filtered.select(cols_of_interest).collect()
        ]

        ground_truth_path_list = [
            [i[idx] for idx in range(len(cols_of_interest))]
            for i in ground_truth_path_df_filtered.select(cols_of_interest).collect()
        ]

        # Check for row count equivalence
        if len(smoke_path_list) != len(ground_truth_path_list):
            return (
                False,
                f"Row counts don't match: {len(ground_truth_path_list)} != {len(smoke_path_list)}",
            )

        # Check for data equivalence
        for row_idx in range(len(smoke_path_list)):

            gt_row = ground_truth_path_list[row_idx]
            smoke_row = smoke_path_list[row_idx]

            for col_idx in range(len(cols_of_interest)):

                smoke_row_col = smoke_row[col_idx]
                gt_row_col = gt_row[col_idx]

                if (type(gt_row_col) == list) | (type(gt_row_col) == set):
                    gt_row_col = [str(i) for i in gt_row_col]
                    smoke_row_col = [str(i) for i in smoke_row_col]

                    if sorted(gt_row_col) != sorted(smoke_row_col):
                        return (
                            False,
                            f"List/set doesn't match between {sorted(gt_row_col)} and {sorted(smoke_row_col)}",
                        )

                else:
                    if str(gt_row_col) != str(smoke_row_col):

                        if (type(gt_row_col) == float) | (type(gt_row_col) == Decimal):
                            if round(gt_row_col, 6) != round(smoke_row_col, 6):
                                return (
                                    False,
                                    f"Float doesn't match between {gt_row_col} and {smoke_row_col}",
                                )

                        else:
                            return (
                                False,
                                f"String doesn't match between {gt_row_col} and {smoke_row_col}",
                            )

    except Exception as e:
        print(e)
        return False, "Parquet path(s) don't exist or cannot be read"

    return True, ""


def assert_smoke_tests_handler(
    project_context: ProjectContext,
    smoke_paths: Dict[str, str],
    ground_truth_paths: Dict[str, str],
    msisdns_df: pyspark.sql.DataFrame,
    laccis_df: pyspark.sql.DataFrame,
):
    """
    Call other asserts.
    Args:
        project_context: Kedro Project Context.
        smoke_paths: Dict of smoke parquet paths.
        ground_truth_paths: Dict of Ground truth parquet path.
        msisdns_df: Dataframe of MSISDNs to assert.
    """

    failed_cnt = 0

    ignored_parquet_catalogs = project_context.params.get("ignored_parquet_checks")
    print("Ignored parquet catalogs:", ignored_parquet_catalogs)

    for catalog_name, ground_truth_parquet_path in ground_truth_paths.items():

        if catalog_name in ignored_parquet_catalogs:
            continue

        smoke_parquet_path = smoke_paths.get(catalog_name)

        print(
            "Asserting ground truth:",
            ground_truth_parquet_path,
            ", against smoke output: ",
            smoke_parquet_path,
        )

        is_success, reason = assert_parquet_equivalence(
            project_context,
            catalog_name,
            smoke_parquet_path,
            ground_truth_parquet_path,
            msisdns_df,
            laccis_df,
        )

        if not is_success:
            print("Error reason:", reason)
            failed_cnt += 1

    if failed_cnt > 0:
        assert (
            False
        ), f"There are {failed_cnt} failed parquet checks between smoke output and ground truth."
    else:
        assert True
