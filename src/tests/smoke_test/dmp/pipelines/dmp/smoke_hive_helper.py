# Copyright 2018-present QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Any, Dict, List

import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def retrieve_new_smoke_msisdns(
    sparkSession: SparkSession, database: str = "mck"
) -> pyspark.sql.DataFrame:
    """
    Retrieve smoke test MSISDNs.
    Args:
        sparkSession: Spark Session.
        database: Database to read from.
    Returns:
        Dataframe of msisdn column
    """
    return sparkSession.read.table(database + "." + "smoke_test_msisdns").select(
        "msisdn"
    )


def retrieve_new_smoke_laccis(
    sparkSession: SparkSession, database: str = "mck"
) -> pyspark.sql.DataFrame:
    """
    Retrieve smoke test MSISDNs.
    Args:
        sparkSession: Spark Session.
        database: Database to read from.
    Returns:
        Dataframe of msisdn column
    """
    return sparkSession.read.table(database + "." + "smoke_test_laccis").select(
        "lac_ci"
    )


def retrieve_new_smoke_tables(
    sparkSession: SparkSession, database: str = "mck"
) -> pyspark.sql.DataFrame:
    """
    Retrieve new smoke test tables to be added.
    Args:
        sparkSession: Spark Session.
        database: Database to read from.
    Returns:
        Dataframe of database, table, primary_column, partition_column, is_partitioned, skip columns
    """
    return sparkSession.read.table(database + "." + "smoke_test_tables").select(
        "database",
        "table",
        "primary_column",
        "partition_column",
        "msisdn_alias",
        "is_partitioned",
        "skip",
    )


def retrieve_existing_smoke_tables(
    sparkSession: SparkSession,
    database: str = "mck",
    filter_starts_with: str = "smoke_",
) -> pyspark.sql.DataFrame:
    """
    Retrieve existing smoke test tables.
    Args:
        sparkSession: Spark Session.
        database: Database to read from.
        filter_starts_with: table prefix to identify smoke test tables.
    Returns:
        Dataframe of existing_table column.
    """
    return (
        sparkSession.sql(f"show tables in {database}")
        .filter(F.col("tableName").startswith(filter_starts_with))
        .select(F.col("tableName").alias("existing_table"))
    )


def retrieve_delta_smoke_tables(
    existing_smoke_tables: pyspark.sql.DataFrame,
    new_smoke_tables: pyspark.sql.DataFrame,
) -> List[Dict[str, str]]:
    """
    Retrieve delta smoke test tables.
    Args:
        existing_smoke_tables: Existing smoke test tables and msisdns.
        new_smoke_tables: Tables to be run for smoke tests.
    Returns:
        List of dictionary of smoke tables to be added.
    """
    delta = new_smoke_tables.join(
        existing_smoke_tables,
        ((new_smoke_tables.table == existing_smoke_tables.existing_table)),
        "left",
    ).filter(F.col("existing_table").isNull())

    delta_dict = list(map(lambda row: row.asDict(), delta.collect()))

    return delta_dict


def _retrieve_alt_msisdn_col(source_primary_col: str, source_msisdn_alias: str) -> str:
    """
    Returns the alternative MSISDN column to be used for the join with MSISDN table

    Args:
        source_primary_col: Primary column defined.
        source_msisdn_alias: MSISDN alias defined.

    Returns:
        String to be used for MSISDN column alternative.
    """
    if source_primary_col == "msisdn":
        return source_primary_col
    elif (source_primary_col == "") and (source_msisdn_alias != ""):
        return source_msisdn_alias
    else:
        raise Exception(
            "`source_primary_col` must equal `msisdn` and `source_msisdn_alias` must not equal ``"
        )


def _migrate_table(
    source_dict: Dict[str, Any],
    source_full_tbl: pyspark.sql.DataFrame,
    dest_full_tbl: str,
    msisdns_to_migrate: pyspark.sql.DataFrame = None,
):
    """
    Generic table migration function.
    Args:
        source_dict: Dictionary of missing smoke test tables to add.
        source_full_tbl: Dataframe of source table.
        dest_full_tbl: Name of output table.
        msisdns_to_migrate: List of MSISDNs to migrate.

    Returns:
        Dataframe of migrated rows.
    """
    source_primary_col = source_dict["primary_column"]
    source_part_col = source_dict["partition_column"]
    source_msisdn_alias = source_dict["msisdn_alias"]
    source_is_part = source_dict["is_partitioned"]
    source_table = source_dict["table"]

    if (source_primary_col == "") and (source_msisdn_alias == ""):
        # In the case where there's no primary column / msisdn alias column don't need to do a full
        # overwrite when not msisdn column because initial dump was on 2020 - 03 - 11 which covers our
        # scope of testing (Jan 1 2019 - Dec 31 2019)
        print(
            f"Ignoring migration to {dest_full_tbl} because {source_primary_col} and {source_msisdn_alias} are empty"
        )
        return True

    elif source_table == "sales_dnkm_dd":

        # For Network Table, it has lacci as primary column
        source_df = source_full_tbl.join(
            F.broadcast(msisdns_to_migrate), on="lac_ci"
        ).repartition(3)

        print(f"Migrating to {dest_full_tbl} for LAC CI: {msisdns_to_migrate}")

        split_part_col = source_part_col.split(",")
        source_df = source_df.repartition(3)
        source_df.write.mode("overwrite").format("parquet").partitionBy(
            *split_part_col
        ).saveAsTable(dest_full_tbl)
        return True

    else:

        # Rename `msisdn` column to be able to join with `source_full_tbl`
        alt_msisdn_col = _retrieve_alt_msisdn_col(
            source_primary_col, source_msisdn_alias
        )
        source_full_tbl = source_full_tbl.withColumnRenamed(alt_msisdn_col, "msisdn")

        # Perform join to filter to MSISDNs of interest
        source_df = source_full_tbl.join(F.broadcast(msisdns_to_migrate), on="msisdn")

        # Rename `msisdn` column back to original so it saves in correct variable
        source_df = source_df.withColumnRenamed("msisdn", alt_msisdn_col).repartition(3)

        print(f"Migrating to {dest_full_tbl} for MSISDNs: {msisdns_to_migrate}")

        if source_is_part:
            split_part_col = source_part_col.split(",")
            source_df.write.mode("overwrite").format("parquet").partitionBy(
                *split_part_col
            ).saveAsTable(dest_full_tbl)
            return True
        else:
            source_df.write.mode("overwrite").format("parquet").saveAsTable(
                dest_full_tbl
            )
            return True


def migrate_missing_tables(
    sparkSession: SparkSession,
    msisdns_to_migrate: pyspark.sql.DataFrame,
    source_dict: Dict[str, str],
):
    """
    Migrate delta smoke tables for particular MSISDNs to migrate.
    Args:
        sparkSession: Spark Session.
        msisdns_to_migrate: List of MSISDNs to migrate.
        source_dict: Dictionary of missing smoke test tables to add.
    Returns:
        Resulting migrated dataframe.
    """

    source_db = source_dict["database"]
    source_tbl = source_dict["table"]

    # Ignore this table as it will be handled by `migrate_tables_with_smoke_dependencies` function
    # This table is edge case as it doesn't have MSISDN column and it's subset for smoke table must be derived
    # from another table.
    if (source_db == "base") and (source_tbl == "ocs_bal"):
        return

    dest_full_tbl = f"mck.smoke_{source_tbl}"
    source_full_tbl = sparkSession.read.table(source_db + "." + source_tbl)

    _migrate_table(source_dict, source_full_tbl, dest_full_tbl, msisdns_to_migrate)


def migrate_tables_with_smoke_dependencies(
    sparkSession: SparkSession, source_dict: Dict[str, str]
):
    """
    Migrate tables which need data from smoke hive tables for migration. This is an edge case.
    Args:
        sparkSession: Spark session.
        source_dict: Dictionary of missing smoke test tables to add.
    """
    source_db = source_dict["database"]
    source_tbl = source_dict["table"]
    dest_full_tbl = f"mck.smoke_{source_tbl}"

    # Since `base.ocs_bal` doesn't have `msisdn` column and is too large for a full migration, we will use
    # ["pay_channel", "event_date"] from cb.cb_pre_dd since that's the inner join condition on recharge domain.

    if (source_db == "base") and (source_tbl == "ocs_bal"):
        base_ocs_bal = sparkSession.read.table(source_db + "." + source_tbl)
        cb_pre_dd = sparkSession.read.table("mck.smoke_cb_pre_dd")
        cb_pre_dd = cb_pre_dd.withColumnRenamed("paychannel", "pay_channel")
        source_full_tbl = base_ocs_bal.join(
            cb_pre_dd, ["pay_channel", "event_date"]
        ).select(
            [
                F.col(f"{source_db}.{source_tbl}.{sel_col}")
                for sel_col in base_ocs_bal.columns
            ]
        )

        _migrate_table(
            source_dict=source_dict,
            source_full_tbl=source_full_tbl,
            dest_full_tbl=dest_full_tbl,
            msisdns_to_migrate=None,
        )
