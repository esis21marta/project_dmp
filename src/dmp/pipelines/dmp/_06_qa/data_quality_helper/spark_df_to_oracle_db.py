import logging
from typing import List

import pandas as pd
import pyspark

from utils import execute_raw_query, fillna_with_null, get_config_parameters

logger = logging.getLogger(__name__)


def spark_df_to_oracle_db_wrapper(
    target_catalog: str = None, upsert: bool = False, primary_keys: List[str] = None
) -> pyspark.sql.DataFrame:
    """Wrapper function for spark_df_to_oracle_db

    Args:
        target_catalog (str, optional): Catalog name from which table name will be picked up in case of upsert. Defaults to None.
        upsert (bool, optional): Update if data with primary key exists. Defaults to False.
        primary_keys (List[str], optional): Primary keys in case of upsert. Defaults to None.

    Returns:
        pyspark.sql.DataFrame: [description]
    """

    def spark_df_to_oracle_db(df: pyspark.sql.DataFrame) -> pd.DataFrame:
        """Node to write data to database. Drop the columns before insert for upsert operation.

        Args:
            df (pyspark.sql.DataFrame): Dataframe to write to DB

        Raises:
            Exception: Primary key is required in case of upset operation
            Exception: Target catalog is required in case of upsert

        Returns:
            pd.DataFrame: Dataframe with NaN replaced with None in input dataframe
        """

        if upsert:
            if len(primary_keys) == 0:
                raise Exception("Primary key is required in case of upset operation")
            if target_catalog is None:
                raise Exception("Target catalog is required in case of upsert")

            unique_values_in_primary_keys = df.select(primary_keys).drop_duplicates()

            conf_catalog = get_config_parameters(config="catalog")
            table_name = conf_catalog[target_catalog]["table"]

            # Delete table
            for record in unique_values_in_primary_keys.rdd.collect():

                query_where_clause = []
                for key in primary_keys:
                    # In case of date column in primary key, convert to date format
                    if key in ["run_time", "date"]:
                        query_where_clause.append(
                            f"{key}=TO_DATE('{record[key]}', 'yyyy-mm-dd')"
                        )
                    else:
                        query_where_clause.append(f"{key}='{record[key]}'")

                delete_query = (
                    f"DELETE FROM {table_name} WHERE {' AND '.join(query_where_clause)}"
                )

                execute_raw_query(delete_query, "qa_credentials_con_string")

        # Replace NaN with None
        df = fillna_with_null(df)
        return df

    return spark_df_to_oracle_db
