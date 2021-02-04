import logging
import os
import sys
from datetime import date, datetime
from functools import reduce  # For Python 3.x
from typing import Dict, List, Optional, Tuple, Union

import pyspark
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from sqlalchemy.exc import DBAPIError

from src.dmp.pipelines.aggregator.agg_rules import (
    apply_rule_criteria,
    apply_rule_msisdn,
    apply_rule_valid_criteria,
    apply_rule_valid_msisdn,
    apply_rule_valid_push_channel,
    apply_schema_validation_checks,
)
from src.dmp.pipelines.aggregator.api import (
    generate_run_id,
    get_job_files,
    insert_whitelist_file_records,
    mark_job_files_as_completed,
    mark_job_files_as_error,
    mark_run_as_completed,
    mark_run_as_error,
)
from src.dmp.pipelines.aggregator.catalog_builder import DatasetBuilder
from src.dmp.pipelines.core.db import db_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def today_str() -> str:
    return datetime.today().strftime("%Y%m%d%H%M%S")


def _exit_successfully(msg: str = None) -> None:
    try:
        log_msg = "Successfully stopping the process for a reason"
        logger.info(f"{log_msg} - {msg}" if msg else f"{log_msg}")
        sys.exit(1)
    except SystemExit:
        pass


def _terminate(msg: str = None) -> None:
    try:
        log_msg = "Terminating the process due to"
        logger.error(f"{log_msg} - {msg}" if msg else f"{log_msg}")
        sys.exit(1)
    except SystemExit:
        pass


def aggregator_inputs(
    input_list_columns: List[str], top_rank: int, push_channels_limit: Dict[str, str]
) -> Tuple[Optional[pyspark.sql.DataFrame], Optional[List[int]], int]:
    """
    The function fetches the file records from database and converts it
    into spark data frames using the kedro data catalog api.
    Args:
        input_list_columns (List[str]): Expected list of columns set in parameters.yml file.
        top_rank (int): The parameter suggests top N rows to be selected.
        push_channels_limit (Dict[str, str]): Hourly limit of push channels set in parameters.yml file.

    Returns:
        Tuple[Optional[DataFrame], Optional[List[int]], int]
    """

    engine = str(os.environ["DOMAIN_ENGINE"])
    if engine is None:
        raise RuntimeError("DOMAIN_ENGINE environment variable is not set.")
    logger.info(f"DOMAIN_ENGINE value found to be {engine}")
    ids = None
    run_id = None
    try:
        run_id = generate_run_id(
            db_session,
            engine=engine,
            top_rank=top_rank,
            push_channel_limits=str(push_channels_limit),
        )

        records = get_job_files(db_session, engine)
        if len(records) == 0:
            mark_run_as_completed(db_session, run_id)
            logger.info("No records found for aggregator processing..")
            exit()
            return None, ids, run_id
        else:
            logger.info("Processing records found for aggregator..")
            ids = [record.id for record in records]
            dataset_builder = DatasetBuilder()

            (
                loaded_datasets,
                loaded_record_ids,
                failed_record_ids,
            ) = dataset_builder.build_with_file_records(records)

            (
                loaded_datasets_validated,
                loaded_validated_ids,
            ) = apply_schema_validation_checks(
                loaded_datasets, loaded_record_ids, input_list_columns
            )

            rejected_ids = list(set(ids) - set(loaded_validated_ids))

            logger.info(f"Files with record ids {loaded_validated_ids} are accepted")
            logger.info(f"Files with record ids {rejected_ids} are rejected")

            if len(loaded_validated_ids) == 0:
                mark_job_files_as_error(db_session, run_id, rejected_ids)
                mark_run_as_error(db_session, run_id)
                db_session.close()
                exit()
                _terminate(
                    f"exception occurred in func - `aggregator_inputs` All files are rejected"
                )

                return None, ids, run_id

            elif len(rejected_ids) > 0:
                mark_job_files_as_error(db_session, run_id, rejected_ids)

            return (
                aggregator_combine(loaded_datasets_validated),
                loaded_validated_ids,
                run_id,
            )

    except DBAPIError as e:
        logger.error(f"Database API occurred. {e}")
    except Exception as e:
        mark_run_as_error(db_session, run_id)
        if ids:
            mark_job_files_as_error(db_session, run_id, ids)
        logger.error(f"Error message: {e}")
        db_session.close()
        exit()
        _terminate(f"exception occurred in func - `aggregator_inputs` {e}")


def aggregator_input_validation(
    df: pyspark.sql.DataFrame, records: List[int], run_id: int, push_channels: List[str]
) -> Tuple[Optional[pyspark.sql.DataFrame], Optional[pyspark.sql.DataFrame], int]:
    """
    This functions carries out the validation on all fields required in input files

    Args:
        df (DataFrame): Input dataset to be validated for aggregated processing. The columns in the schema
        will be msisdn, keyword, push_channel, criteria 1 .. 5
        records (List[int]): List of FileRecord, used by aggregator to fetch file
        info for processing.
        run_id (int): The execution id of the aggregator process for tracking purpose.
        push_channels (List[str]): List of valid push channels values.

    Returns:
        Tuple[Optional[DataFrame], Optional[DataFrame], int]
    """

    if df is not None:
        logger.info(
            f"Running aggregator validation function for run_id={run_id} with {len(records)} records"
        )
        df = apply_rule_msisdn(df)
        df = apply_rule_criteria(df)
        df = apply_rule_valid_criteria(df)
        df = apply_rule_valid_msisdn(df)
        df = apply_rule_valid_push_channel(df, push_channels)
        df = (
            df.withColumn(
                "valid",
                f.when(
                    ((f.trim(f.col("msisdn")) == "") | f.col("msisdn").isNull())
                    | (
                        (f.trim(f.col("push_channel")) == "")
                        | f.col("push_channel").isNull()
                    )
                    | ((f.trim(f.col("keyword")) == "") | f.col("keyword").isNull())
                    | (f.col("valid_msisdn") == "No")
                    | (f.col("valid_criteria") == "No")
                    | (f.col("valid_push_channel") == "No"),
                    "No",
                ).otherwise("Yes"),
            )
            .drop("valid_msisdn")
            .drop("valid_push_channel")
            .drop("valid_criteria")
            .distinct()
        )

        df_clean = df.filter(f.col("valid") == "Yes")
        df_reject = df.filter(f.col("valid") == "No")

        tot_unq_msisdn_df = df.select("msisdn").distinct().count()
        tot_unq_msisdn_clean = df_clean.select("msisdn").distinct().count()

        logger.info(f"Number of unique msisdn in full dataset is {tot_unq_msisdn_df}")
        logger.info(
            f"Number of unique msisdn in valid dataset is {tot_unq_msisdn_clean}"
        )
        logger.info(f"record that valid is {df_clean.count()}")
        logger.info(f"record that reject is {df_reject.count()}")

        return df_clean, df_reject, tot_unq_msisdn_clean


def aggregator_rank(
    df: pyspark.sql.DataFrame, records: List[int], run_id: int, top_rank: int
) -> Optional[pyspark.sql.DataFrame]:
    """
    This functions ranks per msisdn and push channel based on criteria 1 .. 5

    Args:
        df (DataFrame): The dataset to be ranked for aggregator. The columns in the schema
        will be msisdn, keyword, push_channel, criteria 1 .. 5
        records (List[int]): List of FileRecord, used by aggregator to fetch file
        info for processing.
        run_id (int): The execution id of the aggregator process for tracking purpose.
        top_rank (int): The parameter suggests top N rows to be selected.

    Returns:
        Optional[pyspark.sql.DataFrame]
    """

    if df is not None:
        logger.info(
            f"Running aggregator rank function for run_id={run_id} with {len(records)} records"
        )

        df = df.filter(f.col("valid") == "Yes").drop("valid")
        df = df.withColumn(
            "rank",
            f.row_number().over(
                Window.partitionBy("msisdn", "push_channel").orderBy(
                    f.col("criteria_1").desc(),
                    f.col("criteria_2").desc(),
                    f.col("criteria_3").desc(),
                    f.col("criteria_4").desc(),
                    f.col("criteria_5").desc(),
                )
            ),
        ).filter(f.col("rank") <= top_rank)

        return df


def aggregator_combine(dfs: List[pyspark.sql.DataFrame]) -> pyspark.sql.DataFrame:
    """
    The function combines list of data frames returns single data frame.
    It is equivalent to SQL union all operation.

    Args:
        dfs: List of DataFrame

    Returns:
        DataFrame
    """

    df = reduce(pyspark.sql.DataFrame.union, dfs)
    return df


def aggregator_assert(df: pyspark.sql.DataFrame, col: list,) -> bool:

    return sorted(df.columns) == col


def aggregator_whitelist(
    df: pyspark.sql.DataFrame,
    whitelist_path: str,
    records: List[int] = None,
    run_id: int = None,
    domain_to_end_at_whitelist: Union[str, List[str]] = "cms",
    total_unique_msisdn: int = None,
) -> None:
    """
    This functions filters msisdn based on keywords provided and save it for whitelists.

    Args:

        df (DataFrame): The dataset which includes whitelist rows for aggregator run.
        whitelist_path (str): The path to store whitelists of aggregator.
        records (List[int]): List of FileRecord, used by aggregator to fetch file
        info for processing.
        run_id (int): The execution id of the aggregator process for tracking purpose.
        domain_to_end_at_whitelist (Union[str, List[str]]): This represents campaign execution engines
        that should stop at whitelist node - {cms}.
        total_unique_msisdn: Total unique msisdn from ranked data

    Returns:

    """

    if df is not None:
        engine = os.environ["DOMAIN_ENGINE"]
        logger.info(
            f"Running aggregator whitelist function for domain '{engine}' with run_id={run_id} "
            f"and has {len(records)} record ids"
            f"and has {df.select('msisdn').distinct().count()} unique msisdn (expecting {total_unique_msisdn} unique msisdn) "
        )

        list_keywords = [i[0] for i in df.select(f.col("keyword")).distinct().collect()]
        today = today_str()
        if len(list_keywords) > 0:
            logger.info(f"Saving whitelist files")
            whitelist_file_paths = []
            for keyword in list_keywords:
                df_msisdn = (
                    df.filter(f.col("keyword") == keyword).select("msisdn").distinct()
                )
                path = f"{whitelist_path}/{today}/model_{keyword.lower()}.txt"
                df_msisdn.coalesce(1).write.mode("overwrite").csv(path=path)
                whitelist_file_paths.append(path)

            insert_whitelist_file_records(db_session, whitelist_file_paths, run_id)
            logger.info(f"Inserted whitelist file records")

        if engine is not None and engine in domain_to_end_at_whitelist:
            mark_run_as_completed(db_session, run_id)
            mark_job_files_as_completed(db_session, run_id, records)


def aggregator_event_creation(
    df: pyspark.sql.DataFrame, records: List[int] = None, run_id: int = None
) -> Optional[pyspark.sql.DataFrame]:
    """
    This function creates event table for aggregator by adding columns. The columns in event schema
    will be qb, qb_event, msisdn, scheduled_push_time, trx_number, keyword, push_channel,
    criteria 1 .. 5
    Args:
        df (DataFrame): The dataset for event creation.
        info for processing.
        records (List[int]): List of FileRecord, used by aggregator to fetch file
        info for processing.
        run_id (int): The execution id of the aggregator process for tracking purpose.

    Returns:
        Optional[pyspark.sql.DataFrame]
    """
    if df is not None:
        logger.info(
            f"Running aggregator event creation function for run_id={run_id} "
            f"with {len(records)} record ids"
        )
        start_index = int(date.today().strftime("%Y%m%d")) * 100000000
        df = (
            df.withColumn("qb", f.lit("QB"))
            .withColumn("qb_event", f.lit("QB_EVENT"))
            .withColumn("scheduled_push_time", f.current_timestamp())
            .withColumn("trx_number", start_index + f.monotonically_increasing_id())
        )
        cols = [
            "qb",
            "qb_event",
            "msisdn",
            "scheduled_push_time",
            "trx_number",
            "keyword",
            "push_channel",
            "criteria_1",
            "criteria_2",
            "criteria_3",
            "criteria_4",
            "criteria_5",
            "rank",
        ]
        return df.select(cols)


def aggregator_event_validation(
    df: pyspark.sql.DataFrame, records: List[int], run_id: int
) -> Tuple[Optional[pyspark.sql.DataFrame], Optional[pyspark.sql.DataFrame]]:
    """
    The function validates the event records generated by `event_creation` func.
    Args:
        df (DataFrame): DataFrame with event records.
        records (List[int]): List of FileRecord, used by aggregator to fetch file
        info for processing.
        run_id (int): The execution id of the aggregator process for tracking purpose.

    Returns:
        Tuple[Optional[DataFrame], Optional[DataFrame]]
    """
    if df is not None:
        logger.info(
            f"Running aggregator event validation function for run_id={run_id} "
            f"with {len(records)} record ids"
        )
        df_reject = df.groupBy("msisdn", "keyword").count().filter(f.col("count") > 1)
        df_check_null = df.withColumn(
            "is_filled",
            f.when(
                (f.trim(f.col("qb")) == "")
                | f.col("qb").isNull()
                | (f.trim(f.col("qb_event")) == "")
                | f.col("qb_event").isNull()
                | (f.trim(f.col("msisdn")) == "")
                | f.col("msisdn").isNull()
                | (f.trim(f.col("scheduled_push_time")) == "")
                | f.col("scheduled_push_time").isNull()
                | (f.trim(f.col("trx_number")) == "")
                | f.col("trx_number").isNull()
                | (f.trim(f.col("keyword")) == "")
                | f.col("keyword").isNull(),
                "No",
            ).otherwise(f.lit("Yes")),
        )

        df_valid = (
            df_check_null.filter(f.col("is_filled") == "Yes")
            .dropDuplicates(subset=["msisdn", "keyword"])
            .withColumnRenamed("is_filled", "valid")
        )

        df = (
            df_check_null.join(df_reject, ["msisdn", "keyword"], how="left")
            .withColumn(
                "valid",
                f.when(
                    f.col("count").isNotNull() | (f.col("is_filled") == "No"), "No"
                ).otherwise(f.lit("Yes")),
            )
            .drop("count")
            .drop("is_filled")
        )

        df_reject = df.filter(f.col("valid") == "No")

        tot_unq_msisdn_df = df.select("msisdn").distinct().count()
        tot_unq_msisdn_valid = df_valid.select("msisdn").distinct().count()

        logger.info(
            f"Number of unique msisdn in full event dataset is {tot_unq_msisdn_df}"
        )
        logger.info(
            f"Number of unique msisdn in valid event dataset is {tot_unq_msisdn_valid}"
        )
        logger.info(f"record that is valid is {df_valid.count()}")
        logger.info(f"record that is rejected is {df_reject.count()}")

        return df_valid, df_reject


def aggregator_split_and_save_ranked_events(
    df: pyspark.sql.DataFrame,
    run_id: int,
    records: List[int],
    event_file_path: str,
    top_rank: int,
    push_channel_limits: Dict[str, int],
) -> None:
    """
    The function splits the ranked aggregator output into hourly files per
    push channel limits set then save into date wise in event file path.

    Args:
        df (DataFrame): DataFrame coming from event validation.
        run_id (int): The execution id of the aggregator process for tracking purpose.
        records (List[int]): List of FileRecord, used by aggregator to fetch file
        info for processing.
        event_file_path (str): Base path of event files.
        top_rank (int): TopN value for campaigns selection set in parameters.yml file.
        push_channel_limits (Dict[str, int]): Hourly limit of push channels set in parameters.yml file.

    Returns:

    """

    if df is not None:
        logger.info(
            f"Running aggregator_split_and_save_ranked_events function for run_id={run_id} "
            f"with {len(records)} record ids"
        )
        today = today_str()
        df = df.filter(f.col("valid") == "Yes")
        for i in range(1, top_rank + 1):
            cols = [
                "qb",
                "qb_event",
                "msisdn",
                "scheduled_push_time",
                "trx_number",
                "keyword",
                "push_channel",
                "criteria_1",
                "criteria_2",
                "criteria_3",
                "criteria_4",
                "criteria_5",
            ]
            df_rank = df.filter(f.col("rank") == i).select(cols).distinct()
            if df_rank.count() > 0:
                channel_splitter = AggChannelSplitter(key=f"Rank {i}")
                channel_splitter.split_by_channel(df_rank).reduce_by_hourly(
                    push_channel_limits
                ).combine_by_hourly().save(
                    base_path=event_file_path,
                    date_index=today,
                    suffix=f"rank_{i}",
                    cols=cols,
                )
        mark_job_files_as_completed(db_session, run_id, records)
        mark_run_as_completed(db_session, run_id)


class AggChannelSplitter(object):
    def __init__(self, key: str = None):
        self.key = key

    def split_by_channel(self, df: pyspark.sql.DataFrame) -> "AggChannelSplitter":
        """
        This function first ranks within `push_channel` and then splits
        dataframe per `push_channel` wise.
        Returns:
            AggChannelSplitter: returns its class object (`self`)
        """
        self.channel_dict = {}
        df = df.withColumn(
            "rank_by_channel",
            f.row_number().over(
                Window.partitionBy("push_channel").orderBy(
                    f.col("criteria_1").desc(),
                    f.col("criteria_2").desc(),
                    f.col("criteria_3").desc(),
                    f.col("criteria_4").desc(),
                    f.col("criteria_5").desc(),
                )
            ),
        )
        channels = [
            row.push_channel
            for row in df.select(f.col("push_channel")).distinct().collect()
        ]
        for channel in channels:
            channel_df = df.filter(f.col("push_channel") == channel)
            self.channel_dict[channel] = channel_df
        return self

    def reduce_by_hourly(self, conf: Dict[str, int]) -> "AggChannelSplitter":
        """
        The function reduces `push_channel` wise dataframe rows into hourly
        limit dataframes defined into PES system in the parameters.yml.
        Returns:
            AggChannelSplitter: returns its class object (`self`)
        """
        self.dict_of_channel_hourly_ds = {
            channel: list() for channel in self.channel_dict.keys()
        }
        for channel, channel_df in self.channel_dict.items():
            limit = conf[channel]
            maxim = channel_df.count()
            start = 0
            while start < maxim:
                hourly_limit = channel_df.filter(
                    (f.col("rank_by_channel") > start)
                    & (f.col("rank_by_channel") <= start + limit)
                )
                start = start + limit
                self.dict_of_channel_hourly_ds[channel].append(hourly_limit)
        return self

    def combine_by_hourly(self) -> "AggChannelSplitter":
        """
        The function combines same hourly index dataframes into
        single dataframe to represent as hour 1, hour 2 etc.
        Returns:
            AggChannelSplitter: returns its class object (`self`)
        """
        max_index = max(
            len(self.dict_of_channel_hourly_ds[channel])
            for channel in self.dict_of_channel_hourly_ds.keys()
        )
        self.hourly_dfs = []
        for index in range(max_index):
            _combine = []
            for channel in self.dict_of_channel_hourly_ds.keys():
                channel_iter = self.dict_of_channel_hourly_ds[channel]
                if len(channel_iter) > index:
                    df = channel_iter[index]
                    _combine.append(df)
            hourly_df = aggregator_combine(_combine)
            count = hourly_df.count()
            self.hourly_dfs.append((count, hourly_df))
        return self

    def save(
        self,
        base_path: str,
        date_index: str,
        sep: str = "|",
        suffix: str = None,
        cols: Union[str, List[str]] = None,
    ) -> None:
        """
        The function saves all combined hourly dataframes into file system.
        Args:
            base_path (str): The HDFS file system base folder path
            date_index (str): date
            sep (str, optional): Row delimiter. Defaults to "|".
            suffix (str, optional): Suffix is added to every file name. Defaults to None.
            cols (Union[str, List[str]], optional): Specific columns to select.
            Defaults to None.
        """
        num_files = len(self.hourly_dfs) + 1
        for (count, df), index in zip(self.hourly_dfs, range(1, num_files)):
            df = df.select(cols).distinct() if cols else df.select("*").distinct()
            df.coalesce(1).write.mode("overwrite").csv(
                os.path.join(
                    base_path, date_index, f"{suffix}_file{index}_{count}.txt"
                ),
                sep=sep,
            )
