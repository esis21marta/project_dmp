import logging
from datetime import datetime as dt
from typing import List

from sqlalchemy.orm.session import Session

from src.dmp.pipelines.core.entity import FileRecord, Run, WhitelistFileRecord

logger = logging.Logger(__name__)


def catch(msg: str = None):
    """
    The function is a wrapper to catch exceptions for DB APIs.
    Args:
        msg (str, optional): The message which has to be shown in logs.
    """

    def decorator(function):
        def wrapper(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except:
                err = f"There was an exception in `{function.__name__}`"
                if msg:
                    logger.error(f"{err} - {msg}")
                else:
                    logger.error(f"{err}")

        return wrapper

    return decorator


def insert_file_record(session: Session, record: FileRecord) -> None:
    """
    The function inserts records.
    Args:
        session (Session): Database session.
        record (FileRecord): A record as defined by the class
    """
    try:
        session.add(record)
        session.commit()
    except Exception as e:
        logger.error("Error occurred while file record insert :", e)


def update_file_status(
    session: Session, records: List[FileRecord], status: str
) -> None:
    """
    The function updates the status of records
    Args:
        session (Session): Database session.
        records (List[FileRecord]): A list of records
        status (str): updated status
    """
    for record in records:
        record.status = status
        record.updated_at = dt.utcnow()
    session.add_all(records)
    session.commit()


def get_file_records_by_status(session: Session, status: str) -> List[FileRecord]:
    return session.query(FileRecord).filter_by(status=status).all()


@catch(msg="Error occurred while fetching job files")
def get_job_files(session: Session, engine: str = "pes") -> List[FileRecord]:
    """
    The DB API fetches the active records for aggregator processing.
    Args:
        session (Session): Database session.
        engine (str): Domain name for campaign execution mode.

    Returns:
        List[FileRecord]: List of rows represented by class `FileRecord`
    """
    records = (
        session.query(FileRecord)
        .filter(FileRecord.status == "active", FileRecord.engine == engine)
        .all()
    )
    update_file_status(session, records, "locked")
    return records


def _update_agg_job_files_status(
    session: Session, run_id: int, status: str, ids: List[int] = None
):
    """
    The function is the helper function used for updating status of job files
    present in table `aggregator_job_files`
    Args:
        session (Session): The SQLAlchemy db session.
        run_id (int): The execution id which represents the processing of aggregator.
        status (str): Status of files in aggregator stage.
        ids (List[int]): List of aggregator job file ids

    Returns:

    """
    # FileRecord.status == "locked"
    session.query(FileRecord).filter(FileRecord.id.in_(ids)).update(
        {
            FileRecord.status: status,
            FileRecord.run_id: run_id,
            FileRecord.updated_at: dt.now(),
        },
        synchronize_session=False,
    )
    session.commit()


@catch(msg="Error occurred while updating job file status")
def mark_job_files_as_completed(
    session: Session, run_id: int, file_ids: List[int] = None
):
    """
    The DB API updates the file statuses with `completed` and executed `run_id`.
    Args:
        session (Session): Database session
        run_id (int): The active `run_id` from table aggregator_runs.
        file_ids (List[int]): List of aggregator job file ids

    """
    _update_agg_job_files_status(session, run_id, "completed", file_ids)


@catch(msg="Error occurred while rolling back job file statuses")
def rollback_job_files_to_active(
    session: Session, run_id: int, file_ids: List[int] = None
):
    """
    The function is used to rollback job files statuses to active if
    something happens during execution.
    Args:
        session (Session): Database session
        run_id (int): The active `run_id` from table aggregator_runs.
        file_ids (List[int]): List of aggregator job file ids

    Returns:

    """
    _update_agg_job_files_status(session, run_id, "active", file_ids)


@catch(msg="Error occurred while updating job file status")
def mark_job_files_as_error(session: Session, run_id: int, file_ids: List[int] = None):
    """
    The DB API updates the file statuses with `error` and executed `run_id`.
    Args:
        session (Session): Database session
        run_id (int): The active `run_id` from table aggregator_runs.
        file_ids (List[int]): List of aggregator job file ids

    """
    _update_agg_job_files_status(session, run_id, "error", file_ids)


@catch(msg="Error occurred while generating run_id")
def generate_run_id(
    session: Session,
    engine: str = "pes",
    top_rank: int = 2,
    push_channel_limits: str = None,
) -> int:
    """
    The DB API generated run_id sequence and saves in the table.
    Args:
        session (Session): Database session.
        engine (str): Domain name for campaign execution mode.
        top_rank (int): Top recommendations
        push_channel_limits (str): The max limit that push channels can send per hour in string format

    Returns:
        int: Run ID sequence.
    """
    run = Run(
        status="active",
        engine=engine,
        started_at=dt.now(),
        created_at=dt.now(),
        updated_at=dt.now(),
        top_rank=top_rank,
        channel_limits=push_channel_limits,
    )
    session.add(run)
    session.commit()
    return run.run_id


def _update_agg_runs(session: Session, run_id: int, status: str):
    """
    The function updates the status of run record
    Args:
        session (Session): Database session.
        run_id (int): Execution id for aggregator runs.
        status (str): updated status
    """
    try:
        session.query(Run).filter(Run.run_id == run_id).update(
            {Run.status: status, Run.ended_at: dt.now(), Run.updated_at: dt.now()},
            synchronize_session=False,
        )
        session.commit()
    except Exception as e:
        logger.error(f"Error occurred while updating run_id={run_id} : {e}")


def mark_run_as_completed(session: Session, run_id: int):
    """
    The API updates the run record as `completed`.
    Args:
        session (Session): Database session
        run_id (int): Execution id for aggregator runs.

    Returns:

    """
    _update_agg_runs(session, run_id, "completed")


def mark_run_as_error(session: Session, run_id: int):
    """
    The API updates the run record as `error`.
    Args:
        session (Session): Database session
        run_id (int): Execution id for aggregator runs.

    Returns:

    """
    _update_agg_runs(session, run_id, "error")


@catch(msg="Error occurred while inserting records for whitelist")
def insert_whitelist_file_records(
    session: Session, whitelist_file_paths: List[str], run_id: int
) -> None:
    """
    The function inserts whitelist file paths to table
    Args:
        session (Session): Database session.
        whitelist_file_paths (List[str]): List of whitelist file paths
        run_id (str): Execution id for aggregator runs.
    """
    try:
        records = [
            {
                "whitelist_filepath": path,
                "run_id": run_id,
                "created_at": dt.utcnow(),
                "updated_at": dt.utcnow(),
            }
            for path in whitelist_file_paths
        ]
        session.bulk_insert_mappings(WhitelistFileRecord, records)
        session.commit()
        logger.info(f"Inserted whitelist file records to aggregator_whitelist_files")
    except Exception as e:
        logger.error(
            f"Error occurred while inserting whitelist file records={run_id}: {e}"
        )
