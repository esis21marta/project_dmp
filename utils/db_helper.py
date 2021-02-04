import logging

from sqlalchemy import create_engine

from .get_kedro_context import GetKedroContext


def get_db_engine(credentials):
    context = GetKedroContext.get_context()
    con_string = context._get_config_credentials()[credentials]["con"]
    engine = create_engine(con_string, echo=False)
    return engine


def execute_raw_query(query, credentials):
    engine = get_db_engine(credentials)
    logger = logging.getLogger(__name__)
    logger.info(f"Executing {query}")
    with engine.connect() as con:
        con.execute(query)
