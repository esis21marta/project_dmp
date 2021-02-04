from sqlalchemy import create_engine
from sqlalchemy.orm.session import sessionmaker


class DBConnection:
    def __init__(self, user, password, ip_address, port, db=None):
        self._conn_str = "postgresql://{0}:{1}@{2}:{3}".format(
            user, password, ip_address, port
        )
        if db is not None:
            self._conn_str = self._conn_str + "/{0}".format(db)

    def build(self):
        return create_engine(self._conn_str)


class DBSession:

    _session = None

    def __new__(cls, engine):
        if cls._session is None:
            Session = sessionmaker()
            Session.configure(bind=engine)
            cls._session = Session()
        return cls._session
