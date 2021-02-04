from kedro.config import ConfigLoader

from .db_manager import DBConnection, DBSession


def _load_db_credentials():
    config_loader = ConfigLoader("conf/aggregator/")
    credentials = config_loader.get("**/credentials.yml")["agg_db_credentials"]
    return (
        credentials["db_user"],
        credentials["db_password"],
        credentials["db_name"],
        credentials["db_ip_address"],
        credentials["db_port"],
    )


user, password, db, ip, port = _load_db_credentials()
db_connection = DBConnection(user, password, ip, port, db).build()
db_session = DBSession(db_connection)
