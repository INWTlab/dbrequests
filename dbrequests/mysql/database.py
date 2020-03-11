from dbrequests.database import Database as SuperDatabase
from .connection import Connection as MysqlConnection
from sqlalchemy import exc


class Database(SuperDatabase):
    """A Database. Encapsulates a url and an SQLAlchemy engine with a pool of
    connections.

    The url to the database can be provided directly or via a credentials-
    dictionary `creds` with keys:
        - host
        - db
        - user
        - password
        - dialect (defaults to mysql)
        - driver (defaults to pymysql)
    """
    def get_connection(self):
        """Get a connection to this Database. Connections are retrieved from a
        pool.
        """
        if not self.open:
            raise exc.ResourceClosedError('Database closed.')

        return MysqlConnection(self._engine.connect())
