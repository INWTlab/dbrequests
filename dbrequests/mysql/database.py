from dbrequests.database import Database as SuperDatabase
from .connection import Connection as MysqlConnection
from sqlalchemy import exc, create_engine


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

    def open(self, **kwargs):
        """Open a connection."""
        if not self._open:
            url_with_infile = self.db_url + '?local_infile=1'
            self._engine = create_engine(url_with_infile, **kwargs)
            self._open = True
        return self._open
        
    def get_connection(self):
        """Get a connection to this Database. Connections are retrieved from a
        pool.
        """
        if not self.open:
            raise exc.ResourceClosedError('Database closed.')

        return MysqlConnection(self._engine.connect())
