from dbrequests.database import Database as SuperDatabase
from .connection import Connection as MysqlConnection
from sqlalchemy import create_engine


class Database(SuperDatabase):
    """This class is derived from `dbrequests.Database`.

    It uses the dbrequests.mysql.Connection which implements a different
    strategy for writing data into databases (load data local infile).
    Encapsulates a url and an SQLAlchemy engine with a pool of connections.

    The url to the database can be provided directly or via a credentials-
    dictionary `creds` with keys: - host - db - user - password - dialect
    (defaults to mysql) - driver (defaults to pymysql)
    """
    def __init__(self, db_url=None, creds=None, sql_dir=None,
                 escape_percentage=False, remove_comments=False, **kwargs):
        super().__init__(db_url=db_url, creds=creds, sql_dir=sql_dir,
                         connection_class=MysqlConnection,
                         escape_percentage=escape_percentage,
                         remove_comments=remove_comments,
                         **kwargs)

    def open(self, **kwargs):
        """Open a connection."""
        if not self._open:
            url_with_infile = self.db_url + '?local_infile=1'
            self._engine = create_engine(url_with_infile, **kwargs)
            self._open = True
        return self._open
