import os
import warnings
from contextlib import contextmanager

from pandas import DataFrame
from sqlalchemy import create_engine, exc, inspect

from .connection import Connection
from .query import Query


class Database(object):
    """
    Provides useful methods to send and retrieve data as DataFrame. Manages
    opening and closing connections.

    - db_url: (str|None|dict):
        - str: a sqlalchemy url
        - None: using a sqlalchemy url from the environment variable
          'DATABASE_URL'
        - dict: a dict with credentials and connect_args
            - user
            - password
            - host      (defaults to 127.0.0.1)
            - port      (defaults to 3306)
            - db
            - dialect   (defaults to mysql) -
            - driver    (defaults to pymysql)
            - ...: further fields are added to 'connect_args'
    - sql_dir: (str|None) directory where to look for sql queries. Defaults to
      '.'.
    - escape_percentage: (bool) escape percentages when reading queries from a
      file.
    - remove_comments: (bool) remove comments when reading queries from a file.
    - kwargs:
        - creds: (dict) deprecated, provide a dict as db_url
        - ...: all arguments are passed to sqlalchemy.create_engine
    """

    _connection_class = Connection

    def __init__(self, db_url=None, sql_dir=None,
                 escape_percentage=False, remove_comments=False, **kwargs):

        self.sql_dir = sql_dir or os.getcwd()
        self._escape_percentage = escape_percentage
        self._remove_comments = remove_comments
        kwargs = self._init_db_url(db_url, **kwargs)
        self._init_engine(**kwargs)
        self._open = True

    def _init_db_url(self, db_url, **kwargs):
        if db_url is None:
            db_url = os.environ.get('DATABASE_URL')
            if db_url is None:
                db_url = kwargs.pop('creds', None)
                if db_url is not None:
                    warnings.warn(
                        "Parameter 'creds' is depreacated in favor of db_url.",
                        DeprecationWarning)
                else:
                    raise ValueError('db_url is missing')
        if isinstance(db_url, str):
            self.db_url = db_url
        elif isinstance(db_url, dict):
            db_url = db_url.copy()
            self.db_url = '{}+{}://{}:{}@{}:{}/{}'.format(
                db_url.pop('dialect', 'mysql'),
                db_url.pop('driver', 'pymysql'),
                db_url.pop('user'),
                db_url.pop('password'),
                db_url.pop('host', '127.0.0.1'),
                db_url.pop('port', 3306),
                db_url.pop('db'))
            connect_args = kwargs.pop('connect_args', {})
            connect_args.update(db_url)
            if len(connect_args):
                kwargs['connect_args'] = connect_args
        else:
            raise ValueError('db_url has to be a str or dict')
        return kwargs

    def _init_engine(self, **kwargs):
        # We have this method, so that subclasses may override the init
        # process.
        self._engine = create_engine(self.db_url, **kwargs)

    def close(self):
        """Close the connection."""
        self._engine.dispose()
        self._open = False

    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()

    def __repr__(self):
        return '<Database open={}>'.format(self._open)

    def get_table_names(self):
        """Returns a list of table names for the connected database."""
        return inspect(self._engine).get_table_names()

    def get_connection(self):
        """Get a connection from the sqlalchemy engine."""
        if not self._open:
            raise exc.ResourceClosedError('Database closed.')
        return self._connection_class(self._engine.connect())

    def __get_query_text(self, query, escape_percentage, remove_comments, **params):
        """Private wrapper for accessing the text of the query."""
        escape_percentage = escape_percentage or self._escape_percentage
        remove_comments = remove_comments or self._remove_comments
        sql = Query(query, sql_dir=self.sql_dir, escape_percentage=escape_percentage,
                    remove_comments=remove_comments, **params)
        return sql.text

    def send_query(self, query, escape_percentage=None, remove_comments=None, **params):
        """Convenience wrapper for executing a SQL-query as string or a SQL-file. Parameters can,
        optionally, be provided to the sql-file and to pandas.read_sql. Returns a pandas DataFrame.

        Args:
        - query (str): query may be:
            - a sql query as string
            - a file-path as string
            - the name of a file as string (with or without .sql)
            - a sqlalchemy selectable
        """
        text = self.__get_query_text(
            query, escape_percentage, remove_comments, **params)
        return self.query(text, **params)

    def send_bulk_query(self, query, escape_percentage=None, remove_comments=None, **params):
        """Convenience wrapper for executing a bulk SQL-query like insert, update, create or delete
        as string or a SQL-file. Parameters can, optionally, be provided to the sql-file and to pandas.read_sql.
        Returns nothing.

        Args:
        - query (str): query may be:
            - a sql query as string
            - a file-path as string
            - the name of a file as string (with or without .sql)
            - a sqlalchemy selectable
        """
        text = self.__get_query_text(
            query, escape_percentage, remove_comments, **params)
        return self.bulk_query(text, **params)

    def send_data(self, df: DataFrame, table, mode='insert', **params):
        """Sends data to table in database. If the table already exists, different modes of
        insertion are provided.

        Args:
            - df (pandas DataFrame): DataFrame.
            - table_name (str): Name of SQL table.
            - mode ({'insert', 'truncate', 'replace', 'update'}): Mode of Data Insertion. Defaults to 'insert'.
                - 'insert': appends data. If there are duplicates in the primary keys, a sql-error is returned.
                - 'truncate': replaces the complete table.
                - 'replace': replaces duplicate primary keys
                - 'update': updates duplicate primary keys
        """
        with self.transaction() as conn:
            return conn.send_data(df, table, mode, **params)

    def query(self, query, **params):
        """Executes the given SQL query against the Database via pandas. Parameters can,
        optionally, be provided. Returns a pandas DataFrame.
        """
        with self.get_connection() as conn:
            return conn.query(query, **params)

    def bulk_query(self, query, **params):
        """Bulk insert or update."""
        with self.get_connection() as conn:
            conn.bulk_query(query, **params)

    @contextmanager
    def transaction(self):
        """Execute a transaction on this Database."""
        conn = self.get_connection()
        tx = conn.transaction()
        try:
            yield conn
            tx.commit()
        except BaseException as e:
            tx.rollback()
            raise e
        finally:
            conn.close()
