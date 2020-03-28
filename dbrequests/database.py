import os
from contextlib import contextmanager

from pandas import DataFrame
from sqlalchemy import create_engine, exc, inspect

from .connection import Connection as DefaultConnection
from .query import Query


class Database(object):
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

    def __init__(self, db_url=None, creds=None, sql_dir=None, connection_class=DefaultConnection,
                 escape_percentage=False, remove_comments=False, **kwargs):
        # If no db_url was provided, fallback to $DATABASE_URL or creds.
        self.db_url = db_url or os.environ.get('DATABASE_URL')
        self.sql_dir = sql_dir or os.getcwd()
        if not self.db_url:
            try:
                user = creds['user']
                password = creds['password']
                host = creds['host']
                db = creds['db']
                dialect = creds.get('dialect', 'mysql')
                driver = creds.get('driver', 'pymysql')
                self.db_url = '{}+{}://{}:{}@{}/{}'.format(dialect, driver, user, password, host, db)
            except:
                raise ValueError('You must provide a db_url or proper creds.')

        self._escape_percentage = escape_percentage
        self._remove_comments = remove_comments
        self._open = False
        self.open(**kwargs)
        self.connection_class = connection_class

    def open(self, **kwargs):
        """Open a connection."""
        if not self._open:
            self._engine = create_engine(self.db_url, **kwargs)
            self._open = True
        return self._open

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

        # Setup SQLAlchemy for Database inspection.
        return inspect(self._engine).get_table_names()

    def get_connection(self):
        """Get a connection to this Database. Connections are retrieved from a
        pool.
        """
        if not self._open:
            raise exc.ResourceClosedError('Database closed.')

        return self.connection_class(self._engine.connect())

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
        text = self.__get_query_text(query, escape_percentage, remove_comments, **params)
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
        text = self.__get_query_text(query, escape_percentage, remove_comments, **params)
        return self.bulk_query(text, **params)

    def send_data(self, df, table, mode='insert', **params):
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
        if not isinstance(df, DataFrame):
            raise TypeError('df has to be a pandas DataFrame.')
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
