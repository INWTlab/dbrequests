import os
from sqlalchemy import create_engine, inspect, exc
from contextlib import contextmanager
from .connection import Connection
from pandas import DataFrame

class Database(object):
    """A Database. Encapsulates a url and an SQLAlchemy engine with a pool of
    connections.

    The url to the database can be provided directly or via a credentials-dictionary `creds` with keys:
        - host
        - db
        - user
        - password
        - dialect (defaults to mysql)
        - driver (defaults to pymysql)
    """

    def __init__(self, db_url=None, creds=None, sql_dir=None, **kwargs):
        # If no db_url was provided, fallback to $DATABASE_URL.
        self.db_url = db_url
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

        # Create an engine.
        self._engine = create_engine(self.db_url, **kwargs)
        self.open = True

    def close(self):
        """Closes the Database."""
        self._engine.dispose()
        self.open = False

    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()

    def __repr__(self):
        return '<Database open={}>'.format(self.open)

    def get_table_names(self, internal=False):
        """Returns a list of table names for the connected database."""

        # Setup SQLAlchemy for Database inspection.
        return inspect(self._engine).get_table_names()

    def get_connection(self):
        """Get a connection to this Database. Connections are retrieved from a
        pool.
        """
        if not self.open:
            raise exc.ResourceClosedError('Database closed.')

        return Connection(self._engine.connect())

    def send_query(self, query, **params):
        """Convenience wrapper for executing a SQL-query as string or a SQL-file. Parameters can,
        optionally, be provided to the sql-file and to pandas.read_sql. Returns a pandas DataFrame.

        Args:
        - query (str): query may be:
            - a sql query as string
            - a file-path as string
            - the name of a file as string (with or without .sql)
            - a sqlalchemy selectable
        """
        if isinstance(query, str) and not (' ' in query):
            if not '.sql' in query:
                query = query + '.sql'
            try:
                out = self.query_file(query, **params)
            except IOError:
                query = os.path.join(self.sql_dir, query)
                out = self.query_file(query, **params)
        else:
            out = self.query(query, **params)
        return out

    def send_bulk_query(self, query, **params):
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
        if isinstance(query, str) and not (' ' in query):
            if not '.sql' in query:
                query = query + '.sql'
            try:
                self.bulk_query_file(query, **params)
            except IOError:
                query = os.path.join(self.sql_dir, query)
                self.bulk_query_file(query, **params)
        else:
            self.bulk_query(query, **params)

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
        with self.get_connection() as conn:
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

    def query_file(self, path, **params):
        """Like Database.query, but takes a filename to load a query from."""

        with self.get_connection() as conn:
            return conn.query_file(path, **params)

    def bulk_query_file(self, path, **params):
        """Like Database.bulk_query, but takes a filename to load a query from."""

        with self.get_connection() as conn:
            conn.bulk_query_file(path, **params)

    @contextmanager
    def transaction(self):
        """A context manager for executing a transaction on this Database."""

        conn = self.get_connection()
        tx = conn.transaction()
        try:
            yield conn
            tx.commit()
        except:
            tx.rollback()
        finally:
            conn.close()
