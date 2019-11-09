from sqlalchemy import text
from pandas import read_sql
import os
import inspect

class Connection(object):
    """A Database connection."""

    def __init__(self, connection):
        self._conn = connection
        self.open = not connection.closed

    def close(self):
        self._conn.close()
        self.open = False

    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()

    def __repr__(self):
        return '<Connection open={}>'.format(self.open)

    def query(self, query, **params):
        """Executes the given SQL query against the connected Database.
        Parameters can, optionally, be provided. Uses pandas.read_sql and returns a Pandas Dataframe
        """

        # Execute the given query.
        params = {k: v for k, v in params.items() if k in inspect.getfullargspec(read_sql).args}
        results = read_sql(query, self._conn, **params)
        return results

    def bulk_query(self, query, **params):
        """Bulk insert or update."""
        params = {k: v for k, v in params.items() if k in inspect.getfullargspec(self._conn.execute).args}
        self._conn.execute(text(query), **params)


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

        if mode == 'insert':
            self._send_data_pandas(df, table, pandas_mode='append', **params)
        elif mode == 'truncate':
            self._send_data_pandas(df, table, pandas_mode='replace', **params)
        elif mode in ['replace', 'update']:
            self._send_data_update(df, table, mode, **params)
        else:
            raise ValueError('{} is not a known mode.'.format(mode))
        return 'Data successful sent.'

    def _send_data_pandas(self, df, table, pandas_mode='append', **params):
        """Uses the pandas-method to_sql to send data."""

        df.to_sql(table, self._conn, if_exists=pandas_mode, index=False, **params)

    def _send_data_update(self, df, table, mode = 'replace', **params):
        """Insert and replaces or updates the existing records via a temporary table."""
        self.bulk_query("CREATE TEMPORARY TABLE temporary_table_pydbtools LIKE {}".format(table))
        try:
            df.to_sql('temporary_table_pydbtools', self._conn, if_exists='append', index=False, **params)
            if mode == 'replace':
                query = """REPLACE INTO {table}
                    SELECT * FROM temporary_table_pydbtools;""".format(table=table)
            elif mode == 'update':
                query = """INSERT INTO {table} ({columns})
                    SELECT {columns} FROM temporary_table_pydbtools
                    ON DUPLICATE KEY UPDATE {update};""".format(
                    table=table,
                    columns=', '.join(df.columns),
                    update=", ".join(["{name}=VALUES({name})".format(name=name)
                         for name in df.columns]))
            self.bulk_query(query)
            self.bulk_query("DROP TEMPORARY TABLE temporary_table_pydbtools;")
        except Exception as e:
            self.bulk_query("DROP TEMPORARY TABLE temporary_table_pydbtools;")
            raise(e)

    def transaction(self):
        """Returns a transaction object. Call ``commit`` or ``rollback``
        on the returned object as appropriate."""

        return self._conn.begin()
