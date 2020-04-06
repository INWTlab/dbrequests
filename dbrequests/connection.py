import inspect
import warnings
from contextlib import contextmanager

from sqlalchemy import text
from pandas import read_sql


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
        params = {k: v for k, v in params.items(
        ) if k in inspect.getfullargspec(read_sql).args}
        results = read_sql(query, self._conn, **params)
        return results

    def bulk_query(self, query, **params):
        """Bulk insert or update."""
        params = {k: v for k, v in params.items(
        ) if k in inspect.getfullargspec(self._conn.execute).args}
        self._conn.execute(text(query), **params)

    def send_data(self, df, table, mode='insert', **params):
        """
        Sends data to table in database. If the table already exists, different
        modes of insertion are provided.

        Args:
            - df (pandas DataFrame): DataFrame.
            - table_name (str): Name of SQL table.
            - mode ({'insert', 'truncate', 'replace', 'update'}): Mode of Data Insertion. Defaults to 'insert'.
                - 'insert': appends data. If there are duplicates in the primary keys, a sql-error is returned.
                - 'truncate': replaces the complete table.
                - 'replace': replaces duplicate primary keys
                - 'update': updates duplicate primary keys
                - Derived classes may implement additional modes.
        """
        mode_implementation = '_send_data_{}'.format(mode)
        if hasattr(self, mode_implementation):
            getattr(self, mode_implementation)(df, table, **params)
        else:
            raise ValueError('{} is not a known mode'.format(mode))
        return 'Data successfully sent.'

    def _send_data_insert(self, df, table, **params):
        self._send_data_pandas(df, table, 'append', **params)

    def _send_data_truncate(self, df, table, **params):
        self._send_data_pandas(df, table, 'replace', **params)

    def _send_data_replace(self, df, table, **params):
        warnings.warn("""
        The mode 'replace' is deprecated and will be removed in due time. Please
        change to the SQL dialect specific implementation.""", DeprecationWarning)
        with self._temporary_table(table) as tmp_table:
            self._send_data_insert(df, tmp_table)
            self.bulk_query(
                'REPLACE INTO `{table}` SELECT * FROM `{tmp_table}`'.format(
                    table=table,
                    tmp_table=tmp_table
                ))

    def _send_data_update(self, df, table, **params):
        warnings.warn("""
        The mode 'update' is deprecated and will be removed in due time. Please
        change to the SQL dialect specific implementation.""", DeprecationWarning)
        with self._temporary_table(table) as tmp_table:
            self._send_data_insert(df, tmp_table)
            query = """INSERT INTO `{table}` ({columns})
                    SELECT {columns} FROM `{tmp_table}`
                    ON DUPLICATE KEY UPDATE {update};""".format(
                    table=table,
                    tmp_table=tmp_table,
                    columns=', '.join(df.columns),
                    update=", ".join(["{name}=VALUES({name})".format(name=name)
                                      for name in df.columns]))
            self.bulk_query(query)

    def _send_data_pandas(self, df, table, pandas_mode='append', **params):
        """Uses the pandas-method to_sql to send data."""

        df.to_sql(table, self._conn, if_exists=pandas_mode,
                  index=False, **params)

    @contextmanager
    def _temporary_table(self, table):
        tmp_table = 'tmp_dbrequests_' + table
        self.bulk_query('''
        create temporary table `{tmp_table}` like `{table}`;'''.format(
            tmp_table=tmp_table,
            table=table
        ))
        try:
            yield tmp_table
        except BaseException as e:
            self.bulk_query(
                'drop temporary table if exists {};'.format(tmp_table))
            raise e
        finally:
            self.bulk_query(
                'drop temporary table if exists {};'.format(tmp_table))

    def transaction(self):
        """Returns a transaction object. Call ``commit`` or ``rollback``
        on the returned object as appropriate."""

        return self._conn.begin()
