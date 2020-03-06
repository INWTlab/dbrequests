from dbrequests import Connection
from sqlalchemy import text


class InfileConnection(Connection):
    """A Database connection."""

    def send_data(self, df, table, mode='insert', **params):
        """Sends data to table in database. This method uses the SQL LOAD DATA
        INFILE command. It will always write the input DataFrame to a CSV file,
        and then execute the SQL Statement.

        Args:
            - df (pandas DataFrame): DataFrame.
            - table_name (str): Name of SQL table.
            - mode ({'insert', 'truncate', 'replace', 'update'}): Mode of Data
              Insertion. Defaults to 'insert'.
                - 'insert': appends data. If there are duplicates in the
                  primary keys, a sql-error is returned.
                - 'truncate': replaces the complete table.
                - 'replace': replaces duplicate primary keys
                - 'update': updates duplicate primary keys
        """

        if mode == 'insert':
            self.send_data_insert(df, table, **params)
        elif mode == 'truncate':
            self.send_data_truncate(df, table, **params)
        elif mode == 'replace':
            self.send_data_replace(df, table, **params)
        elif mode == 'update':
            self.send_data_update(df, table, **params)
        else:
            raise ValueError('{} is not a known mode.'.format(mode))
        return 'Data successfully sent.'

    def send_data_insert(self, df, table):
        raise NotImplementedError('Mode is not yet implemented!')

    def send_data_truncate(self, df, table):
        raise NotImplementedError('Mode is not yet implemented!')

    def send_data_replace(self, df, table):
        raise NotImplementedError('Mode is not yet implemented!')

    def send_data_update(self, df, table):
        raise NotImplementedError('Mode is not yet implemented!')

    # def _send_data_update(self, df, table, mode = 'replace', **params):
    #     """Insert and replaces or updates the existing records via a
    #     temporary table."""
    #     self.bulk_query("CREATE TEMPORARY TABLE temporary_table_pydbtools LIKE {}".format(table))
    #     try:
    #         df.to_sql('temporary_table_pydbtools', self._conn, if_exists='append', index=False, **params)
    #         if mode == 'replace':
    #             query = """REPLACE INTO {table}
    #                 SELECT * FROM temporary_table_pydbtools;""".format(table=table)
    #         elif mode == 'update':
    #             query = """INSERT INTO {table} ({columns})
    #                 SELECT {columns} FROM temporary_table_pydbtools
    #                 ON DUPLICATE KEY UPDATE {update};""".format(
    #                 table=table,
    #                 columns=', '.join(df.columns),
    #                 update=", ".join(["{name}=VALUES({name})".format(name=name)
    #                      for name in df.columns]))
    #         self.bulk_query(query)
    #         self.bulk_query("DROP TEMPORARY TABLE temporary_table_pydbtools;")
    #     except Exception as e:
    #         self.bulk_query("DROP TEMPORARY TABLE temporary_table_pydbtools;")
    #         raise(e)
