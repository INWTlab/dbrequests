"""Implements the send_data method using load data local infile.

This is mysql and mariadb compliant.
"""
from tempfile import NamedTemporaryFile as TmpFile
from dbrequests import Connection as SuperConnection
from contextlib import contextmanager


class Connection(SuperConnection):
    """A Database connection."""

    def send_data(self, df, table, mode='insert', **params):
        """Send data to table in database.

        This method uses the SQL LOAD DATA LOCAL INFILE command. It will always
        write the input DataFrame to a CSV file, and then execute the SQL
        Statement.

        Args:
            - df (pandas DataFrame): DataFrame.
            - table_name (str): Name of SQL table.
            - mode ({'insert', 'truncate', 'replace', 'update', 'delete'}):
            Mode of Data Insertion. Defaults to 'insert'.
                - 'insert': appends data. If there are duplicates in the
                  primary keys, a sql-error is returned.
                - 'truncate': drop table and recreate it. There is no comming
                back, no rollback.
                - 'delete': a safer truncate, but more expensive. We can roll
                back from this one.
                - 'replace': replaces duplicate primary keys
                - 'update': updates duplicate primary keys
        """
        if mode == 'insert':
            self._send_data_insert(df, table, **params)
        elif mode == 'replace':
            self._send_data_replace(df, table, **params)
        elif mode == 'truncate':
            self._send_data_truncate(df, table, **params)
        elif mode == 'delete':
            self._send_data_delete(df, table, **params)
        elif mode == 'update':
            self._send_data_update(df, table, **params)
        else:
            raise ValueError('{} is not a known mode.'.format(mode))
        return 'Data successfully sent.'

    def _send_data_insert(self, df, table):
        with TmpFile(mode='w', newline='') as tf:
            self._write_csv(df, tf)
            self._infile_csv(tf, df, table)

    def _send_data_replace(self, df, table):
        with TmpFile(mode='w', newline='') as tf:
            self._write_csv(df, tf)
            self._infile_csv(tf, df, table, replace='replace')

    def _send_data_truncate(self, df, table):
        self.bulk_query("truncate table {table};".format(table=table))
        self._send_data_insert(df, table)

    def _send_data_delete(self, df, table):
        self.bulk_query("delete from {table};".format(table=table))
        self._send_data_insert(df, table)

    def _send_data_update(self, df, table, mode='replace', **params):
        # We override the method from the super-class and need to honor the
        # interface. However, mode and **params are not needed here.
        #
        # TODO: We may want to delete columns that are not in df. Currently
        # this method will enforce that there are default values for fields not
        # part of df. This is an unnecessary restriction.
        with self._temporary_table(table) as tmp_table:
            self._send_data_insert(df, tmp_table)
            self._insert_update(df, table, tmp_table)

    def _write_csv(self, df, file):
        df.to_csv(path_or_buf=file.name, line_terminator='\n',
                  chunksize=10000000, encoding='utf-8', index=False,
                  sep=',', na_rep='\\N', header=False)

    def _infile_csv(self, file, df, table, replace=''):
        self.bulk_query("""
        load data local infile '{path}'
        {replace}
        into table `{table}`
        character set utf8mb4
        fields terminated by ','
        optionally enclosed by '\"'
        lines terminated by '\\n'
        ({columns});""".format(
            path=file.name,
            replace=replace,
            table=table,
            columns=self._sql_cols(df)))

    def _insert_update(self, df, table, tmp_table):
        self.bulk_query('''
        insert into `{table}` ({columns})
        select {columns}
        from `{tmp_table}`
        on duplicate key update {update};'''.format(
            table=table,
            columns=self._sql_cols(df),
            tmp_table=tmp_table,
            update=self._sql_update(df)))

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
            self.bulk_query('drop temporary table {};'.format(tmp_table))
            raise e
        finally:
            self.bulk_query('drop temporary table {};'.format(tmp_table))

    @staticmethod
    def _sql_cols(df):
        cols = ', '.join(['`' + str(name) + '`' for name in df.columns.values])
        return cols

    @staticmethod
    def _sql_update(df):
        stmt = ", ".join(
            ["`{name}`=values(`{name}`)".format(name=str(name))
                for name in df.columns.values])
        return stmt
