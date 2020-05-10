"""
Implements the backend for MySQL databases. This is mysql and mariadb
compliant.
"""

from inspect import getfullargspec as getargs
from contextlib import contextmanager
from tempfile import NamedTemporaryFile as TmpFile
from datatable import dt, f, str64, Frame, rbind, join
from dbrequests import Connection as SuperConnection


class Connection(SuperConnection):
    """A Database connection."""

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

    def _send_data_update_diffs(self, df, table, **params):
        diffs = self._make_diffs(df, table, **params)
        self._send_data_update(diffs, table)

    def _send_data_insert_diffs(self, df, table, **params):
        diffs = self._make_diffs(df, table, **params)
        self._send_data_insert(diffs, table)

    def _send_data_replace_diffs(self, df, table, **params):
        diffs = self._make_diffs(df, table, **params)
        self._send_data_replace(diffs, table)

    def _write_csv(self, df, file):
        # Before writing, we need to convert all columns to strings for two
        # reasons:
        # - We have to convert any obj64 types to str64: Frame.to_csv can't
        #   process them.
        # - We have to replace None with NULL to tell MySQL, that we have
        #   actual NULL values. An empty cell is sometimes, but not always a
        #   NULL value. See #30
        df = df[:, f[:].remove(f[:]).extend(str64(f[:]))][:, df.names]
        df.replace(None, 'NULL')
        df.to_csv(path=file.name, header=False)

    def _infile_csv(self, file, df, table, replace=''):
        self.bulk_query("""
        load data local infile '{path}'
        {replace}
        into table `{table}`
        character set utf8mb4
        fields terminated by ','
        optionally enclosed by '\"'
        escaped by ''
        lines terminated by '\\n'
        ({columns});""".format(
            path=file.name,
            replace=replace,
            table=table,
            columns=self._sql_cols(df.names)))

    def _insert_update(self, df, table, tmp_table):
        self.bulk_query('''
        insert into `{table}` ({columns})
        select {columns}
        from `{tmp_table}`
        on duplicate key update {update};'''.format(
            table=table,
            columns=self._sql_cols(df.names),
            tmp_table=tmp_table,
            update=self._sql_update(df)))

    @staticmethod
    def _sql_cols(names):
        cols = ', '.join(['`' + str(name) + '`' for name in names])
        return cols

    @staticmethod
    def _sql_update(df):
        stmt = ", ".join(
            ["`{name}`=values(`{name}`)".format(name=str(name))
                for name in df.names])
        return stmt

    def query(self, query, **params):
        """
        Executes the given SQL query against the connected dsatabase.
        """
        chunksize = params.pop('chunksize', 100000)
        to_pandas = params.pop('to_pandas', True)
        with self._cursor() as cursor:
            params = {k: v for k, v in params.items()
                      if k in getargs(cursor.execute).args}
            cursor.execute(query, **params)
            fields = [i[0] for i in cursor.description]
            res = []
            while True:
                result = cursor.fetchmany(chunksize)
                if not result:
                    break
                res.append(Frame(result))
        frame = rbind(res, bynames=False)
        if frame.shape == (0, 0):
            frame = Frame({n: [] for n in fields})
        else:
            frame.names = fields
        if to_pandas:
            frame = frame.to_pandas()
        return frame

    def _make_diffs(self, df, table, keys=None, in_range=None,
                    chunksize=10000000):
        # Here is the strategy to construct diffs:
        # - pull down the complete target table
        # - to spare memory we do this chunkwise
        # - for each chunk, do a left join on the column set 'keys'
        # - drop the matched lines
        # - repeat
        # Dealing with input params:
        if keys is None:
            keys = df.names
        if in_range:
            where = 'where `{col}` >= {min} and `{col}` <= {max}'.format(
                col=in_range,
                min=df[:, dt.min(f[in_range])][0, 0],
                max=df[:, dt.max(f[in_range])][0, 0],
            )
        else:
            where = ''
        # Executing the strategy:
        df.key = keys
        with self._cursor() as cursor:
            cursor.execute('select distinct {cols} from {table} {where};'.format(
                cols=self._sql_cols(keys), table=table, where=where))
            while True:
                result = cursor.fetchmany(chunksize)
                if not result:
                    break
                # prepare the data
                result = Frame(result)
                result.names = keys
                result = result[:, f[:].extend({'__a__': 1})]
                result.key = keys
                # do the join
                df = df[:, :, join(result)]
                # remove the duplicates
                del df[f.__a__ == 1, :]
                del df[:, '__a__']
        return df

    @contextmanager
    def _cursor(self):
        cursor = self._conn.connection.cursor()
        try:
            yield cursor
        except BaseException as error:
            raise error
        finally:
            cursor.close()
