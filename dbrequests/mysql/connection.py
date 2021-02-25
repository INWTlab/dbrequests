"""
Implements the backend for MySQL databases. This is mysql and mariadb
compliant.
"""

import logging
import re
from contextlib import contextmanager
from inspect import getfullargspec as getargs

from datatable import Frame, dt, f, join, rbind, str64
from dbrequests import Connection as SuperConnection
from dbrequests.temp_file import temp_file


class Connection(SuperConnection):
    """A Database connection."""

    def send_delete(self, df: Frame, table: str, mode: str, **params) -> int:
        """See mysql.Database.send_delete for documentation."""
        mode_implementation = "_send_delete_{}".format(mode)
        if hasattr(self, mode_implementation):
            affected_rows = getattr(self, mode_implementation)(df, table, **params)
        else:
            raise ValueError("{} is not a known mode".format(mode))
        return affected_rows

    def _send_delete_in_set(self, df, table, **params):
        with self._temporary_table(table, df.names, params.pop("with_temp", True)) as tmp_table:
            self._send_data_insert(df, tmp_table)
            return self._delete_set(table, tmp_table, df.names, False)

    def _send_delete_not_in_set(self, df, table, **params):
        with self._temporary_table(table, df.names, params.pop("with_temp", True)) as tmp_table:
            self._send_data_insert(df, tmp_table)
            return self._delete_set(table, tmp_table, df.names, True)

    def _delete_set(self, table, tmp_table, cols, not_in=True):
        if not_in:
            not_in = "not"
        else:
            not_in = ""
        where_stmt = " and ".join(
            [
                "`{col}` {not_in} in (select distinct `{col}` from `{tmp_table}`)".format(
                    col=col, not_in=not_in, tmp_table=tmp_table
                )
                for col in cols
            ]
        )
        delete_stmt = "delete from `{table}` where {where_stmt};".format(table=table, where_stmt=where_stmt)
        return self.bulk_query(delete_stmt)

    def _send_delete_in_join(self, df, table, **params):
        with self._temporary_table(table, df.names, params.pop("with_temp", True)) as tmp_table:
            self._send_data_insert(df, tmp_table)
            return self._delete_join(table, tmp_table, df.names)

    def _send_delete_not_in_join(self, df, table, **params):
        with self._temporary_table(table, df.names, params.pop("with_temp", True)) as tmp_table:
            self._send_data_insert(df, tmp_table)
            return self._delete_join(table, tmp_table, df.names, False)

    def _send_delete_in_delete_col(self, df, table, **params):
        logging.info(f"delete {df.shape[0]} rows")
        if df.shape[0] > 0:
            df = df[:, f[:].extend({"delete": 1})]
            self._send_data_replace(df, table)
            self.bulk_query("delete from `{}` where `delete` = 1;".format(table))

    def _delete_join(self, table, tmp_table, df_names, not_null=True):
        if not_null:
            not_null = "not"
        else:
            not_null = ""
        delete_query = """
            delete `{table}` from
                `{table}` left join `{tmp_table}` as tt using({tmp_cols})
            where `tt`.{tmp_first_col} is {not_null} NULL;
            """.format(
            table=table,
            tmp_table=tmp_table,
            tmp_cols=self._sql_cols(df_names),
            tmp_first_col=self._sql_cols([df_names[0]]),
            not_null=not_null,
        )
        return self.bulk_query(delete_query)

    def _send_data_insert(self, df, table):
        logging.info(f"sending data with insert: {df.shape[0]} rows")
        with temp_file() as tf:
            self._write_csv(df, tf)
            self._infile_csv(tf, df, table)

    def _send_data_replace(self, df, table):
        logging.info(f"sending data with replace: {df.shape[0]} rows")
        with temp_file() as tf:
            self._write_csv(df, tf)
            self._infile_csv(tf, df, table, replace="replace")

    def _send_data_truncate(self, df, table):
        logging.info(f"sending data with truncate: {df.shape[0]} rows")
        self.bulk_query("truncate table {table};".format(table=table))
        self._send_data_insert(df, table)

    def _send_data_delete(self, df, table):
        logging.info(f"sending data with delete: {df.shape[0]} rows")
        self.bulk_query("delete from {table};".format(table=table))
        self._send_data_insert(df, table)

    def _send_data_update(self, df, table, mode="replace", **params):
        logging.info(f"sending data with update: {df.shape[0]} rows")
        with_temp = params.pop("with_temp", True)
        with self._temporary_table(table, df.names, with_temp) as tmp_table:
            self._send_data_insert(df, tmp_table)
            self._insert_update(df, table, tmp_table)

    def _send_data_update_diffs(self, df, table, **params):
        logging.info(f"sending data with update_diffs: {df.shape[0]} rows")
        remote_table = self._get_diff_table(df, table, **params)
        diffs = self._make_diffs(df, remote_table, **params)
        self._send_data_update(diffs, table, **params)

    def _send_data_insert_diffs(self, df, table, **params):
        logging.info(f"sending data with insert_diffs: {df.shape[0]} rows")
        remote_table = self._get_diff_table(df, table, **params)
        diffs = self._make_diffs(df, remote_table, **params)
        self._send_data_insert(diffs, table)

    def _send_data_replace_diffs(self, df, table, **params):
        logging.info(f"sending data with replace_diffs: {df.shape[0]} rows")
        remote_table = self._get_diff_table(df, table, **params)
        diffs = self._make_diffs(df, remote_table, **params)
        self._send_data_replace(diffs, table)

    def _send_data_sync_diffs(self, df, table, **params):
        logging.info(f"sending data with sync_diffs: {df.shape[0]} rows")
        remote_table = self._get_diff_table(df, table, **params)
        diffa = self._make_diffs(df, remote_table, **params)
        diffb = self._make_diffs(remote_table, df, keys=self._get_primary_key(table), **params)
        self._send_delete_in_delete_col(diffb, table, **params)
        self._send_data_replace(diffa, table)

    def _write_csv(self, df, file):
        # Before writing, we need to convert all columns to strings for two
        # reasons:
        # - We have to convert any obj64 types to str64: Frame.to_csv can't
        #   process them.
        # - We have to replace None with NULL to tell MySQL, that we have
        #   actual NULL values. An empty cell is sometimes, but not always a
        #   NULL value. See #30
        # - We have to check if the frame is empty. If so we have to
        #   circumvent a  bug in datatable: see #36
        if df.shape[0] == 0:
            return None
        df = df[:, f[:].remove(f[:]).extend(str64(f[:]))][:, df.names]
        df.replace(None, "NULL")
        df.to_csv(path=file, header=False)

    def _infile_csv(self, file, df, table, replace=""):
        # On Windows paths are denoted by '\\'. A backslash in the sql statement
        # has to be escaped; so we have to escape both of them -> 4 backslashes.
        # In the regular expression every backslash, of those 4, needs to be
        # escaped, leading to 8. This should be without effect on Linux.
        file = re.sub("\\\\", "\\\\\\\\", file)
        query = """
        load data local infile '{path}'
        {replace}
        into table `{table}`
        character set utf8mb4
        fields terminated by ','
        optionally enclosed by '\"'
        escaped by ''
        lines terminated by '\\n'
        ({columns});""".format(
                path=file,
                replace=replace,
                table=table,
                columns=self._sql_cols(df.names),
            )
        self.bulk_query(query)

    def _insert_update(self, df, table, tmp_table):
        self.bulk_query(
            """
        insert into `{table}` ({columns})
        select {columns}
        from `{tmp_table}`
        on duplicate key update {update};""".format(
                table=table, columns=self._sql_cols(df.names), tmp_table=tmp_table, update=self._sql_update(df)
            )
        )

    @staticmethod
    def _sql_cols(names):
        cols = ", ".join(["`" + str(name) + "`" for name in names])
        return cols

    @staticmethod
    def _sql_update(df):
        stmt = ", ".join(["`{name}`=values(`{name}`)".format(name=str(name)) for name in df.names])
        return stmt

    def query(self, query, **params):
        """
        Executes the given SQL query against the connected dsatabase.
        """
        chunksize = params.pop("chunksize", 100000)
        to_pandas = params.pop("to_pandas", True)
        with self._cursor() as cursor:
            params = {k: v for k, v in params.items() if k in getargs(cursor.execute).args}
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

    def _get_diff_table(self, df, table, keys=None, in_range=None, **params):
        if keys is None:
            keys = df.names
        if in_range:
            where = "where `{col}` >= {min} and `{col}` <= {max}".format(
                col=in_range,
                min=df[:, dt.min(f[in_range])][0, 0],
                max=df[:, dt.max(f[in_range])][0, 0],
            )
        else:
            where = ""
        query = "select {cols} from {table} {where};".format(cols=self._sql_cols(keys), table=table, where=where)
        res = self.query(query, to_pandas=False, **params)
        return res

    def _get_primary_key(self, table):
        query = f"""SHOW INDEXES FROM {table}
        where key_name = 'PRIMARY'
        and column_name != 'row_end'"""
        res = self.query(query, to_pandas=False)
        if res.shape[0] > 0:
            pk = res[:, "Column_name"].to_list()[0]
        else:
            pk = None
        return pk

    def _make_diffs(self, dfa, dfb, keys=None, **params):
        if keys is None:
            keys = dfa.names
        if dfb.shape[0] > 0:
            # prepare the data
            dfa.key = keys
            dfb = dfb[:, keys]  # we only need the keys to find diffs
            dfb = dfb[:, f[:].extend({"_a_": 1})]
            dfb.key = keys
            # remove the duplicates from dfa
            diffs = dfa[:, :, join(dfb)]
            del diffs[f._a_ == 1, :]
            del diffs[:, "_a_"]
            del dfb[:, "_a_"]
        else:
            # return dfa if dfb is empty
            diffs = dfa

        return diffs

    @contextmanager
    def _cursor(self):
        cursor = self._conn.connection.cursor()
        try:
            yield cursor
        except BaseException as error:
            raise error
        finally:
            cursor.close()
