"""
Implements the backend for MySQL databases. This is mysql and mariadb
compliant.
"""
import os
from contextlib import contextmanager
from inspect import getfullargspec as getargs
from pathlib import Path
from tempfile import NamedTemporaryFile as TmpFile

from datatable import dt, f, str64, Frame, rbind, join
from filediffs.filediffs_python.filediffs import file_diffs

from dbrequests import Connection as SuperConnection


class Connection(SuperConnection):
    """A Database connection."""

    def send_delete(self, df: Frame, table: str, mode: str, **params) -> int:
        """See mysql.Database.send_delete for documentation."""
        mode_implementation = '_send_delete_{}'.format(mode)
        if hasattr(self, mode_implementation):
            affected_rows = getattr(
                self, mode_implementation)(df, table, **params)
        else:
            raise ValueError('{} is not a known mode'.format(mode))
        return affected_rows

    def _send_delete_in_set(self, df, table, **params):
        with self._temporary_table(
                table, df.names, params.pop('with_temp', True)) as tmp_table:
            self._send_data_insert(df, tmp_table)
            return self._delete_set(table, tmp_table, df.names, False)

    def _send_delete_not_in_set(self, df, table, **params):
        with self._temporary_table(
                table, df.names, params.pop('with_temp', True)) as tmp_table:
            self._send_data_insert(df, tmp_table)
            return self._delete_set(table, tmp_table, df.names, True)

    def _delete_set(self, table, tmp_table, cols, not_in=True):
        if not_in:
            not_in = 'not'
        else:
            not_in = ''
        where_stmt = ' and '.join([
            '`{col}` {not_in} in (select distinct `{col}` from `{tmp_table}`)'.format(
                col=col, not_in=not_in, tmp_table=tmp_table)
            for col in cols])
        delete_stmt = 'delete from `{table}` where {where_stmt};'.format(
            table=table,
            where_stmt=where_stmt
        )
        return self.bulk_query(delete_stmt)

    def _send_delete_in_join(self, df, table, **params):
        with self._temporary_table(
                table, df.names, params.pop('with_temp', True)) as tmp_table:
            self._send_data_insert(df, tmp_table)
            return self._delete_join(table, tmp_table, df.names)

    def _send_delete_not_in_join(self, df, table, **params):
        with self._temporary_table(
                table, df.names, params.pop('with_temp', True)) as tmp_table:
            self._send_data_insert(df, tmp_table)
            return self._delete_join(table, tmp_table, df.names, False)

    def _send_delete_in_delete_col(self, df, table, **params):
        df = df[:, f[:].extend({'delete': 1})]
        print(f"_send_delete_in_delete_col: '_send_data_update(df, {table}, **params)' with params {params}")
        print("The sent df has shape: {}".format(df.shape))
        self._send_data_update(df, table, **params)
        delete_query = 'delete from `{}` where `delete` = 1;'.format(table)
        print(f"_send_delete_in_delete_col: bulk_query({delete_query})")
        self.bulk_query('delete from `{}` where `delete` = 1;'.format(table))

    def _delete_join(self, table, tmp_table, df_names, not_null=True):
        if not_null:
            not_null = 'not'
        else:
            not_null = ''
        delete_query = '''
            delete `{table}` from
                `{table}` left join `{tmp_table}` as tt using({tmp_cols})
            where `tt`.{tmp_first_col} is {not_null} NULL;
            '''.format(
            table=table,
            tmp_table=tmp_table,
            tmp_cols=self._sql_cols(df_names),
            tmp_first_col=self._sql_cols([df_names[0]]),
            not_null=not_null
        )
        return self.bulk_query(delete_query)

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
        with_temp = params.pop('with_temp', True)
        with self._temporary_table(table, df.names, with_temp) as tmp_table:
            self._send_data_insert(df, tmp_table)
            self._insert_update(df, table, tmp_table)

    def _send_data_update_diffs(self, df, table, **params):
        diffs, dump = self._make_diffs(df, table, **params)
        self._send_data_update(diffs, table, **params)

    def _send_data_insert_diffs(self, df, table, **params):
        diffs, dump = self._make_diffs(df, table, **params)
        self._send_data_insert(diffs, table)

    def _send_data_replace_diffs(self, df, table, **params):
        diffs, dump = self._make_diffs(df, table, **params)
        self._send_data_replace(diffs, table)

    def _send_data_sync_diffs(self, df, table, **params):
        diffs, deletes = self._make_diffs(
            df, table, both_directions=True, **params)
        self._send_delete_in_delete_col(deletes, table, **params)
        self._send_data_insert(diffs, table)

    def _send_data_sync_filediffs(self, df, table, **params):
        print("Start function make_diffs_filediffs.")
        diffs, deletes = self._make_diffs_filediffs(df, table, **params)
        print("Execute '_send_delete_in_delete_col':")
        self._send_delete_in_delete_col(deletes, table, **params)
        print("Execute '_send_data_insert':")
        self._send_data_insert(diffs, table)

    def _write_csv(self, df, file, append=False):
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
        df.replace(None, 'NULL')
        df.to_csv(path=file.name, header=False, append=append)

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
        save_to_file = params.pop('save_to_file', False)
        verbose = params.pop('verbose', False)
        if not save_to_file:
            filepath = None
        else:
            filepath = "dbtable.csv"
            if filepath in os.listdir():
                if verbose:
                    print(f"Remove file {filepath} to have a clean file to save the db table data.")
                os.remove(filepath)

        with self._cursor() as cursor:
            params = {k: v for k, v in params.items()
                      if k in getargs(cursor.execute).args}
            if verbose:
                print("Run 'cursor.execute(query, **params)'")
            cursor.execute(query, **params)
            fields = [i[0] for i in cursor.description]
            res = []
            if verbose:
                print("Starting the chunkwise file reading process from the databse."
                      " Use 'chunksize' argument to change the chunk size.")
                print(f"Currently working with chunksize: {chunksize}")
            chunk_count = 0
            notify_n = 1000000
            while True:
                result = cursor.fetchmany(chunksize)
                if not result:
                    if verbose:
                        print(
                            f"Finished getting data from the database chunkwise. Saved to Ram: {not save_to_file}. Saved to File: {save_to_file}")
                    break
                if save_to_file:
                    # open file connection and append chunk to file
                    results_frame = Frame(result)
                    results_frame.names = fields
                    with open(filepath, "a+") as fcon:
                        self._write_csv(results_frame, fcon, append=True)

                else:
                    res.append(Frame(result))

                # log processed chunk every notify_n rows processed
                if verbose:
                    chunk_count = chunk_count + 1
                    lines_processed = chunk_count * chunksize
                    if lines_processed % notify_n == 0:
                        print(f"Downloaded {lines_processed} lines from the database")
        if save_to_file:
            frame = None
            return filepath  # frame,

        frame = rbind(res, bynames=False)
        if frame.shape == (0, 0):
            frame = Frame({n: [] for n in fields})
        else:
            frame.names = fields
        if to_pandas:
            frame = frame.to_pandas()
        return frame  # , filepath

    def _make_diffs_filediffs(self, df, table, **params):
        keys = df.names
        if "verbose" in params:
            verbose = params["verbose"]
        else:
            verbose = False

        # save df and free ram
        if verbose:
            print("Save df to file for use in filediffs")
        input_df_path = "input_table.csv"
        # use function write_csv für datatable
        # todo: use TmpFile
        # with TmpFile(mode='w', newline='') as tf:
        with open(input_df_path, "wb+") as fcon:
            self._write_csv(df, fcon)
        if verbose:
            print("delete the df from ram reference counter")
        # check obs hilft. weil datatable wird es gelöscht? bei pandas?
        df = 1
        del df

        # Executing the strategy:
        query = 'select {cols} from {table};'.format(cols=self._sql_cols(keys), table=table)

        # save directly to file and return filepath
        # find conecept to deal with temporary file management. Maybe just manually delete it
        if verbose:
            print(f"Get the database table and save it to disk for use in filediffs\nUsing the query: '{query}'")
        # save db table to file
        db_df_path = self.query(query, to_pandas=False, save_to_file=True, **params)

        # split input df into lines only in input df and lines only in db and save to file
        if verbose:
            print("Create filediffs and save them to disk.")
        file_diffs(input_df_path, db_df_path, verbose=verbose)
        diffa_path = 'lines_present_only_in_file1.txt'
        diffb_path = 'lines_present_only_in_file2.txt'

        diffa = dt.fread(diffa_path)
        diffb = dt.fread(diffb_path)
        # renaming necessary because filediffs is stupid and dropping the header line because its present in both files
        if diffa.shape == (0, 0):
            diffa = dt.Frame([[] for col in keys])
            diffa.names = keys
        else:
            diffa.names = keys

        if diffb.shape == (0, 0):
            diffb = dt.Frame([[] for col in keys])
            diffb.names = keys
        else:
            diffb.names = keys

        # cleanup files # todo: use context manager
        if verbose:
            print("Cleanup local temporary files")
        tmp_files = [diffa_path, diffb_path, input_df_path, db_df_path, "lines_present_in_both_files.txt"]
        for file in tmp_files:
            Path(file).unlink(missing_ok=True)

        return diffa, diffb

    def _make_diffs(self, df, table, keys=None, in_range=None,
                    chunksize=10000000, both_directions=False, **params):
        # Here is the strategy to construct diffs:
        # - pull down the complete target table
        # - to spare memory we do this chunkwise
        # - for each chunk, do a left join on the column set 'keys'
        # - drop the matched lines
        # - repeat for remote data if both_directions is True
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
        res = []
        with self._cursor() as cursor:
            cursor.execute('select {cols} from {table} {where};'.format(
                cols=self._sql_cols(keys), table=table, where=where))
            while True:
                result = cursor.fetchmany(chunksize)
                if len(result) == 0:
                    break
                # prepare the data
                result = Frame(result)
                result.names = keys
                result = result[:, f[:].extend({'__a__': 1})]
                result.key = keys
                # remove the duplicates from df
                diffa = df[:, :, join(result)]
                del diffa[f.__a__ == 1, :]
                del diffa[:, '__a__']
                if both_directions:
                    del result[:, '__a__']
                    # remove the duplicates from result
                    df = df[:, f[:].extend({'__b__': 1})]
                    df.key = keys
                    diffb = result[:, :, join(df)]
                    del diffb[f.__b__ == 1, :]
                    diffb = diffb[:, result.names]
                    # store for next iteration
                    res.append(diffb)
                df = diffa  # this HAS to happen, but after diffb
        if both_directions:
            res = rbind(res, bynames=False)
            if res.shape == (0, 0):
                res = Frame({n: [] for n in keys})
        else:
            res = None
        return df, res

    @contextmanager
    def _cursor(self):
        cursor = self._conn.connection.cursor()
        try:
            yield cursor
        except BaseException as error:
            raise error
        finally:
            cursor.close()
