import re

from datatable import Frame

from dbrequests.database import Database as SuperDatabase

from .connection import Connection as MysqlConnection


class Database(SuperDatabase):
    """
    This class is derived from `dbrequests.Database`.

    It uses the dbrequests.mysql.Connection which implements a different
    strategy for writing and reating data.

    **Sending data**
    For sending data we utilize the LOAD DATA LOCAL INFILE command from MySQL.
    For large datasets this is the most efficient approach to get data into
    your database. For writing the data to CSV and to disk we use the
    f(ast)write method from the datatable package.

    **Reading data**
    For reading data, we (1) use server side cursors and (2) use datatables
    Frame to optimize the memory consumption. For raw speed you may want to use
    the mysqldb driver, which can be 10x faster than the pymysql driver.
    """

    _connection_class = MysqlConnection

    def _init_engine(self, **kwargs):
        connect_args = kwargs.pop('connect_args', {})
        # This option is needed for send data via csv: #20
        connect_args['local_infile'] = connect_args.get('local_infile', 1)
        # This option is needed for memory efficient send query: #22
        # mysqldb can be difficult to install, so we also support
        # pymysql. Depending on the driver we pick the apropriate cursorclass.
        connect_args['cursorclass'] = connect_args.get(
            'cursorclass', self._pick_cursorclass(self.db_url))
        super()._init_engine(connect_args=connect_args, **kwargs)

    def send_data(self, df, table, mode='insert', **params):
        """Sends df to table in database.

        - df (DataFrame): internally we use datatable Frame. Any object
        that can be converted to a Frame may be supplied.
        - table_name (str): Name of the table.
        - mode ({'insert', 'truncate', 'replace',
        'update'}): Mode of Data Insertion. Defaults to 'insert'.
            - 'insert': appends data. Duplicates in the
            primary keys are not replaced.
            - 'truncate': drop the table, recreate it, then insert. No
            rollback on error.
            - 'delete': delete all rows in the table, then insert. This
            operation can be rolled back on error, but can be very
            expensive.
            - 'replace': replaces (delete, then insert) duplicate primary
            keys.
            - 'update': insert but with update on duplicate primary keys
            - 'mode_diffs': sync|insert|update|replace_diffs. Instead of
              sending the complete dataset, first identify the changes and then
              only send the changes. This works most effectively if you only
              expect few changes in your data. 'sync' will not only update new
              rows, but will also delete rows; this has the same effect as a
              truncate.
              - keys (str|list[str]|None): defaults to None. Columns to
                identify unique values and find differences. None is the
                default and uses all columns.
              - in_range (str|None): optionally provide a name of a
                numeric column, e.g. an id. We derive min and max and reduce
                the amount of data we have to pull down to construct diffs.
              - chunksize (int): defaults to 10 million. We pull data in chunks
                and remove duplicates from the dataset.
        """
        if not isinstance(df, Frame):
            df = Frame(df)
        with self.transaction() as conn:
            return conn.send_data(df, table, mode, **params)

    def send_delete(self, df, table: str, mode: str = 'in_set', **params) -> int:
        """
        Delete entries in a table. Use this method instead of send_bulk_query
        for deleting rows in a table; e.g. instead of looping over a long
        vector and deleting chunkwise.

        - df (DataFrame): a data frame containing the information which rows to
          delete. See mode for details.
        - table (str): the table where rows are to be deleted
        - mode (str):
          - 'in_join': delete entries with a match in df. If it is possible to
            do a left join with df we have a match.
          - 'not_in_join': delete entries with **no** match in df.
          - 'in_set': delete entries which are in the set defined by df. We do
            a 'where in col from df' and concatenate columns with 'and'. This
            can bring a considerable speedup, compared to the join strategy, if
            you only delete values from one enum field.
          - 'not_in_set': delete entries which are not in the set defined by
            df.
          - 'in_delete_col': updates the 'delete column in the target table
            and then sends a delete statement. This can have more pretictable
            performance compared to 'in_join'.
        """
        if not isinstance(df, Frame):
            df = Frame(df)
        with self.transaction() as conn:
            return conn.send_delete(df, table, mode, **params)

    @staticmethod
    def _pick_cursorclass(url):
        """
        Pick the SSCursor for the defined driver in url.

        We can easily extract the driver from the sqlalchemy.engine. BUT: we
        want to pass the cursorclass to the create_engine function and hence
        need to extract it beforhand.
        """
        driver = re.findall(r'mysqldb|pymysql', url)[0]
        if driver == 'mysqldb':
            from MySQLdb.cursors import SSCursor
        else:
            from pymysql.cursors import SSCursor
        return SSCursor
