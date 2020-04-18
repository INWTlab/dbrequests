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

    def __init__(self, db_url=None, creds=None, sql_dir=None,
                 escape_percentage=False, remove_comments=False, **kwargs):
        connect_args = kwargs.pop('connect_args', {})
        # This option is needed for send data via csv: #20
        connect_args['local_infile'] = connect_args.get('local_infile', 1)
        # This option is needed for memory efficient send query: #22
        # mysqldb can be difficult to install, so we also support
        # pymysql. Depending on the driver we pick the apropriate cursorclass.
        connect_args['cursorclass'] = connect_args.get(
            'cursorclass', self._pick_cursorclass(db_url, creds))
        super().__init__(db_url=db_url, creds=creds, sql_dir=sql_dir,
                         connection_class=MysqlConnection,
                         escape_percentage=escape_percentage,
                         remove_comments=remove_comments,
                         connect_args=connect_args, **kwargs)

    def send_data(self, df, table, mode='insert', **params):
        """Sends df to table in database.

        Args:
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
                - 'update': updates duplicate primary keys
        """
        if not isinstance(df, Frame):
            df = Frame(df)
        with self.transaction() as conn:
            return conn.send_data(df, table, mode, **params)

    @staticmethod
    def _pick_cursorclass(url, creds):
        """
        Pick the SSCursor for the defined driver in url or creds.

        We can easily extract the driver from the sqlalchemy.engine. BUT: we
        want to pass the cursorclass to the create_engine function and hence
        need to extract it beforhand.
        """
        if creds:
            driver = creds.get('driver', 'pymysql')
        elif url:
            driver = re.findall(r'mysqldb|pymysql', url)[0]
        else:
            raise ValueError(
                'Please provide either a valid db_url or creds object.')
        if driver == 'mysqldb':
            from MySQLdb.cursors import SSCursor
        else:
            from pymysql.cursors import SSCursor
        return SSCursor
