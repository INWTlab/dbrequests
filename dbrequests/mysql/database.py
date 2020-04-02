from datatable import Frame

from dbrequests.database import Database as SuperDatabase
from .connection import Connection as MysqlConnection


class Database(SuperDatabase):
    """This class is derived from `dbrequests.Database`.

    It uses the dbrequests.mysql.Connection which implements a different
    strategy for writing data into databases (load data local infile).

    The url to the database can be provided directly or via a credentials-
    dictionary `creds` with keys: - host - db - user - password - dialect
    (defaults to mysql) - driver (defaults to pymysql)
    """

    def __init__(self, db_url=None, creds=None, sql_dir=None,
                 escape_percentage=False, remove_comments=False, **kwargs):
        connect_args = kwargs.pop('connect_args', {})
        connect_args["local_infile"] = 1
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
