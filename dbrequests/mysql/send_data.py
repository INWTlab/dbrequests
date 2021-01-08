import logging

from datatable import Frame

from dbrequests.mysql.statements import infile as infile_statement
from dbrequests.mysql.statements import insert_update as insert_update_statement
from dbrequests.mysql.temporary_table import temporary_table
from dbrequests.send_query import send_query
from dbrequests.session import Session
from dbrequests.temp_file import temp_file
from dbrequests.write_csv import write_csv


def truncate(session: Session, df: Frame, table: str):
    """First truncate a table, then insert new data."""
    logging.info("sending data with truncate: {n_rows} rows", n_rows=df.shape[0])
    send_query(session, "truncate table {table};".format(table=table))
    insert(session, df, table, with_replace=True)


def insert(session: Session, df: Frame, table: str, with_replace: bool = False):
    """Insert data into a table using 'load data local infile'."""
    logging.info("sending data with insert: {n_rows} rows", n_rows=df.shape[0])
    with temp_file() as file_name:
        write_csv(df, file_name)
        query = infile_statement(file_name, df.names, table, with_replace)
        send_query(session, query)


def replace(session: Session, df: Frame, table: str):
    """
    Insert data into a table and replace duplicate entries.

    This is a convenience wrapper around insert with replace=True.
    """
    insert(session, df, table, with_replace=True)


def update(session: Session, df: Frame, table: str, with_temp: bool = True):
    logging.info("sending data with update: {n_rows} rows", n_rows=df.shape[0])
    with temporary_table(session, table, df.names, with_temp) as tmp_table:
        insert(session, df, tmp_table)
        query = insert_update_statement(df.names, table, tmp_table)
        send_query(session, query)
