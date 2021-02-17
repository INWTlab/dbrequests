import logging
from typing import List

from datatable import Frame

from dbrequests.make_diff import make_diff
from dbrequests.mysql import send_data, send_delete
from dbrequests.mysql.statements import select_cols, show_primary
from dbrequests.send_query import send_query
from dbrequests.session import Session


def update(
    session: Session,
    df: Frame,
    table: str,
    with_temp: bool = True,
):
    logging.info("sending data with send_diff.update: {rows} rows", rows=df.shape[0])
    remote_table = _get_remote_table(session, table, df.names)
    diffs = make_diff(df, remote_table, df.names)
    send_data.update(session, diffs, table, with_temp)


def insert(session: Session, df: Frame, table: str):
    logging.info("sending data with send_diff.insert: {rows} rows", rows=df.shape[0])
    remote_table = _get_remote_table(session, table, df.names)
    diffs = make_diff(df, remote_table, df.names)
    send_data.insert(session, diffs, table)


def replace(session: Session, df: Frame, table: str):
    logging.info("sending data with send_diff.replace: {rows} rows", rows=df.shape[0])
    remote_table = _get_remote_table(session, table, df.names)
    diffs = make_diff(df, remote_table, df.names)
    send_data.replace(session, diffs, table)


def sync(session: Session, df: Frame, table: str):
    logging.info("sending data with send_diff.replace: {rows} rows", rows=df.shape[0])
    primary_key = _get_primary_key(session, table)
    remote_table = _get_remote_table(session, table, df.names)

    diffa = make_diff(df, remote_table, df.names)
    diffb = make_diff(remote_table, df, primary_key)

    send_delete.delete_col(session, diffb, table)
    send_data.replace(session, diffa, table)


def _get_remote_table(session: Session, table: str, cols: List[str]):
    return send_query(session, select_cols(table, cols))


def _get_primary_key(session: Session, table: str):
    res = send_query(session, show_primary(table))
    if res.shape[0] > 0:
        pk = res[:, "Column_name"].to_list()[0]
    else:
        pk = None
    return pk
