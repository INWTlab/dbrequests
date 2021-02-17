import logging

from datatable import Frame, f

from dbrequests.mysql import send_data
from dbrequests.mysql.statements import delete_in_delete_col
from dbrequests.send_query import send_query
from dbrequests.session import Session


def delete_col(session: Session, df: Frame, table):
    logging.info("deleting rows marked by delete column: {rows} rows", rows=df.shape[0])
    if df.shape[0] > 0:
        df = df[:, f[:].extend({"delete": 1})]
        send_data.replace(session, df, table)
        send_query(session, delete_in_delete_col(table))
