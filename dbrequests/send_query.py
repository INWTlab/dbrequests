from datatable import Frame, rbind

from dbrequests.query import Query
from dbrequests.session import Session


def send_query(
    session: Session,
    query: str,
    **query_args,
) -> Frame:
    """
    Send a query to a database and collect result set.

    :param session: Session object
    :param query: String can be a filename or a query
        - a sql query as string
        - a file-path as string
        - the name of a file as string (with or without .sql)
    """
    query_args = dict(session.configuration.query_args.copy(), **query_args)
    queries = Query(query, **query_args).split()
    res = Frame()
    for single_query in queries:
        res = _send_single_query(session, single_query, session.configuration.chunksize)
    return res


def _send_single_query(session, query: str, chunksize: int) -> Frame:
    res = []
    for frame in _collect_query_results(session, query, chunksize):
        res.append(frame)
    return rbind(res)


def _collect_query_results(
    session,
    query: str,
    chunksize: int,
) -> Frame:
    with session.execute(query) as dbresult:
        if dbresult.returns_rows:
            fields = [field[0] for field in dbresult.cursor.description]
            while True:
                frame = Frame(dbresult.cursor.fetchmany(chunksize))
                if frame.shape == (0, 0):
                    yield Frame({field: [] for field in fields})
                    break
                else:
                    frame.names = fields
                    yield frame
        else:
            # In case of create, update, insert, set statements we return an
            # empty frame:
            yield Frame()
