from datatable import Frame, rbind

from dbrequests.query import Query


def send_query(
    session,
    query: str,
    **query_args,
) -> Frame:
    """
    Send a query to a database and collect result set.

    Args:
        session (Session): Session object.
        query (str): String can be a filename or a query.
        query_args: Arguments passed to query.

    Returns:
        A frame with the result set. In case the sql statement comprises more
        than one result set, all are collected but only the last one is
        returned.

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