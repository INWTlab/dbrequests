from datatable import Frame, rbind

from dbrequests.configuration import Configuration
from dbrequests.query import Query
from dbrequests.session import Session


def send_query(
    configuration: Configuration,
    query: str,
    **query_args,
) -> Frame:
    """Send a query to a database and collect results."""
    with Session(configuration) as session:
        res = _send_query_in_session(session, query, **query_args)
    return res


def _send_query_in_session(
    session: Session,
    query: str,
    **query_args,
) -> Frame:
    query_args = dict(session.configuration.query_args.copy(), **query_args)
    queries = Query(query, **query_args).split()
    for single_query in queries:
        res = _send_single_query(session, single_query, session.configuration.chunksize)
    return res


def _send_single_query(session: Session, query: str, chunksize: int) -> Frame:
    res = []
    for frame in _collect_query_results(session, query, chunksize):
        res.append(frame)
    return rbind(res)


def _collect_query_results(
    session: Session,
    query: str,
    chunksize: int,
) -> Frame:
    with session.cursor() as cursor:
        cursor.execute(query)
        if cursor.description is None:
            # In case of create, update, insert, set statements we return an
            # empty frame:
            yield Frame()
        else:
            fields = [field[0] for field in cursor.description]
            while True:
                frame = Frame(cursor.fetchmany(chunksize))
                if frame.shape == (0, 0):
                    yield Frame({field: [] for field in fields})
                    break
                else:
                    frame.names = fields
                    yield frame
