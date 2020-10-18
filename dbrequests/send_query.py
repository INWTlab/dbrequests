from datatable import Frame, rbind

from dbrequests.configuration import Configuration
from dbrequests.query import Query
from dbrequests.session import Session


def send_query(
    configuration: Configuration,
    query: str,
    **query_args,
) -> Frame:
    query_args = dict(configuration.query_args.copy(), **query_args)
    queries = Query(query, **query_args).split()
    with Session(configuration) as session:
        for one_query in queries:
            res = send_query_in_session(session, one_query, configuration.chunksize)
    return res


def send_query_in_session(
    session: Session,
    query: str,
    chunksize: int = 100000,
) -> Frame:
    res = []
    for frame in collect_query_results(session, query, chunksize):
        res.append(frame)
    return rbind(res)


def collect_query_results(session: Session, query: str, chunksize: int) -> Frame:
    with session.cursor() as cursor:
        cursor.execute(query)
        fields = [field[0] for field in cursor.description]
        while True:
            frame = Frame(cursor.fetchmany(chunksize))
            if frame.shape == (0, 0):
                yield Frame({field: [] for field in fields})
                break
            else:
                frame.names = fields
                yield frame
