from contextlib import contextmanager

from sqlalchemy import create_engine

from dbrequests.configuration import Configuration
from dbrequests.send_query import send_query as sendquery


class Session(object):
    """A session represents a connection to a database."""

    def __init__(self, configuration: Configuration):
        """
        Create a session object to handle open connections.

        Args:
            configuration (Configuration): see docs of Configuration class.

        This is a thin wrapper around connections opened by sqlalchemy. It
        handels opening and closing of connections; preferrably as
        contextmanager. A connection is opened upon initialization.
        """
        self._engine = create_engine(
            configuration.url,
            connect_args=configuration.connect_args,
        )
        self.connection = self._engine.connect()
        self.configuration = configuration

    def __enter__(self):  # noqa: D105
        return self

    def __exit__(self, exc, val, traceback):  # noqa: WPS110,D105
        self.close()

    def close(self):
        """Close any open connections."""
        self.connection.close()
        self._engine.dispose()

    @contextmanager
    def transaction(self):
        """
        Contextmanager to handle starting and commiting transactions.

        Raises:
            Exception: In case of any error, rollback is attempted and exception
                is raised.

        Yields:
            A session with started transaction.
        """
        tx = self.connection.transaction()
        try:
            yield self
        except Exception as error:
            tx.rollback()
            raise error
        tx.commit()

    @contextmanager
    def execute(self, query: str):
        """
        Contextmanager to execute a sql statement and close the result.

        Args:
            query (str): a sql query as string.

        Yields:
            A result object closed by the contexmanager.
        """
        dbresult = self.connection.execute(query)
        try:
            yield dbresult
        finally:
            dbresult.close()

    # For convenience we offer to call send_query as a member of this class.
    query_query = sendquery
