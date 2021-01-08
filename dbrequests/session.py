"""A session handles opened connections."""

from contextlib import contextmanager

from sqlalchemy import create_engine

from dbrequests.configuration import Configuration


class Session(object):
    """This is a thin wrapper around connections opened by sqlalchemy. It
    handels opening and closing; preferrably as contextmanager. A connection is
    opened upon initialization.

    - configuration: (Configuration) a dict with two member:
        - url: a sqlalchemy url
        - connect_args: a dictionary with arguments passed on to create_engine.
    """

    def __init__(self, configuration: Configuration):
        self._engine = create_engine(
            configuration.url,
            connect_args=configuration.connect_args,
        )
        self.connection = self._engine.connect()
        self.configuration = configuration

    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()

    def close(self):
        """Close any open connections."""
        self.connection.close()
        self._engine.dispose()

    @contextmanager
    def transaction(self):
        """Contextmanager to handle opening and closing transactions. A
        rollback is attempted in case of an error."""
        tx = self.connection.transaction()
        try:
            yield self
        except Exception as error:
            tx.rollback()
            raise error
        tx.commit()

    @contextmanager
    def execute(self, query: str):
        """Contextmanager to handle execute a query and close the result."""
        dbresult = self.connection.execute(query)
        try:
            yield dbresult
        finally:
            dbresult.close()
