from contextlib import contextmanager

from sqlalchemy import create_engine

from dbrequests.credentials import Credentials


class Session:
    """This is a thin wrapper around connections opened by sqlalchemy. It
    handels opening and closing; preferrably as contextmanager. A connection is
    opened upon initialization.

    - credentials: (Credentials) a dict with two member:
        - url: a sqlalchemy url
        - connect_args: a dictionary with arguments passed on to create_engine.
    """

    def __init__(self, credentials: Credentials):
        self._engine = create_engine(credentials.url, **credentials.connect_args)
        self._conn = self._engine.connect()

    def __enter__(self):
        return self._conn

    def __exit__(self, exc, val, traceback):
        self.close()

    def close(self):
        """Close any open connections."""
        self._conn.close()
        self._engine.dispose()

    @contextmanager
    def transaction(self):
        """Contextmanager to handle opening and closing transactions. A
        rollback is attempted in case of an error."""
        tx = self._conn.transaction()
        try:
            yield self._conn
            tx.commit()
        except BaseException as e:
            tx.rollback()
            raise e
        finally:
            pass

    @contextmanager
    def cursor(self):
        """Contextmanager to handle opening and closing cursors."""
        cursor = self._conn.connection.cursor()
        try:
            yield cursor
        except BaseException as error:
            raise error
        finally:
            cursor.close()
