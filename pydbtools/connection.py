from sqlalchemy import text
from pandas import read_sql
import os
import inspect

class Connection(object):
    """A Database connection."""

    def __init__(self, connection):
        self._conn = connection
        self.open = not connection.closed

    def close(self):
        self._conn.close()
        self.open = False

    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()

    def __repr__(self):
        return '<Connection open={}>'.format(self.open)

    def query(self, query, **params):
        """Executes the given SQL query against the connected Database.
        Parameters can, optionally, be provided. Uses pandas.read_sql and returns a Pandas Dataframe
        """

        # Execute the given query.
        params = {k: v for k, v in params.items() if k in inspect.getfullargspec(read_sql).args}
        results = read_sql(query, self._conn, **params)

        return results

    def bulk_query(self, query, *multiparams):
        """Bulk insert or update."""

        self._conn.execute(text(query), *multiparams)

    def query_file(self, path, **params):
        """Like Connection.query, but takes a filename to load a query from."""

        # If path doesn't exists
        if not os.path.exists(path):
            raise IOError("File '{}' not found!".format(path))

        # If it's a directory
        if os.path.isdir(path):
            raise IOError("'{}' is a directory!".format(path))

        # Read the given .sql file into memory.
        with open(path) as f:
            query = f.read()

        query = query.format(**params)
        # Defer processing to self.query method.
        return self.query(query=query, **params)

    def bulk_query_file(self, path, *multiparams):
        """Like Connection.bulk_query, but takes a filename to load a query
        from.
        """

         # If path doesn't exists
        if not os.path.exists(path):
            raise IOError("File '{}'' not found!".format(path))

        # If it's a directory
        if os.path.isdir(path):
            raise IOError("'{}' is a directory!".format(path))

        # Read the given .sql file into memory.
        with open(path) as f:
            query = f.read()

        self._conn.execute(text(query), *multiparams)

    def transaction(self):
        """Returns a transaction object. Call ``commit`` or ``rollback``
        on the returned object as appropriate."""

        return self._conn.begin()
