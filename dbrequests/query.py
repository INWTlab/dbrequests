import os


class Query(object):
    """A Query. Encapsulates SQL code given directly or via a SQL file in a specified directory.

        Args:
        - query (str): query may be:
            - a sql query as string
            - a file-path as string
            - the name of a file as string (with or without .sql)
            - a sqlalchemy selectable
        - sql_dir (str): the path to a directory containing sql

    """

    def __init__(self, query, sql_dir='', **kwargs):

        self.sql_dir = sql_dir
        self.path = None
        if isinstance(query, str) and not (' ' in query):
            if not '.sql' in query:
                query = query + '.sql'
            self.path = os.path.join(self.sql_dir, query)
            self.text = self._read_file(**kwargs)
        else:
            self.text = query

    def __enter__(self):
        return self

    def __repr__(self):
        return '<Query: {}'.format(self.text)

    def _read_file(self, **params):
        # If path doesn't exists
        if not os.path.exists(self.path):
            raise IOError("File '{}' not found!".format(self.path))

        # If it's a directory
        if os.path.isdir(self.path):
            raise IOError("'{}' is a directory!".format(self.path))

        # Read the given .sql file into memory.
        with open(self.path) as f:
            text = f.read()

        return text.format(**params)
