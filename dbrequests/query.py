import os
import warnings
import re

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

    def __init__(self, query, sql_dir='', escape_percentage=False, remove_comments=False, **kwargs):

        self._escape_percentage = escape_percentage
        self.sql_dir = sql_dir
        self.path = None
        if isinstance(query, str) and not (' ' in query):
            if '.sql' not in query:
                query = query + '.sql'
            self.path = os.path.join(self.sql_dir, query)
            text = self._read_file(**kwargs)
        else:
            text = query
        if remove_comments:
            text = self.__remove_comments(text)
        self.text = self.__escape_percentage(text)

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

    def __escape_percentage(self, text):
        '''escapes percentage signs within sql files'''
        escaped_text = re.sub(r'(?<!%)%(?!%)', '%%', text)
        if (escaped_text != text):
            if self._escape_percentage:
                text = escaped_text
            else:
                warnings.warn('Query contains percentage sign without esacping. Please use escape_percentage=True', SyntaxWarning)
        return text

    @staticmethod
    def __remove_comments(code):
        '''remove multi- and singleline comments from the sql query'''
        out = (' ').join(re.split(r'/\*|\*/', code)[0::2])  # multiline comments
        out = '\n'.join([st.split('--')[0] for st in out.split('\n')])  # single line comments
        return out
