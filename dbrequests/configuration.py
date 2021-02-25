import json

from sqlalchemy.engine.url import URL, make_url


class Configuration(object):
    """
    A configuration stores all information needed to connect and query a database.

    Class Methods:
        from_url: Construct a configuration from sqlalchemy url.
        from_json_file: Construct a configuration from json file.

    """

    def __init__(  # noqa: WPS211
        self,
        dialect: str,
        driver: str,
        username: str,
        password: str,
        host: str,
        port: int,
        database: str,
        chunksize: int = 1000000,
        sql_dir: str = "./sql",
        sql_remove_comments: bool = True,
        sql_escape_percentage: bool = True,
        connect_args: dict = None,
    ):
        """
        Initialize a new configuration.

        Args:
            dialect (str): The sql dialect, e.g. mysql.
            driver (str): The sql driver, e.g. pymysql.
            username (str): The username used when connecting.
            password (str): The password used when connecting.
            host (str): The host to connect to.
            port (int): The port to connect to.
            database (str): The database / schema used by default in queries.
            chunksize (int): The number of rows to collect when fetching a result set.
            sql_dir (str): The folder in which to look for sql files.
            sql_remove_comments (bool): Whether to remove comments when parsing queries.
            sql_escape_percentage (bool): Whether to escape percentage signs when parsing queries.
            connect_args (dict): Additional argument passed to sqlalchemy.create_engine.

        Server credentials are used to construct a sqlalchemy URL.

        """
        self.url: URL = URL(
            f"{dialect}+{driver}",
            username,
            password,
            host,
            port,
            database,
        )
        self.chunksize = chunksize
        self.query_args: dict = {
            "sql_dir": sql_dir,
            "remove_comments": sql_remove_comments,
            "escape_percentage": sql_escape_percentage,
        }
        self.connect_args = connect_args

    @classmethod
    def from_url(cls, url: str, **kwargs):
        """
        Construct a new configuration from a url.

        Args:
            url (str): A sqlalchemy url.
            kwargs: Arguments passed to configuration.

        Returns:
            A configuration.

        """
        surl = make_url(url)
        return cls(
            dialect=surl.get_backend_name(),
            driver=surl.get_driver_name(),
            **surl.translate_connect_args(),
            **kwargs,
        )

    @classmethod
    def from_json_file(cls, file_name: str, element: str = None):
        """
        Construct a new configuration from a json file.

        Args:
            file_name (str): A file name.
            element (str): The element in the json file to be used as configuration.

        Returns:
            A configuration.

        """
        with open(file_name) as fname:
            config = json.load(fname)
        if element:
            config = config[element]
        return cls(**config)
