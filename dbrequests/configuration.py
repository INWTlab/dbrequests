import json
from copy import deepcopy

from sqlalchemy.engine.url import URL, make_url


class Configuration(object):
    """Deconstruct and prepare a configuration."""

    def __init__(self, config: dict):

        config = deepcopy(config)

        # credentials:
        self.url: URL = URL(
            "{}+{}".format(config.pop("dialect"), config.pop("driver")),
            config.pop("username", config.pop("user", "root")),
            config.pop("password", "root"),
            config.pop("host", "127.0.0.1"),
            config.pop("port"),
            config.pop("database", config.pop("db", "test")),
        )

        # misc parameters:
        self.mode: str = config.pop("mode", self.url.get_dialect())
        self.chunksize: int = config.pop("chunksize", 100000)  # noqa: WPS432
        # query arguments
        self.query_args: dict = config.pop("query_args", {})
        self.query_args["sql_dir"] = self.query_args.get("sql_dir", "./sql")
        self.query_args["remove_comments"] = self.query_args.get(
            "remove_comments",
            True,
        )
        self.query_args["escape_percentage"] = self.query_args.get(
            "escape_percentage",
            False,
        )

        # connect_args:
        self.connect_args: dict = config.pop("connect_args", {})

        # Prevent any configuration items ending up in the void:
        if config:
            raise ValueError(
                (
                    "Can't handle the following configuration items: {}".format(
                        ", ".join([name for name, _ in config.items()])
                    )
                )
            )

    @classmethod
    def from_url(cls, url: str, config=None):
        """Construct a new configuration from a url."""
        if config is None:
            config = {}
        else:
            config = deepcopy(config)
        config["dialect"] = "mysql"
        config["driver"] = "pymysql"
        config["port"] = 3306
        new_instance = cls({"dialect": "unknown", "port": 1})
        new_instance.url = make_url(url)
        return new_instance

    @classmethod
    def from_json_file(cls, file_name: str):
        with open(file_name) as fname:
            config = json.load(fname)
        return cls(config)
