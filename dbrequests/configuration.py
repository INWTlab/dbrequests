import json
import logging
from copy import deepcopy

from sqlalchemy.engine.url import URL, make_url

from dbrequests.generic import generic


class Configuration(object):
    """Deconstruct and prepare a configuration."""

    def __init__(self, config: dict):

        config = default_configuration(
            config.get("dialect", "mysql"),
            deepcopy(config),
        )

        # credentials:
        self.url: URL = URL(
            "{}+{}".format(config.pop("dialect"), config.pop("driver")),
            config.pop("username", config.pop("username", "root")),
            config.pop("password", "root"),
            config.pop("host", "127.0.0.1"),
            config.pop("port"),
            config.pop("db", config.pop("database", "test")),
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


def config_from_url(url: str) -> Configuration:
    config = Configuration({"dialect": "unknown"})
    config.url = make_url(url)
    return config


def config_from_json_file(file_name: str) -> Configuration:
    with open(file_name) as fname:
        config = json.load(fname)
    return Configuration(config)


def get_dialect(*args, **kw):
    return kw.get("dialect", args[0])


@generic(get_dialect)
def default_configuration(dialect: str, config: dict) -> dict:
    """This is a generic function, dispatching on the first argument as key.
    You may extend default arguments for a specific dialect by registering a
    concrete method."""
    logging.debug(
        "Entering default_configuration for unknown dialect.",
        dialect=dialect,
    )
    return config
