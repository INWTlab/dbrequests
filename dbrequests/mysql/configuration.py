from copy import deepcopy

from dbrequests.configuration import Configuration


class MySQLConfiguration(Configuration):

    def __init__(self, config: dict):

        config = deepcopy(config)

        config["dialect"] = "mysql"
        config["driver"] = config.get("driver", "mysqldb")
        config["port"] = config.get("port", 3306)  # noqa: WPS432 (magic number)
        config["connect_args"] = config.get("connect_args", {})  # noqa: WPS204
        config["connect_args"]["cursorclass"] = config["connect_args"].get(
            "cursorclass",
            _pick_cursorclass(config.get("driver")),
        )
        config["connect_args"]["local_infile"] = config["connect_args"].get(
            "local_infile",
            1,
        )

        super().__init__(config)


def _pick_cursorclass(driver):
    """Pick the SSCursor for the defined driver in url."""
    if driver == "mysqldb":
        from MySQLdb.cursors import SSCursor  # noqa: WPS433,WPS440
    elif driver == "pymysql":
        from pymysql.cursors import SSCursor  # noqa: WPS433,WPS440
    else:
        raise ValueError(
            (
                "The only supported driver are mysqldb and pymysql.",
                "Provide a 'cursorclass' as connect_arg explicitly.",
            ),
        )
    return SSCursor
