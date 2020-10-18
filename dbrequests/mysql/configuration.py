import logging

from dbrequests.configuration import default_configuration


@default_configuration.register("mysql")
def mysql_configuration(dialect: str, config: dict) -> dict:
    logging.debug(
        "Entering add_default_configuration for dialect: '{dialect}'",
        dialect=dialect,
    )

    config["dialect"] = "mysql"
    config["driver"] = config.get("driver", "mysqldb")
    config["port"] = 3306
    config["connect_args"] = config.get("connect_args", {})  # noqa: WPS204
    config["connect_args"]["cursorclass"] = config["connect_args"].get(
        "cursorclass",
        pick_cursorclass(config.get("driver")),
    )
    config["connect_args"]["local_infile"] = config["connect_args"].get(
        "local_infile",
        1,
    )
    return config


def pick_cursorclass(driver):
    """
    Pick the SSCursor for the defined driver in url.
    """
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
