from dbrequests.configuration import Configuration


class MySQLConfiguration(Configuration):  # noqa: D101

    def __init__(  # noqa: D107, S107, WPS211
        self,
        dialect: str = "mysql",
        driver: str = "mysqldb",
        username: str = "root",
        password: str = "root",
        host: str = "127.0.0.1",
        port: int = 3306,
        database: str = "",
        chunksize: int = 1000000,
        sql_dir: str = "./sql",
        sql_remove_comments: bool = True,
        sql_escape_percentage: bool = True,
        connect_args: dict = None,
    ):
        connect_args = connect_args or {}
        connect_args["cursorclass"] = connect_args.get(
            "cursorclass",
            _pick_cursorclass(driver),
        )
        # This option is needed to allow for a insert local infile which is
        # used throughout the send_data methods:
        connect_args["local_infile"] = connect_args.get(
            "local_infile",
            1,
        )
        super().__init__(
            dialect,
            driver,
            username,
            password,
            host,
            port,
            database,
            chunksize,
            sql_dir,
            sql_remove_comments,
            sql_escape_percentage,
            connect_args,
        )


def _pick_cursorclass(driver):
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
