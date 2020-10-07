class Credentials:
    """Deconstruct credentials object into url, connect args, etc."""

    def __init__(self, credentials: dict):

        credentials = credentials.copy()

        self.url = "{}+{}://{}:{}@{}:{}/{}".format(
            credentials.pop("dialect", "mysql"),
            credentials.pop("driver", "pymysql"),
            credentials.pop("user", "root"),
            credentials.pop("password", "root"),
            credentials.pop("host", "127.0.0.1"),
            credentials.pop("port", 3306),
            credentials.pop("db", credentials.pop("database", "test")),
        )

        self.connect_args = credentials.pop("connect_args", {})
        self.connect_args.update(credentials)
