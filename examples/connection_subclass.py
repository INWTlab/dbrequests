from dbrequests import Connection
from dbrequests import Database
from docker import from_env


class ExampleConnection(Connection):
    """
    Within this example, we inherit everything from the Connection class, but overwrite bulk_query, which is accessed
    via the Database method send_bulk_query.
    """

    def bulk_query(self, query, **params):
        print('You sent the query: {}'.format(query))


class ExampleDatabase(Database):
    """
    This Database child class only overwrites the Database initialization by passing the ExampleConnection class.
    If additional parameters are to be handed to the new Connection class, this could be done by additionally overwriting
    get_connection.
    """

    def __init__(self, db_url=None, creds=None, sql_dir=None,
                 escape_percentage=False, remove_comments=False, **kwargs):
        super().__init__(db_url=db_url, creds=creds, sql_dir=sql_dir, connection_class=ExampleConnection,
                         escape_percentage=escape_percentage, remove_comments=remove_comments, **kwargs)


if __name__ == '__main__':
    """We test the example by setting up a mariadb database to run our new model against"""
    creds = {
        'user': 'root',
        'password': 'root',
        'host': '127.0.0.1',
        'db': 'test',
        'port': 3307
    }
    client = from_env()
    container = client.containers.run('mariadb:10.3', name='test-mariadb-database',
                                      ports={3306: creds['port']},
                                      environment={'MYSQL_ROOT_PASSWORD': creds['password'],
                                                   'MYSQL_DATABASE': creds['db']},
                                      detach=True)
    url = ("mysql+pymysql://{}:{}@{}:{}/{}".format(creds['user'], creds['password'], creds['host'], creds['port'],
                                                   creds['db']))
    db = ExampleDatabase(url)
    db.send_bulk_query('select hi from hi')  # only prints the query to stdout
    db.close()
    container.kill()
    container.remove()
    assert client.containers.list() == []
    client.close()
