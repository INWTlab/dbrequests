"""Shared pytest fixtures.
"""
import pytest
import dbrequests
import os
from sqlalchemy import create_engine
from docker import from_env
import time

sql_dir = os.path.join(os.getcwd(), 'dbrequests/tests/sql')
sql_dir = os.path.join(os.path.dirname(__file__), 'sql')

def run_docker_container():
    """run mariadb-docker container and return proper url to access"""
    creds = {
        'user': 'root',
        'password': 'root',
        'host': '127.0.0.1',
        'db': 'test',
        'port': 3307
    }
    client = from_env()
    client.containers.list()
    container = client.containers.run('mariadb:10.3', name='test-mariadb-database',
        ports={3306: creds['port']}, environment={'MYSQL_ROOT_PASSWORD': creds['password'], 'MYSQL_DATABASE': creds['db']},
        detach=True)
    url = ("mysql+pymysql://{}:{}@{}:{}/{}".format(creds['user'], creds['password'], creds['host'], creds['port'], creds['db']))
    return url, container


def kill_remove_docker_container(container):
    """kill and remove mariadb docker container"""
    container.kill()
    container.remove()
    return 0, 'Container {} removed.'.format(container.id[:12])


def set_up_table(db):
    """ set up the right table for testing"""
    db.bulk_query("""
        SET @@SQL_MODE = REPLACE(@@SQL_MODE, 'STRICT_TRANS_TABLES', '');
        """)
    db.bulk_query("""
        CREATE TABLE cats
            (
              id              INT unsigned NOT NULL AUTO_INCREMENT, # Unique ID for the record
              name            VARCHAR(150) NOT NULL DEFAULT '',                # Name of the cat
              owner           VARCHAR(150) NOT NULL DEFAULT '',                # Owner of the cat
              birth           DATE NOT NULL,                        # Birthday of the cat
              PRIMARY KEY     (id)                                  # Make the id the primary key
            );
        """)
    db.bulk_query("""
        INSERT INTO cats ( name, owner, birth) VALUES
          ( 'Sandy', 'Lennon', '2015-01-03' ),
          ( 'Cookie', 'Casey', '2013-11-13' ),
          ( 'Charlie', 'River', '2016-05-21' );
        """)


@pytest.fixture(scope="module")
def db(request):
    """Instance of `dbrequests.Database(dburl)`
    Ensure, it gets closed after being used in a test or fixture.
    Parametrized with (sql_url_id, sql_url_template) tuple.
    If `sql_url_template` contains `{dbfile}` it is replaced with path to a
    temporary file.
    Feel free to parametrize for other databases and experiment with them.
    """
    url, container = run_docker_container()
    try:
        time.sleep(100)
        db = dbrequests.Database(url, sql_dir=sql_dir)
        yield db  # providing fixture value for a test case
        # tear_down
        db.close()
        kill_remove_docker_container(container)
    except Exception as e:
        kill_remove_docker_container(container)
        raise(e)


@pytest.fixture(scope="module")
def con(request):
    """Instance of `dbrequests.Connection(dburl)`
    Ensure, it gets closed after being used in a test or fixture.
    Parametrized with (sql_url_id, sql_url_template) tuple.
    If `sql_url_template` contains `{dbfile}` it is replaced with path to a
    temporary file.
    Feel free to parametrize for other databases and experiment with them.
    """
    url, container = run_docker_container()
    engine = create_engine(url)
    try:
        time.sleep(100)
        con = dbrequests.Connection(engine.connect())
        yield con  # providing fixture value for a test case
        # tear_down
        con.close()
        kill_remove_docker_container(container)
    except Exception as e:
        kill_remove_docker_container(container)
        raise(e)


@pytest.fixture
def cats(db):
    """Database with table `cats` created
    tear_down drops the table.
    Typically applied by `@pytest.mark.usefixtures('cats')`
    """
    set_up_table(db)
    yield
    db.send_bulk_query('DROP TABLE cats;')


@pytest.fixture
def cats_con(con):
    """Database with table `cats` created
    tear_down drops the table.
    Typically applied by `@pytest.mark.usefixtures('cats_con')`
    """
    set_up_table(con)
    yield
    con.bulk_query('DROP TABLE cats;')
