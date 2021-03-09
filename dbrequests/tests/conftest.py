"""Shared pytest fixtures.
"""
import logging
import os

import pytest

import dbrequests
from dbrequests.mysql.configuration import Configuration
from dbrequests.session import Session
from dbrequests.tests.docker import start_db, tear_down_db

sql_dir = os.path.join(os.getcwd(), 'dbrequests/tests/sql')
sql_dir = os.path.join(os.path.dirname(__file__), 'sql')

MySQLConf = Configuration(
    "mysql",
    "mysqldb",
    "user",
    "password",
    "127.0.0.1",
    3306,
    "test",
    sql_dir=sql_dir,
)


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


@pytest.fixture(scope="package")
def db(request):
    """Instance of `dbrequests.Database(dburl)`."""
    db = dbrequests.Database(
        str(MySQLConf.url),
        sql_dir=MySQLConf.query_args["sql_dir"],
    )
    yield db
    db.close()


@pytest.fixture(scope="package")
def con(db):
    """Instance of `dbrequests.Connection(dburl)`."""
    return db.get_connection()


@pytest.fixture
def cats(db):
    """Database with table `cats` created."""
    set_up_table(db)
    yield
    db.send_bulk_query('DROP TABLE cats;')


@pytest.fixture
def cats_con(con):
    """Database with table `cats` created."""
    set_up_table(con)
    yield
    con.bulk_query('DROP TABLE cats;')


@pytest.fixture(scope="package", params=[MySQLConf])
def session(request):
    """Create instances of session objects."""
    test_session = Session(request.param)
    try:
        yield test_session
    finally:
        test_session.close()


@pytest.fixture(scope="package", autouse=True)
def container_controller(request):
    """Startup database fixture."""
    logging.info("Starting test database.")
    start_db(MySQLConf)
    logging.info("here")
    yield
    logging.info("now here")
    tear_down_db(MySQLConf)
