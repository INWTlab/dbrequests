"""Configures the test environment. Some helper functions and fixtures."""
import time

from docker import from_env
from docker.errors import APIError

import pytest
from dbrequests.mysql import Database


CREDS = {
    'user': 'root',
    'password': 'root',
    'host': '0.0.0.0:3307',
    'db': 'test',
    'port': 3307
}


@pytest.yield_fixture(scope='module', params=['pymysql', 'mysqldb'])
def db(request):
    """Create instances of database connections."""
    creds = CREDS
    creds['driver'] = request.param
    db = Database(creds=creds)
    try:
        yield db
    except BaseException as error:
        raise error
    finally:
        db.close()


def run_docker_container():
    """Run mariadb-docker container and return proper url for access."""

    client = from_env()
    try:
        container = client.containers.run(
            'mariadb:10.3',
            name='test-mariadb-database',
            ports={3306: CREDS['port']},
            environment={
                'MYSQL_ROOT_PASSWORD':
                CREDS['password'],
                'MYSQL_DATABASE': CREDS['db']
            },
            detach=True)
        time.sleep(60)
    except APIError:
        container = client.containers.get('test-mariadb-database')

    return container


def kill_remove_docker_container(container):
    """Kill and remove mariadb docker container."""
    container.kill()
    container.remove()
    return 0, 'Container {} removed.'.format(container.id[:12])


def set_up_cats(db):
    """Set up the cats table for testing."""
    db.bulk_query("""
        SET @@SQL_MODE = REPLACE(@@SQL_MODE, 'STRICT_TRANS_TABLES', '');
        """)
    db.bulk_query("DROP TABLE IF EXISTS cats;")
    db.bulk_query("""
        CREATE TABLE cats
            (
              id              INT unsigned NOT NULL AUTO_INCREMENT,
              name            VARCHAR(150) NOT NULL,
              owner           VARCHAR(150) NOT NULL,
              birth           DATE NOT NULL,
              PRIMARY KEY     (id)
            );
        """)
    db.bulk_query("""
        INSERT INTO cats (id, name, owner, birth) VALUES
          (1, 'Sandy', 'Lennon', '2015-01-03' ),
          (2, 'Cookie', 'Casey', '2013-11-13' ),
          (3, 'Charlie', 'River', '2016-05-21' );
        """)


def set_up_membership(db):
    db.bulk_query("""
    CREATE TABLE IF NOT EXISTS `membership`
        (
            `id` int(10) unsigned NOT NULL,
            `membership` json DEFAULT NULL,
            `average` decimal(3, 2) DEFAULT NULL,
            PRIMARY KEY(`id`)
        ) CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)
    db.bulk_query("""
    TRUNCATE TABLE membership;
    """)
    db.bulk_query("""
    INSERT INTO membership (id, membership, average) VALUES
        ( 1, '{"BookClub": 1, "SportsClub": 1, "ClubClub": 1}', 1.03 )
    """)


@pytest.fixture(scope="module", autouse=True)
def container_controller(request):
    """Startup database fixture."""
    container = run_docker_container()
    try:
        yield container
    except BaseException as error:
        raise error
    finally:
        kill_remove_docker_container(container)
