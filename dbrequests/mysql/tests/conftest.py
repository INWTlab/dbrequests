import time

import pandas as pd
import pytest
from dbrequests.mysql import Database
from docker import from_env

infile
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
    container = client.containers.run('mariadb:10.3',
                                      name='test-mariadb-database',
                                      ports={3306: creds['port']},
                                      environment={
                                          'MYSQL_ROOT_PASSWORD':
                                          creds['password'],
                                          'MYSQL_DATABASE': creds['db']
                                      },
                                      detach=True)
    url = ("mysql+pymysql://{}:{}@{}:{}/{}".format(creds['user'],
                                                   creds['password'],
                                                   creds['host'],
                                                   creds['port'], creds['db']))
    return url, container


def kill_remove_docker_container(container):
    """kill and remove mariadb docker container"""
    container.kill()
    container.remove()
    return 0, 'Container {} removed.'.format(container.id[:12])


def set_up_cats(db):
    """ set up the right table for testing"""
    db.bulk_query("""
        SET @@SQL_MODE = REPLACE(@@SQL_MODE, 'STRICT_TRANS_TABLES', '');
        """)
    db.bulk_query("""
        CREATE TABLE IF NOT EXISTS cats
            (
              id              INT unsigned NOT NULL AUTO_INCREMENT,
              name            VARCHAR(150) NOT NULL,
              owner           VARCHAR(150) NOT NULL,
              birth           DATE NOT NULL,
              PRIMARY KEY     (id)
            );
        """)
    db.bulk_query("TRUNCATE TABLE cats;")
    db.bulk_query("""
        INSERT INTO cats (id, name, owner, birth) VALUES
          (1, 'Sandy', 'Lennon', '2015-01-03' ),
          (2, 'Cookie', 'Casey', '2013-11-13' ),
          (3, 'Charlie', 'River', '2016-05-21' );
        """)


@pytest.fixture
def db():
    url, container = run_docker_container()
    time.sleep(20)
    try:
        db = Database(url)
        yield db
        db.close()
        kill_remove_docker_container(container)
    except Exception as e:
        kill_remove_docker_container(container)
        raise (e)
