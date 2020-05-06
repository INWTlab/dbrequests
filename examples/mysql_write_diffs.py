from datatable import f, Frame
import string
import time
import random as rnd
from docker import from_env

import dbrequests.mysql as mysql

# Globals
DOCKER_CONFIG = {
    'image': 'mariadb:10.3',
    'name': 'test-mariadb-database',
    'ports': {3306: 3307},
    'environment': {'MYSQL_ROOT_PASSWORD': 'root', 'MYSQL_DATABASE': 'test'},
    'detach': True
}
CLIENT = from_env()
CONTAINER = CLIENT.containers.run(**DOCKER_CONFIG)
URL = 'mysql+mysqldb://root:root@0.0.0.0:3307/test'
NROW = 20000000


# Helper
class Stopwatch:

    def __init__(self, label='execution'):
        self._label = label
        self._start_time = time.time()

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed_time = time.time() - self._start_time
        print('[{}] finished in {} s'.format(
            self._label, int(elapsed_time)))


def numbers(nrow):
    "Generate 'nrow' random integers."
    return [rnd.randint(0, 1) for _ in range(nrow)]


def chars(nrow):
    "Generate 'nrow' random strings."
    return [''.join(rnd.choices(string.ascii_letters, k=8))
            for _ in range(nrow)]


DF = Frame(
    id=range(NROW),
    char1=chars(NROW),
    char2=chars(NROW),
    num1=numbers(NROW),
    num2=numbers(NROW)
)

with mysql.Database(URL) as db:
    db.send_bulk_query("""
    CREATE TABLE test.some_table
    (
        id INTEGER NOT NULL,
        char1 VARCHAR(8) NOT NULL,
        char2 VARCHAR(8) NOT NULL,
        num1 INTEGER NOT NULL,
        num2 INTEGER NOT NULL
    )
    PRIMARY KEY (`id`)
    ENGINE=InnoDB
    DEFAULT CHARSET=utf8mb4;
    """)

# With ~ 25% rows changed:
DF_NEW = DF.copy()
DF_NEW[f.id < NROW / 2, 4] = 1

with mysql.Database(URL) as db:
    db.send_data(DF, 'some_table', mode='truncate')
    time.sleep(200)
    with Stopwatch('Send with update'):
        db.send_data(DF_NEW, 'some_table', 'update')

with mysql.Database(URL) as db:
    db.send_data(DF, 'some_table', mode='truncate')
    time.sleep(200)
    with Stopwatch('Send update diffs'):
        db.send_data(DF_NEW, 'some_table', 'update_diffs')

# With ~5% changes
DF_NEW = DF.copy()
DF_NEW[f.id < NROW * 0.1, 4] = 1
with mysql.Database(URL) as db:
    db.send_data(DF, 'some_table', mode='truncate')
    time.sleep(200)
    with Stopwatch('Send only diffs'):
        db.send_data(DF_NEW, 'some_table', 'update_diffs')

# With ~2.5% changes
DF_NEW = DF.copy()
DF_NEW[f.id < NROW * 0.5, 4] = 1
with mysql.Database(URL) as db:
    db.send_data(DF, 'some_table', mode='truncate')
    time.sleep(200)
    with Stopwatch('Send only diffs'):
        db.send_data(DF_NEW, 'some_table', 'update_diffs')

CONTAINER.kill()
CONTAINER.remove()
CLIENT.close()
