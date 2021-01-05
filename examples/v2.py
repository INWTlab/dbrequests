"""
Her you can find some examples how to use the v2 module of dbrequests. It
provides mostly the same features as the core implementation but foxuses on an
extensible approach. This has also changed the user API and workflow.
"""
#%%
import random as rnd
import string
import time
from contextlib import contextmanager

from datatable import Frame
from dbrequests.mysql.send_data import send_data
from dbrequests.configuration import Configuration
from dbrequests.send_query import send_query
from docker import from_env

# %% Globals
DOCKER_CONFIG = {
    "image": "mariadb:10.4",
    "name": "test-mariadb-database",
    "ports": {3306: 3307},
    "environment": {"MYSQL_ROOT_PASSWORD": "root", "MYSQL_DATABASE": "test"},
    "detach": True,
}
CLIENT = from_env()
CONTAINER = CLIENT.containers.run(**DOCKER_CONFIG)
CREDS = Configuration(
    {
        "dialect": "mysql",
        "driver": "mysqldb",
        "user": "root",
        "password": "root",
        "host": "0.0.0.0",
        "port": 3307,
        "database": "test",
    }
)
NROW = 200000


# %% Helper
@contextmanager
def stopwatch(name):
    "Time the execution of a long running function call / code block."
    start_time = time.time()
    yield
    elapsed_time = time.time() - start_time
    print("[{}] finished in {} s".format(name, int(elapsed_time)))


def numbers(nrow):
    "Generate 'nrow' random integers."
    return [rnd.randint(0, 100000) for _ in range(nrow)]


def chars(nrow):
    "Generate 'nrow' random strings."
    return ["".join(rnd.choices(string.ascii_letters, k=8)) for _ in range(nrow)]


# %% setup
DT = Frame(
    id=range(NROW),
    char1=chars(NROW),
    char2=chars(NROW),
    num1=numbers(NROW),
    num2=numbers(NROW),
)

send_query(
    CREDS,
        """
    CREATE TABLE test.some_table
    (
        id INTEGER NOT NULL,
        char1 VARCHAR(8) NOT NULL,
        char2 VARCHAR(8) NOT NULL,
        num1 INTEGER NOT NULL,
        num2 INTEGER NOT NULL
    )
    ENGINE=InnoDB
    DEFAULT CHARSET=utf8mb4;
    """
    )

# %% send data
with stopwatch("send data with pymysql/infile"):
    send_data(CREDS, DT, "some_table", "truncate")


# %% get data
with stopwatch("get data"):
    res = send_query(CREDS, "select * from some_table;")
    res = send_query(CREDS, "set global local_infile=1;")


# %%
CONTAINER.kill()
CONTAINER.remove()
CLIENT.close()


[].append(None) is None
