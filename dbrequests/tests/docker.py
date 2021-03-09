import logging
import os
import re
import time

from docker import from_env
from docker.client import DockerClient

from dbrequests import Configuration


def start_db(configuration: Configuration):
    """
    Start a database running inside a docker container.

    Args:
        configuration (Configuration): A configuration.

    Returns:
        A container object.
    """
    client = from_env()
    db_name = _make_db_name(configuration)
    if _get_running_db(db_name, client):
        logging.info("Database already running.")
        container = _get_running_db(db_name, client)[0]
    else:
        container = client.containers.run(**_make_container_settings(configuration))
        time.sleep(int(os.getenv("DBREQUESTS_SLEEP_AFTER_STARTUP", "60")))
    return container


def _make_container_settings(configuration: Configuration) -> dict:
    dialect = configuration.url.get_dialect().name
    settings = {"name": _make_db_name(configuration), "detach": True}
    if dialect == "mysql":
        if configuration.url.host != "127.0.0.1":
            raise ResourceWarning("The 'host' is ignored and replaced by '127.0.0.1'.")
        settings["image"] = "mariadb:10.3"
        settings["ports"] = {3306: configuration.url.port}
        settings["environment"] = {
            "MYSQL_ROOT_PASSWORD": configuration.url.password,
            "MYSQL_DATABASE": configuration.url.database,
            "MYSQL_USER": configuration.url.username,
            "MYSQL_PASSWORD": configuration.url.password,
        }
    else:
        raise NotImplementedError("Settings for ", dialect, "are not implemented!")
    return settings


def _make_db_name(configuration: Configuration) -> str:
    dialect = configuration.url.get_dialect().name
    return f"dbrequests-test-{dialect}"


def _get_running_db(db_name: str, client: DockerClient):
    running_containers = client.containers.list()
    return [
        container
        for container in running_containers
        if re.sub("^/", "", container.attrs.get("Name")) == db_name
    ]


def tear_down_db(configuration: Configuration) -> None:
    """
    Tear down a database.

    Args:
        configuration (Configuration): The configuration.

    Returns: None

    Raises:
        ResourceWarning: If the database can't be found.
    """
    client = from_env()
    db_name = _make_db_name(configuration)
    if _get_running_db(db_name, client):
        container = _get_running_db(db_name, client)[0]
        container.kill()
        container.remove()
    else:
        raise ResourceWarning("No database found to tear down.")
