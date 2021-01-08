from contextlib import contextmanager
from typing import List, Optional

from dbrequests.send_query import send_query
from dbrequests.session import Session


@contextmanager
def temporary_table(
    session: Session,
    table: str,
    with_cols: Optional[List[str]] = None,
    with_temp: bool = True,
):
    """Contextmanager to create and drop temporary tables."""
    tmp_table = "tmp_dbrequests_{table}".format(table=table)
    if with_temp:
        temp_stmt = "temporary"
    else:
        temp_stmt = ""
    try:
        yield _create_temporary_table(session, table, tmp_table, with_cols, temp_stmt)
    finally:
        send_query(
            session,
            "drop {temp_stmt} table if exists {tmp_table};".format(
                temp_stmt=temp_stmt,
                tmp_table=tmp_table,
            ),
        )


def _create_temporary_table(
    session: Session,
    table: str,
    tmp_table: str,
    with_cols: Optional[List[str]],
    temp_stmt: str,
):
    _send_create_statement(session, temp_stmt, tmp_table, table)
    _drop_partition(session, tmp_table)
    _drop_system_versioning(session, tmp_table)
    _drop_columns(session, tmp_table, with_cols, table)
    return tmp_table


def _send_create_statement(
    session: Session,
    temp_stmt: str,
    tmp_table: str,
    table: str,
):
    send_query(
        session,
        "create {temp} table `{tmp_table}` like `{table}`;".format(
            temp=temp_stmt,
            tmp_table=tmp_table,
            table=table,
        ),
    )


def _drop_partition(session: Session, tmp_table: str):
    is_partitioned = send_query(
        session,
        """
        select `create_options` from `information_schema`.`tables`
        where `table_name` = '{tmp_table}';""".format(
            tmp_table=tmp_table,
        ),
    )
    if is_partitioned.shape[0] > 0:
        if is_partitioned.create_options[0] == "partitioned":
            send_query(
                session,
                "alter table `{tmp_table}` remove partitioning;".format(
                    tmp_table=tmp_table,
                ),
            )


def _drop_system_versioning(session: Session, tmp_table: str):
    is_system_versioned = send_query(
        session,
        """
        select `table_type`
        from `information_schema`.`tables`
        where `table_name` = "{tmp_table}";""".format(
            tmp_table=tmp_table,
        ),
    )
    if is_system_versioned.shape[0] > 0:
        if is_system_versioned.table_type[0] == "SYSTEM VERSIONED":
            send_query(
                session,
                "alter table `{tmp_table}` drop system versioning".format(
                    tmp_table=tmp_table,
                ),
            )


def _drop_columns(
    session: Session,
    tmp_table: str,
    with_cols: Optional[List[str]],
    table: str,
):
    if with_cols is not None:
        res = send_query(session, "show columns from {table};".format(table=table))
        cols_to_drop = [name for name in res.Field.to_list() if name not in with_cols]
        if cols_to_drop:
            drop_query = "alter table `{tmp_table}` {cols_to_drop};".format(
                tmp_table=tmp_table,
                cols_to_drop=", ".join(
                    ["drop column `{col}`".format(col=col) for col in cols_to_drop],
                ),
            )
            send_query(session, drop_query)
