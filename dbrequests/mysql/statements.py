from typing import List


def infile(file_name: str, col_names: str, table: str, replace: bool = False) -> str:
    if replace:
        replace_str = "replace"
    else:
        replace_str = ""
    stmt = """
    load data local infile '{path}'
    {replace}
    into table `{table}`
    character set utf8mb4
    fields terminated by ','
    optionally enclosed by '\"'
    escaped by ''
    lines terminated by '\\n'
    ({columns});
    """  # noqa: WPS342
    return stmt.format(
        path=file_name,
        replace=replace_str,
        table=table,
        columns=_sql_cols(col_names),
    )


def insert_update(col_names: str, table: str, tmp_table: str) -> str:
    stmt = """
    insert into `{table}` ({columns})
    select {columns}
    from `{tmp_table}`
    on duplicate key update {update};
    """
    return stmt.format(
        table=table,
        columns=_sql_cols(col_names),
        tmp_table=tmp_table,
        update=_sql_update(col_names),
    )


def select_cols(table: str, cols: List[str]) -> str:
    return "select {cols} from {table};".format(cols=_sql_cols(cols), table=table)


def delete_in_delete_col(table: str) -> str:
    return "delete from `{table}` where `delete` = 1;".format(table=table)


def show_primary(table: str) -> str:
    query = """
    SHOW INDEXES FROM {table}
    where key_name = "PRIMARY"
    and column_name != "row_end";
    """
    return query.format(table=table)


def _sql_cols(col_names):
    return ", ".join(["`{name}`".format(name=str(name)) for name in col_names])


def _sql_update(col_names):
    return ", ".join(
        ["`{name}`=values(`{name}`)".format(name=str(name)) for name in col_names]
    )
