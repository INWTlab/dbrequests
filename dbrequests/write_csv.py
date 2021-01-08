from datatable import Frame, f, str64


def write_csv(df: Frame, file_name) -> None:
    """Write a Frame as csv."""
    # Before writing, we need to convert all columns to strings for two
    # reasons:
    # - We have to convert any obj64 types to str64: Frame.to_csv can't
    #   process them.
    # - We have to replace None with NULL to tell MySQL, that we have
    #   actual NULL values. An empty cell is sometimes, but not always a
    #   NULL value. See #30
    # - We have to check if the frame is empty. If so we have to
    #   circumvent a  bug in datatable: see #36
    if df.shape[0] == 0:
        return None
    df = df[:, f[:].remove(f[:]).extend(str64(f[:]))][:, df.names]  # noqa: WPS221
    df.replace(None, "NULL")
    df.to_csv(path=file_name, header=False)
