from typing import List, Optional

from datatable import Frame, f, join


def make_diff(dfa: Frame, dfb: Frame, keys: Optional[List[str]] = None):
    """
    Find the rows in dfa that are not in dfb.

    Args:
        dfa (Frame): A data frame.
        dfb (Frame): A data frame.
        keys (List[str]): names of columns identifying unique rows in frame.

    Returns:
        A frame with rows from dfa that can not be found in dfb.

    """
    if keys is None:
        keys = dfa.names
    if dfb.shape[0] > 0:
        # prepare the data
        dfa.key = keys
        dfb = dfb[:, keys]  # we only need the keys to find diffs
        dfb = dfb[:, f[:].extend({"_a_": 1})]
        dfb.key = keys
        # remove the duplicates from dfa
        diffs = dfa[:, :, join(dfb)]
        diffs = diffs[f._a_ == 1, :]  # noqa: W0212,WPS437 (access of _var)
        diffs = _drop_col(diffs, "_a_")
        dfb = _drop_col(dfb, "_a_")
    else:
        # return dfa if dfb is empty
        diffs = dfa

    return diffs


def _drop_col(df: Frame, col: str):
    return df[:, f[:].remove(f[col])]
