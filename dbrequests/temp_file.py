"""A contextmanager for temporary files."""

import os
from contextlib import contextmanager
from tempfile import NamedTemporaryFile


@contextmanager
def temp_file():
    """
    Create a contextmanager for creating and removing a temporary file.

    Yields:
        - str the name of the temporary file.

    We need this for Windows compatibility: see
    https://bugs.python.org/msg157925
    This allows the file connection to be closed during the context manager;
    but we then have to clean up, hence this function.
    """
    tfile = NamedTemporaryFile(mode="w", newline="", delete=False, encoding="utf-8")
    tfile.close()
    try:
        yield tfile.name
    finally:
        os.unlink(tfile.name)
