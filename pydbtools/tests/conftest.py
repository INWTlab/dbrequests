"""Shared pytest fixtures.
"""
import pytest
import pydbtools


@pytest.fixture
def cats(db):
    """Database with table `cats` created
    tear_down drops the table.
    Typically applied by `@pytest.mark.usefixtures('cats')`
    """
    db.query("""
        CREATE TABLE cats
            (
              id              INT unsigned NOT NULL AUTO_INCREMENT, # Unique ID for the record
              name            VARCHAR(150) NOT NULL,                # Name of the cat
              owner           VARCHAR(150) NOT NULL,                # Owner of the cat
              birth           DATE NOT NULL,                        # Birthday of the cat
              PRIMARY KEY     (id)                                  # Make the id the primary key
            );
        """)
    yield
    db.query('DROP TABLE cats')
