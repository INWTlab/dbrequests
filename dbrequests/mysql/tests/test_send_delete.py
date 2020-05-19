"""Testing send_data functionality."""

import datatable as dt
import pytest
from dbrequests.mysql.tests.conftest import (
    set_up_diffs as reset_diffs)


pytestmark = pytest.mark.usefixtures('db')


class TestSendDeleteInSet:
    """Tests for mode=in_set."""

    def test_delete_entries(self, db):
        """Delete some entries from a table."""
        reset_diffs(db)
        df = dt.Frame({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        })

        df_delete = dt.Frame({
            # ids we want to delete:
            'id': [1, 2]
        })

        db.send_data(df, 'diffs')
        nrows = db.send_delete(df_delete, 'diffs', 'in_set')
        res = db.send_query('select * from diffs', to_pandas=False)
        assert nrows == 2
        assert res.shape == (1, 3)
        assert res[0, 'id'] == 3

    def test_delete_with_multiple_cols(self, db):
        """Delete entries from a table identified by multiple columns."""
        reset_diffs(db)
        df = dt.Frame({
            'id': [1, 2, 3, 4],
            'value': ['a', 'b', 'b', 'c']
        })

        df_delete = dt.Frame({
            # ids we want to delete:
            'id': [2, 3],
            # in set means in 'id' and in 'value', so we expect that both rows
            # will be deleted, because we have a value in the space of allowed
            # values. This is the difference compared to in_join!
            'value': ['b', 'c']
        })

        db.send_data(df, 'diffs')
        nrows = db.send_delete(df_delete, 'diffs', 'in_set')
        res = db.send_query('select * from diffs', to_pandas=False)
        assert nrows == 2
        assert res.shape == (2, 3)


class TestSendDeleteNotInSet:
    """Tests for mode=not_in_set."""

    def test_delete_entries(self, db):
        """Delete some entries from a table."""
        reset_diffs(db)
        df = dt.Frame({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        })

        df_delete = dt.Frame({
            # ids we want to keep:
            'id': [1, 2]
        })

        db.send_data(df, 'diffs')
        nrows = db.send_delete(df_delete, 'diffs', 'not_in_set')
        res = db.send_query('select * from diffs', to_pandas=False)
        assert nrows == 1
        assert res.shape == (2, 3)
        assert res[0, 'id'] == 1
        assert res[1, 'id'] == 2


class TestSendDeleteInJoin:
    """Tests for mode=in_join."""

    def test_delete_entries(self, db):
        """Delete some entries from a table."""
        reset_diffs(db)
        df = dt.Frame({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        })

        df_delete = dt.Frame({
            # ids we want to delete:
            'id': [1, 2]
        })

        db.send_data(df, 'diffs')
        nrows = db.send_delete(df_delete, 'diffs', 'in_join')
        res = db.send_query('select * from diffs', to_pandas=False)
        assert nrows == 2
        assert res.shape == (1, 3)
        assert res[0, 'id'] == 3

    def test_delete_with_multiple_cols(self, db):
        """Delete entries from a table identified by multiple columns."""
        reset_diffs(db)
        df = dt.Frame({
            'id': [1, 2, 3, 4],
            'value': ['a', 'b', 'b', 'c']
        })

        df_delete = dt.Frame({
            # ids we want to delete:
            'id': [2, 3],
            # in_join means rows are identified as in the df we submit. So we
            # expect that only one row will be deleted. This is different from
            # in_set!
            'value': ['b', 'c']
        })

        db.send_data(df, 'diffs')
        nrows = db.send_delete(df_delete, 'diffs', 'in_join')
        res = db.send_query('select * from diffs', to_pandas=False)
        assert nrows == 1
        assert res.shape == (3, 3)


class TestSendDeleteNotInJoin:
    """Tests for mode=not_in_join."""

    def test_delete_entries(self, db):
        """Delete some entries from a table."""
        reset_diffs(db)
        df = dt.Frame({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        })

        df_delete = dt.Frame({
            # ids we want to keep:
            'id': [1, 2]
        })

        db.send_data(df, 'diffs')
        nrows = db.send_delete(df_delete, 'diffs', 'not_in_join')
        res = db.send_query('select * from diffs', to_pandas=False)
        assert nrows == 1
        assert res.shape == (2, 3)
        assert res[0, 'id'] == 1
        assert res[1, 'id'] == 2
