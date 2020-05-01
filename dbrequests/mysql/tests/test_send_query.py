"""
We test send query specific features. Happy path functionality is covered in
the send_data test suite.
"""
import pytest
from dbrequests.mysql.tests.conftest import set_up_cats as reset


@pytest.mark.usefixtures('db')
class TestSendQueryBehaviours:
    """Unit Tests for send_data method of a mysql connection."""

    def test_empty_result_set(self, db):
        """Dealing with empty result sets: #32"""
        reset(db)
        res = db.send_query('select * from cats where id < 0')
        assert res.shape == (0, 4)
