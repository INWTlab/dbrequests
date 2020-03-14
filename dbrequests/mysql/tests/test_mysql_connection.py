import pandas as pd
import pytest
from dbrequests.mysql.tests.conftest import set_up_cats as reset


# TODOs: Ideas for further Tests:
# Since we write to CSV, we should check that write, then read
# - numbers
# - dates
# - NAs/NaN
# is idempotent. Don't know how to do that with the pandas data types.
# Everything is just an 'object'.
@pytest.mark.usefixtures('db')
class TestConnection:
    """Unit Tests for send_data method of a mysql connection."""

    def test_insert_happy_path(self, db):
        """Insert some data and check, that it is actually there."""
        df_add = pd.DataFrame({
            'name': ['Chill'],
            'owner': ['Alex'],
            'birth': ['2018-03-03']
        })

        reset(db)
        db.send_data(df_add, 'cats', mode='insert')
        df_out = db.query("select name, owner, birth from cats where id = 4;")

        df_out.birth = df_out.birth.astype(str)
        assert (df_add == df_out).all(axis=None)

    def test_insert_no_override(self, db):
        """Do not override on duplictate key."""
        df_add = pd.DataFrame({
            'id': [3],
            'name': ['Charlie'],
            'owner': ['River'],
            'birth': ['2016-05-22']
        })

        reset(db)
        db.send_data(df_add, 'cats', mode='insert')
        df_out = db.query("SELECT * FROM cats where id = 3;")

        assert df_out.birth.astype(str).values[0] == '2016-05-21'

    def test_send_data_truncate(self, db):
        df_replace = pd.DataFrame({
            'id': [1],
            'name': ['Chill'],
            'owner': ['Alex'],
            'birth': ['2018-03-03']
        })

        reset(db)
        db.send_data(df_replace, 'cats', mode='truncate')

        df_nrow = db.query("SELECT count(*) as nrows FROM cats;")
        assert df_nrow.nrows.values[0] == 1

        df_out = db.query("SELECT * FROM cats;")
        df_out.birth = df_out.birth.astype(str)
        assert (df_replace == df_out).all(axis=None)

    def test_send_data_delete(self, db):
        ## Testing for rollback
        df_replace = pd.DataFrame({
            'id': [1],
            'name': ['Chill'],
            'owner': ['Alex'],
            'wrong_col': ['2018-03-03']
        })
        
        reset(db)
        try:
            db.send_data(df_replace, 'cats', mode='delete')
        finally:
            print('totally intended')
        
        df_nrow = db.query("SELECT count(*) as nrows FROM cats;")
        assert df_nrow.nrows.values[0] == 3

        ## Now again for a happy delete mode:
        df_replace = pd.DataFrame({
            'id': [1],
            'name': ['Chill'],
            'owner': ['Alex'],
            'birth': ['2018-03-03']
        })
        
        reset(db)
        db.send_data(df_replace, 'cats', mode='delete')
        
        df_nrow = db.query("SELECT count(*) as nrows FROM cats;")
        assert df_nrow.nrows.values[0] == 1

    def test_send_data_replace(self, db):
        df_replace = pd.DataFrame({
            'id': [1],
            'name': ['Chill'],
            'owner': ['Alex'],
            'birth': ['2018-03-03']
        })

        reset(db)
        db.send_data(df_replace, 'cats', mode='replace')

        df_nrow = db.query("SELECT count(*) as nrows FROM cats;")
        assert df_nrow.nrows.values[0] == 3

        df_out = db.query("SELECT * FROM cats where id = 1;")
        df_out.birth = df_out.birth.astype(str)
        assert (df_replace == df_out).all(axis=None)

    def test_send_data_update(self, db):
        df_replace = pd.DataFrame({
            'id': [1, 4],
            'name': ['Chill', 'Pi'],
            'owner': ['Alex', 'Matt'],
            'birth': ['2018-03-03', '2019-08-05']
        })

        df_replace_small = pd.DataFrame({
            'id': [2],
            'birth': ['2014-11-13']
        })

        reset(db)
        db.send_data(df_replace, 'cats', mode='update')

        df_nrow = db.query("SELECT count(*) as nrows FROM cats;")
        assert df_nrow.nrows.values[0] == 4

        df_out = db.query("SELECT * FROM cats where id in (1, 4);")
        assert (df_replace == df_out).all(axis=None)

        reset(db)
        db.send_data(df_replace_small, 'cats', mode='update')

        df_out = db.query("SELECT * FROM cats where id = 2;")
        assert (pd.DataFrame({
            'id': [2],
            'name': ['Cookie'],
            'owner': ['Casey'],
            'birth': ['2014-11-13']
        }) == df_out).all(axis=None)
