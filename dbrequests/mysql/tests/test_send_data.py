"""Testing send_data functionality."""

import pandas as pd
import pytest
import numpy as np
from dbrequests.mysql.tests.conftest import set_up_cats as reset
from dbrequests.mysql.tests.conftest import (
    set_up_membership as reset_membership)
from sqlalchemy.exc import OperationalError, InternalError


@pytest.mark.usefixtures('db')
class TestSendDataInsert:
    """Tests for mode=insert."""

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
        """Do not override on duplicate key."""
        df_add = pd.DataFrame({
            'id': [3],
            'name': ['Charlie'],
            'owner': ['River'],
            'birth': ['2016-05-22']
        })

        reset(db)
        db.send_data(df_add, 'cats', mode='insert')
        df_out = db.query("SELECT * FROM cats where id = 3;")

        assert df_out.birth.astype(str)[0] == '2016-05-21'


@pytest.mark.usefixtures('db')
class TestSendDataDeletes:
    """Tests for mode=delete|truncate."""

    def test_send_data_truncate(self, db):
        """Truncate table before insert."""
        df_replace = pd.DataFrame({
            'id': [1],
            'name': ['Chill'],
            'owner': ['Alex'],
            'birth': ['2018-03-03']
        })

        reset(db)
        db.send_data(df_replace, 'cats', mode='truncate')

        df_nrow = db.query("SELECT count(*) as nrows FROM cats;")
        assert df_nrow.nrows[0] == 1

        df_out = db.query("SELECT * FROM cats;")
        df_out.birth = df_out.birth.astype(str)
        assert (df_replace == df_out).all(axis=None)

    def test_delete_happy_path(self, db):
        """First delete all rows, then insert new data"""
        df_replace = pd.DataFrame({
            'id': [1],
            'name': ['Chill'],
            'owner': ['Alex'],
            'birth': ['2018-03-03']
        })

        reset(db)
        db.send_data(df_replace, 'cats', mode='delete')

        df_nrow = db.query("SELECT count(*) as nrows FROM cats;")
        assert df_nrow.nrows[0] == 1

    def test_delete_rollback_on_failure(self, db):
        """Delete before insert and check the rollback."""
        df_replace = pd.DataFrame({
            'id': [1],
            'name': ['Chill'],
            'owner': ['Alex'],
            'wrong_col': ['2018-03-03']
        })

        reset(db)
        # InternalError with pymysql
        # OperationalError with mysqldb
        with pytest.raises((OperationalError, InternalError)):
            db.send_data(df_replace, 'cats', mode='delete')

        df_nrow = db.query("SELECT count(*) as nrows FROM cats;")
        assert df_nrow.nrows[0] == 3


@pytest.mark.usefixtures('db')
class TestSendDataReplace:
    """Tests for mode=replace."""

    def test_send_data_replace(self, db):
        """Send data and replace on duplicate key."""
        df_replace = pd.DataFrame({
            'id': [1],
            'name': ['Chill'],
            'owner': ['Alex'],
            'birth': ['2018-03-03']
        })

        reset(db)
        db.send_data(df_replace, 'cats', mode='replace')

        df_nrow = db.query("SELECT count(*) as nrows FROM cats;")
        assert df_nrow.nrows[0] == 3

        df_out = db.query("SELECT * FROM cats where id = 1;")
        df_out.birth = df_out.birth.astype(str)
        assert (df_replace == df_out).all(axis=None)


@pytest.mark.usefixtures('db')
class TestSendDataUpdate:
    """Tests for mode=update."""

    def test_send_data_update(self, db):
        """Check for mode update.

        Update means:
        - we can add rows / new data, similar to insert
        - we can update on duplicate key instead of replace
        - we can update selected columns, maybe just one field + primary key
        """
        df_replace = pd.DataFrame({
            'id': [1, 4],
            'name': ['Chill', 'Pi'],
            'owner': ['Alex', 'Matt'],
            'birth': ['2018-03-03', '2019-08-05']
        })

        reset(db)
        db.send_data(df_replace, 'cats', mode='update')

        # We have a new row:
        df_nrow = db.query("SELECT count(*) as nrows FROM cats;")
        assert df_nrow.nrows[0] == 4

        # Changes are made:
        df_out = db.query("SELECT * FROM cats where id in (1, 4);")
        df_out.birth = df_out.birth.astype(str)
        assert (df_replace == df_out).all(axis=None)

        # We can send partial updates, aka single column
        df_replace_small = pd.DataFrame({
            'id': [2],
            'birth': ['2014-11-13']  # we update this value for id = 2
        })
        df_expected = pd.DataFrame({
            'id': [2],
            'name': ['Cookie'],
            'owner': ['Casey'],
            'birth': ['2014-11-13']
        })
        reset(db)
        db.send_data(df_replace_small, 'cats', mode='update')
        df_out = db.query("SELECT * FROM cats where id = 2;")
        df_out.birth = df_out.birth.astype(str)
        assert (df_expected == df_out).all(axis=None)


@pytest.mark.usefixtures('db')
class TestSendDataBehaviours:
    """Behaviours which are due to CSV and work for all modes."""

    def test_send_data_idempotence(self, db):
        """We check that reading and writing back in is idempotent.

        This is not obvious because we write to a CSV as intermediate step!
        Special cases, we need to check:
        - missing values
        - dates / (datetimes)
        - (decimals)
        - (64bit integer)
        TODO: Currently we hold hands and pray that these cases actually work!
        """
        df_replace = pd.DataFrame({
            'id': [1],
            'name': ['Chill'],
            'owner': [np.nan],
            'birth': ['2018-03-03']
        })

        reset(db)
        db.send_data(df_replace, 'cats', mode='replace')
        df_in = db.query("SELECT * FROM cats;")
        db.send_data(df_in, 'cats', mode='truncate')
        df_inn = db.query("SELECT * FROM cats;")

        assert (df_in == df_inn).all(axis=None)

    def test_column_arrangemant_is_maintained(self, db):
        """Insert some data with fliped columns: #24"""
        reset(db)
        df_1 = db.send_query(
            "select birth, name, owner from cats where id = 3;")
        db.send_data(df_1, 'cats', mode='insert')
        df_2 = db.send_query(
            "select birth, name, owner from cats where id = 4;")
        assert (df_1 == df_2).all(axis=None)

    def test_escape_sequences(self, db):
        """Insert some data with escape sequences: #28"""
        reset(db)
        db.send_bulk_query('truncate table cats;')
        db.send_data({'name': ['\\'], 'owner': ['0bnrtZN']}, 'cats')
        res = db.send_query('select name, owner from cats;')

        assert res.name[0] == '\\'
        # all known escape sequences from:
        #   https://dev.mysql.com/doc/refman/8.0/en/load-data.html
        assert res.owner[0] == '0bnrtZN'

    def test_update_json_and_decimal(self, db):
        """Insert None/NULL values for json and decimal types: #30"""
        reset_membership(db)
        df_update = pd.DataFrame({
            'id': range(4),
            'membership': [
                '{"BookClub": 1, "SportsClub": 1, "ClubClub": 1,}',
                '{"BookClub": 0, "SportsClub": 0.5, "ClubClub": 0}',
                '{"BookClub": null, "SportsClub": 1, "ClubClub": 2}',
                None],
            'average': [34.49, 34.51, 43.86, None]})

        db.send_data(df_update, 'membership', mode='truncate')
        df_in = db.send_query('SELECT * FROM membership')
        assert self.is_na(df_in.membership[3])
        assert np.isnan(df_in.average[3])

    @staticmethod
    def is_na(x):
        if x:
            return np.isnan(x)
        else:
            return True
