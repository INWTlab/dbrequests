from dbrequests import Database
import pandas as pd
import pytest
import pymysql
import os

# creds = {
#     'user': 'testuser',
#     'password': 'test',
#     'host': 'localhost',
#     'db': 'test'
# }
# url = ("mysql+pymysql://{}:{}@{}/{}".format(creds['user'], creds['password'], creds['host'], creds['db']))
sql_dir = os.path.join(os.getcwd(), 'dbrequests/tests/sql')
sql_dir = os.path.join(os.path.dirname(__file__), 'sql')

@pytest.mark.usefixtures('cats')
class TestDatabase:
    def test_send_query(self, db):
        df = db.send_query("""
            SELECT * FROM cats;
            """, index_col='id')
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (3,3)
        assert (df.columns == ['name', 'owner', 'birth']).all()
        assert (df.name == ['Sandy', 'Cookie', 'Charlie']).all()

    def test_send_file(self, db):
        df = db.send_query(os.path.join(sql_dir, 'select.sql'), index_col='id')
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (3,3)
        assert (df.columns == ['name', 'owner', 'birth']).all()
        assert (df.name == ['Sandy', 'Cookie', 'Charlie']).all()

    def test_create_insert_file_param(self, db):
        df = db.send_query(os.path.join(sql_dir, 'select_param.sql'),
                            col1='name', col2='id', index_col='id')
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (3,1)
        assert (df.columns == ['name']).all()
        assert (df.name == ['Sandy', 'Cookie', 'Charlie']).all()

    def test_create_insert_file_param_dir(self, db):
        df = db.send_query('select_param',
                            col1='name', col2='id', index_col='id')
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (3,1)
        assert (df.columns == ['name']).all()
        assert (df.name == ['Sandy', 'Cookie', 'Charlie']).all()

    def test_send_data_create(self, db):
        db.bulk_query("""
            DROP TABLE cats;
            """)
        df_in = pd.DataFrame({'name': ['Sandy', 'Cookie', 'Charlie'],
                           'owner': ['Lennon', 'Casey', 'River'],
                           'birth': ['2015-01-03', '2013-11-13', '2016-05-21']})
        db.send_data(df_in, 'cats')
        df_out = db.query("""
            SELECT * FROM cats;
            """)
        assert (df_in == df_out).all(axis=None)

    def test_send_data_insert(self, db):
        df_in = pd.DataFrame({'name': ['Sandy', 'Cookie', 'Charlie'],
                           'owner': ['Lennon', 'Casey', 'River'],
                           'birth': ['2015-01-03', '2013-11-13', '2016-05-21']})
        df_add = pd.DataFrame({'name': ['Chill'], 'owner': ['Alex'], 'birth':['2018-03-03']}, index=[3])
        db.send_data(df_add, 'cats', mode = 'insert')
        df_out = db.send_query("""
            SELECT * FROM cats;
            """)
        df_out.birth = df_out.birth.astype(str)
        assert (pd.concat([df_in, df_add]) == df_out[['name', 'owner', 'birth']]).all(axis=None)

    def test_send_data_truncate(self, db):
        df_replace = pd.DataFrame({'name': ['Chill'], 'owner': ['Alex'], 'birth':['2018-03-03']}, index=[0])
        db.send_data(df_replace, 'cats', mode = 'truncate')
        df_out = db.query("""
            SELECT * FROM cats;
            """)
        assert (df_replace == df_out).all(axis=None)

    def test_send_data_replace(self, db):
        df_replace = pd.DataFrame({'id': [1], 'name': ['Chill'], 'owner': ['Alex'], 'birth':['2018-03-03']}, index = [0])
        db.send_data(df_replace, 'cats', mode = 'replace')
        df_out = db.query("""
            SELECT * FROM cats;
            """)
        df_out.birth = df_out.birth.astype(str)
        assert (pd.DataFrame({'id': [1, 2, 3],
                              'name': ['Chill', 'Cookie', 'Charlie'],
                              'owner': ['Alex', 'Casey', 'River'],
                              'birth': ['2018-03-03', '2013-11-13', '2016-05-21']}) == df_out).all(axis=None)

    def test_send_data_update(self, db):
        df_replace = pd.DataFrame({'id': [1, 4], 'name': ['Chill', 'Pi'],
                                   'owner': ['Alex', 'Matt'], 'birth':['2018-03-03', '2019-08-05']}, index = [0, 1])
        db.send_data(df_replace, 'cats', mode='update')
        df_replace_small = pd.DataFrame({'id': [2], 'birth': ['2014-11-13']}, index=[0])
        db.send_data(df_replace_small, 'cats', mode='update')
        df_out = db.query("""
            SELECT * FROM cats;
            """)
        df_out.birth = df_out.birth.astype(str)
        assert (pd.DataFrame({'id': [1, 2, 3, 4],
                              'name': ['Chill', 'Cookie', 'Charlie', 'Pi'],
                              'owner': ['Alex', 'Casey', 'River', 'Matt'],
                              'birth': ['2018-03-03', '2014-11-13', '2016-05-21', '2019-08-05']}) == df_out).all(axis=None)

    def test_percentage_escape(self, db):
        df = db.send_query("SELECT * FROM cats WHERE owner LIKE 'Cas%';", escape_percentage=True)
        df.birth = df.birth.astype(str)
        assert (pd.DataFrame({'id': [2],
                              'name': ['Cookie'],
                              'owner': ['Casey'],
                              'birth': ['2013-11-13']}) == df).all(axis=None)
        with pytest.raises(ValueError):
            with pytest.warns(SyntaxWarning):
                df = db.send_query("SELECT * FROM cats WHERE owner LIKE 'Cas%';")

    def test_remove_comments(self, db):
        df = db.send_query("""
            SELECT name --, owner
            from /*{comm}*/ cats
            """, remove_comments=True)
        assert df.shape == (3, 1)
        assert df.name.to_list() == ['Sandy', 'Cookie', 'Charlie']
        with pytest.raises(Exception):
            df = db.send_query("""
                SELECT name --, owner
                from /*{comm}*/ cats
                """)
