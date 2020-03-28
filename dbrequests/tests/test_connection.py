import os

import pandas as pd
import pymysql
import pytest

sql_dir = os.path.join(os.getcwd(), 'dbrequests/tests/sql')
sql_dir = os.path.join(os.path.dirname(__file__), 'sql')

@pytest.mark.usefixtures('cats_con')
class TestConnection:
    def test_select(self, con):
        df = con.query("""
            SELECT * FROM cats;
            """, index_col='id')
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (3,3)
        assert (df.columns == ['name', 'owner', 'birth']).all()
        assert (df.name == ['Sandy', 'Cookie', 'Charlie']).all()

    def test_send_data_create(self, con):
        con.bulk_query("""
            DROP TABLE cats;
            """)
        df_in = pd.DataFrame({'name': ['Sandy', 'Cookie', 'Charlie'],
                           'owner': ['Lennon', 'Casey', 'River'],
                           'birth': ['2015-01-03', '2013-11-13', '2016-05-21']})
        con.send_data(df_in, 'cats')
        df_out = con.query("""
            SELECT * FROM cats;
            """)
        assert (df_in == df_out).all(axis=None)

    def test_send_data_insert(self, con):
        df_in = pd.DataFrame({'name': ['Sandy', 'Cookie', 'Charlie'],
                           'owner': ['Lennon', 'Casey', 'River'],
                           'birth': ['2015-01-03', '2013-11-13', '2016-05-21']})
        df_add = pd.DataFrame({'name': ['Chill'], 'owner': ['Alex'], 'birth':['2018-03-03']}, index=[3])
        con.send_data(df_add, 'cats', mode = 'insert')
        df_out = con.query("""
            SELECT * FROM cats;
            """)
        df_out.birth = df_out.birth.astype(str)
        assert (pd.concat([df_in, df_add]) == df_out[['name', 'owner', 'birth']]).all(axis=None)

    def test_send_data_truncate(self, con):
        df_replace = pd.DataFrame({'name': ['Chill'], 'owner': ['Alex'], 'birth':['2018-03-03']}, index=[0])
        con.send_data(df_replace, 'cats', mode = 'truncate')
        df_out = con.query("""
            SELECT * FROM cats;
            """)
        assert (df_replace == df_out).all(axis=None)

    def test_send_data_replace(self, con):
        df_replace = pd.DataFrame({'id': [1], 'name': ['Chill'], 'owner': ['Alex'], 'birth':['2018-03-03']}, index = [0])
        con.send_data(df_replace, 'cats', mode = 'replace')
        df_out = con.query("""
            SELECT * FROM cats;
            """)
        df_out.birth = df_out.birth.astype(str)
        assert (pd.DataFrame({'id': [1, 2, 3],
                              'name': ['Chill', 'Cookie', 'Charlie'],
                              'owner': ['Alex', 'Casey', 'River'],
                              'birth': ['2018-03-03', '2013-11-13', '2016-05-21']}) == df_out).all(axis=None)

    def test_send_data_update(self, con):
        df_replace = pd.DataFrame({'id': [1, 4], 'name': ['Chill', 'Pi'],
                                   'owner': ['Alex', 'Matt'], 'birth':['2018-03-03', '2019-08-05']}, index = [0, 1])
        con.send_data(df_replace, 'cats', mode='update')
        df_replace_small = pd.DataFrame({'id': [2], 'birth': ['2014-11-13']}, index=[0])
        con.send_data(df_replace_small, 'cats', mode='update')
        df_out = con.query("""
            SELECT * FROM cats;
            """)
        df_out.birth = df_out.birth.astype(str)
        assert (pd.DataFrame({'id': [1, 2, 3, 4],
                              'name': ['Chill', 'Cookie', 'Charlie', 'Pi'],
                              'owner': ['Alex', 'Casey', 'River', 'Matt'],
                              'birth': ['2018-03-03', '2014-11-13', '2016-05-21', '2019-08-05']}) == df_out).all(axis=None)
