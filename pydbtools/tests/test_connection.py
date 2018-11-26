from pydbtools import Connection
from sqlalchemy import create_engine
import pymysql
import pandas as pd
import os

creds = {
    'user': 'testuser',
    'password': 'test',
    'host': 'localhost',
    'db': 'test'
}
engine = create_engine("mysql+pymysql://{}:{}@{}/{}".format(creds['user'], creds['password'], creds['host'], creds['db']))

sql_dir = os.path.join(os.getcwd(), 'pydbtools/tests/sql')
sql_dir = os.path.join(os.path.dirname(__file__), 'sql')

class TestConnection:
    def test_create(self):
        con = Connection(engine.connect())
        con.bulk_query("""
            CREATE TABLE cats
                (
                  id              INT unsigned NOT NULL AUTO_INCREMENT, # Unique ID for the record
                  name            VARCHAR(150) NOT NULL,                # Name of the cat
                  owner           VARCHAR(150) NOT NULL,                # Owner of the cat
                  birth           DATE NOT NULL,                        # Birthday of the cat
                  PRIMARY KEY     (id)                                  # Make the id the primary key
                );
            """)
        df = con.query("""
            SELECT * FROM cats;
            """)
        con.bulk_query("""
            DROP TABLE cats;
            """)
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (0,4)
        assert (df.columns == ['id', 'name', 'owner', 'birth']).all()

    def test_create_insert(self):
        con = Connection(engine.connect())
        con.bulk_query("""
            CREATE TABLE cats
                (
                  id              INT unsigned NOT NULL AUTO_INCREMENT, # Unique ID for the record
                  name            VARCHAR(150) NOT NULL,                # Name of the cat
                  owner           VARCHAR(150) NOT NULL,                # Owner of the cat
                  birth           DATE NOT NULL,                        # Birthday of the cat
                  PRIMARY KEY     (id)                                  # Make the id the primary key
                );
            """)
        con.bulk_query("""
            INSERT INTO cats ( name, owner, birth) VALUES
              ( 'Sandy', 'Lennon', '2015-01-03' ),
              ( 'Cookie', 'Casey', '2013-11-13' ),
              ( 'Charlie', 'River', '2016-05-21' );
            """)
        df = con.query("""
            SELECT * FROM cats;
            """, index_col='id')
        con.bulk_query("""
            DROP TABLE cats;
            """)
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (3,3)
        assert (df.columns == ['name', 'owner', 'birth']).all()
        assert (df.name == ['Sandy', 'Cookie', 'Charlie']).all()

    def test_create_file(self):
        con = Connection(engine.connect())
        con.bulk_query_file(os.path.join(sql_dir, 'create.sql'))
        df = con.query_file(os.path.join(sql_dir, 'select.sql'))
        con.bulk_query_file(os.path.join(sql_dir, 'drop.sql'))
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (0,4)
        assert (df.columns == ['id', 'name', 'owner', 'birth']).all()

    def test_create_insert_file_param(self):
        con = Connection(engine.connect())
        con.bulk_query_file(os.path.join(sql_dir, 'create.sql'))
        con.bulk_query_file(os.path.join(sql_dir, 'insert_param.sql'), table='cats')
        df = con.query_file(os.path.join(sql_dir, 'select_param.sql'),
                            col1='name', col2='id', index_col='id')
        con.bulk_query_file(os.path.join(sql_dir, 'drop.sql'))
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (3,1)
        assert (df.columns == ['name']).all()
        assert (df.name == ['Sandy', 'Cookie', 'Charlie']).all()

    def test_send_data_create(self):
        con = Connection(engine.connect())
        df_in = pd.DataFrame({'name': ['Sandy', 'Cookie', 'Charlie'],
                           'owner': ['Lennon', 'Casey', 'River'],
                           'birth': ['2015-01-03', '2013-11-13', '2016-05-21']})
        con.send_data(df_in, 'cats')
        df_out = con.query("""
            SELECT * FROM cats;
            """)
        con.bulk_query("""
            DROP TABLE cats;
            """)
        assert (df_in == df_out).all(axis=None)

    def test_send_data_insert(self):
        con = Connection(engine.connect())
        df_in = pd.DataFrame({'name': ['Sandy', 'Cookie', 'Charlie'],
                           'owner': ['Lennon', 'Casey', 'River'],
                           'birth': ['2015-01-03', '2013-11-13', '2016-05-21']})
        con.send_data(df_in, 'cats')
        df_add = pd.DataFrame({'name': ['Chill'], 'owner': ['Alex'], 'birth':['2018-03-03']}, index=[3])
        con.send_data(df_add, 'cats', mode = 'insert')
        df_out = con.query("""
            SELECT * FROM cats;
            """)
        con.bulk_query("""
            DROP TABLE cats;
            """)
        assert (pd.concat([df_in, df_add]) == df_out).all(axis=None)

    def test_send_data_truncate(self):
        con = Connection(engine.connect())
        df_in = pd.DataFrame({'name': ['Sandy', 'Cookie', 'Charlie'],
                           'owner': ['Lennon', 'Casey', 'River'],
                           'birth': ['2015-01-03', '2013-11-13', '2016-05-21']})
        con.send_data(df_in, 'cats')
        df_replace = pd.DataFrame({'name': ['Chill'], 'owner': ['Alex'], 'birth':['2018-03-03']}, index=[0])
        con.send_data(df_replace, 'cats', mode = 'truncate')
        df_out = con.query("""
            SELECT * FROM cats;
            """)
        con.bulk_query("""
            DROP TABLE cats;
            """)
        assert (df_replace == df_out).all(axis=None)
