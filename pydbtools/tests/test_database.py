from pydbtools import Database
import pandas as pd
import pymysql
import os

creds = {
    'user': 'testuser',
    'password': 'test',
    'host': 'localhost',
    'db': 'test'
}
url = ("mysql+pymysql://{}:{}@{}/{}".format(creds['user'], creds['password'], creds['host'], creds['db']))
sql_dir = os.path.join(os.path.dirname(__file__), 'sql')

sql_dir = os.path.join(os.getcwd(), 'pydbtools/tests/sql')

class TestDatabase:
    def test_send_query_create(self):
        db = Database(creds=creds)
        db.send_bulk_query("""
            CREATE TABLE cats
                (
                  id              INT unsigned NOT NULL AUTO_INCREMENT, # Unique ID for the record
                  name            VARCHAR(150) NOT NULL,                # Name of the cat
                  owner           VARCHAR(150) NOT NULL,                # Owner of the cat
                  birth           DATE NOT NULL,                        # Birthday of the cat
                  PRIMARY KEY     (id)                                  # Make the id the primary key
                );
            """)
        df = db.send_query("""
            SELECT * FROM cats;
            """)
        db.send_bulk_query("""
            DROP TABLE cats;
            """)
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (0,4)
        assert (df.columns == ['id', 'name', 'owner', 'birth']).all()

    def test_create_insert(self):
        db = Database(url)
        db.send_bulk_query("""
            CREATE TABLE cats
                (
                  id              INT unsigned NOT NULL AUTO_INCREMENT, # Unique ID for the record
                  name            VARCHAR(150) NOT NULL,                # Name of the cat
                  owner           VARCHAR(150) NOT NULL,                # Owner of the cat
                  birth           DATE NOT NULL,                        # Birthday of the cat
                  PRIMARY KEY     (id)                                  # Make the id the primary key
                );
            """)
        db.send_bulk_query("""
            INSERT INTO cats ( name, owner, birth) VALUES
              ( 'Sandy', 'Lennon', '2015-01-03' ),
              ( 'Cookie', 'Casey', '2013-11-13' ),
              ( 'Charlie', 'River', '2016-05-21' );
            """)
        df = db.send_query("""
            SELECT * FROM cats;
            """, index_col='id')
        db.bulk_query("""
            DROP TABLE cats;
            """)
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (3,3)
        assert (df.columns == ['name', 'owner', 'birth']).all()
        assert (df.name == ['Sandy', 'Cookie', 'Charlie']).all()

    def test_create_file(self):
        db = Database(url)
        db.send_bulk_query(os.path.join(sql_dir, 'create.sql'))
        df = db.send_query(os.path.join(sql_dir, 'select.sql'))
        db.send_bulk_query(os.path.join(sql_dir, 'drop.sql'))
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (0,4)
        assert (df.columns == ['id', 'name', 'owner', 'birth']).all()

    def test_create_insert_file_param(self):
        db = Database(creds=creds)
        db.send_bulk_query(os.path.join(sql_dir, 'create.sql'))
        db.send_bulk_query(os.path.join(sql_dir, 'insert.sql'))
        df = db.send_query(os.path.join(sql_dir, 'select_param.sql'),
                            col1='name', col2='id', index_col='id')
        db.send_bulk_query(os.path.join(sql_dir, 'drop.sql'))
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (3,1)
        assert (df.columns == ['name']).all()
        assert (df.name == ['Sandy', 'Cookie', 'Charlie']).all()

    def test_create_insert_file_param_dir(self):
        db = Database(creds=creds, sql_dir=sql_dir)
        db.send_bulk_query('create')
        db.send_bulk_query('insert.sql')
        df = db.send_query('select_param',
                            col1='name', col2='id', index_col='id')
        db.send_bulk_query('drop')
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (3,1)
        assert (df.columns == ['name']).all()
        assert (df.name == ['Sandy', 'Cookie', 'Charlie']).all()
