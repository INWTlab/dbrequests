from dbrequests import Query
import os
import pytest

sql_dir = os.path.join(os.getcwd(), 'dbrequests/tests/sql')
sql_dir = os.path.join(os.path.dirname(__file__), 'sql')

class TestQuery:
    def test_direct_query(self):
        text = 'select * from table;'
        query = Query(text)
        assert query.sql_dir == ''
        assert query.path is None
        assert query.text == text

    def test_file(self):
        query = Query('select', sql_dir=sql_dir)
        assert query.sql_dir == sql_dir
        assert query.path == os.path.join(sql_dir, 'select.sql')
        assert query.text == 'SELECT * FROM cats;\n'

    def test_file_dot(self):
        query = Query('select.sql', sql_dir=sql_dir)
        assert query.sql_dir == sql_dir
        assert query.path == os.path.join(sql_dir, 'select.sql')
        assert query.text == 'SELECT * FROM cats;\n'

    def test_file_parameters(self):
        query = Query('select_param', sql_dir=sql_dir, col1='hi', col2='ho')
        assert query.sql_dir == sql_dir
        assert query.path == os.path.join(sql_dir, 'select_param.sql')
        assert query.text == 'SELECT hi, ho FROM cats;\n'

    def test_file_not_found(self):
        with pytest.raises(IOError) as e:
            query = Query('select_asdf', sql_dir)
        assert str(e.value) == "File '{}' not found!".format(sql_dir + '/select_asdf.sql')

    def test_percentage_warning(self):
        with pytest.warns(SyntaxWarning):
            query = Query("like a%")
            assert query.text == "like a%"

    def test_percentage_escape(self):
        query = Query("like a%", escape_percentage=True)
        assert query.text == "like a%%"

    def test_remove_comments(self):
        singleline = '''x --comment'''
        assert Query(singleline, remove_comments=True).text == 'x '
        singleline_break = '''
x 
--comment
'''
        assert Query(singleline_break, remove_comments=True).text == '\nx \n\n'
        multiline = '''x/*y*/ x/*y*/'''
        assert Query(multiline, remove_comments=True).text == 'x  x '
