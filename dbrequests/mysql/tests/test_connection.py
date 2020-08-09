"""Testing connection bahaviours."""

import pandas as pd
import pytest
from dbrequests.mysql.tests.conftest import set_up_cats as reset
from sqlalchemy.exc import InternalError, OperationalError


@pytest.mark.usefixtures('db_connect_args')
class TestConnectionWithConnectArgs:
    """Test that passing on connect_args works for credentials."""

    def test_send_query(self, db_connect_args):
        """Test that we have working connection."""
        reset(db_connect_args)
        res = db_connect_args.send_query('select 1 as x')
        assert res.shape == (1, 1)

    def test_send_data(self, db_connect_args):
        """The connection has local_infile set to 0, so we expect an error."""
        df_add = pd.DataFrame({
            'name': ['Chill'],
            'owner': ['Alex'],
            'birth': ['2018-03-03']
        })

        reset(db_connect_args)
        with pytest.raises((InternalError, OperationalError)):
            db_connect_args.send_data(df_add, 'cats', mode='insert')
