import unittest
import pytest
import mock

import sqlalchemy.orm.session

import dbcluster.sacontainer as ds

class SQLAlchemyEngineContainerTestCase(unittest.TestCase):
    def test__init__(self):
        out = ds.SQLAlchemyEngineContainer('sqlite:///:memory:')
        assert isinstance(out, ds.SQLAlchemyEngineContainer)

    def test_get_new_session(self):
        out = ds.SQLAlchemyEngineContainer('sqlite:///:memory:')
        session = out.get_new_session()
        assert isinstance(session, sqlalchemy.orm.session.Session)

    def test_get_controlled_session(self):
        out = ds.SQLAlchemyEngineContainer('sqlite:///:memory:')
        with out.get_controlled_session() as session:
            assert isinstance(session, sqlalchemy.orm.session.Session)

    def test_destroy(self):
        out = ds.SQLAlchemyEngineContainer('sqlite:///:memory:')
        out.destroy()
        assert True


class SQLAlchemyControlledSessionTestCase(unittest.TestCase):

    @mock.patch("sqlalchemy.orm.session.Session")
    def test__init__(self, mock_session):
        mock_session.close.return_value = True
        session = mock_session()
        out = ds.SQLAlchemyControlledSession(session)
        assert isinstance(out, ds.SQLAlchemyControlledSession)

    @mock.patch("sqlalchemy.orm.session.Session")
    def test__enter__(self, mock_session):
        mock_session.close.return_value = True
        session = mock_session()
        out = ds.SQLAlchemyControlledSession(session)
        with out as a:
            assert a == session

    @mock.patch("sqlalchemy.orm.session.Session")
    def test__exit__(self, mock_session):
        mock_session.close.return_value = True
        session = mock_session()
        out = ds.SQLAlchemyControlledSession(session)
        with out as a:
            assert a == session
        assert mock_session.is_called('close')
