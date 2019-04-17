import unittest
import pytest

import dbcluster as d

# Python 2/3 support
from builtins import range

class MasterSlaveManagerTestCase(unittest.TestCase):
    def test__init__(self):
        out = d.MasterSlaveManager()
        assert isinstance(out, d.MasterSlaveManager)

    def test_append_master(self):
        out = d.MasterSlaveManager()
        out.append_master(
                "sqlite:///:memory:",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        assert len(out._master_engines) == 1

    def test_append_master_unknownorm(self):
        out = d.MasterSlaveManager()
        with pytest.raises(ValueError):
            out.append_master(
                    "sqlite:///:memory:",
                    -1
                    )

    def test_append_master_toomany(self):
        out = d.MasterSlaveManager()
        out.append_master(
                "sqlite:///:memory:",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        with pytest.raises(d.DBClusterException):
            out.append_master(
                    "sqlite:///:memory:",
                    d.MasterSlaveManager.ORM_SQLALCHEMY
                    )

    def test_append_master2(self):
        out = d.MasterSlaveManager(2)
        out.append_master(
                "sqlite:///:memory:",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        out.append_master(
                "sqlite:///:memory:",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        assert len(out._master_engines) == 2

    def test_append_slave(self):
        out = d.MasterSlaveManager()
        out.append_slave(
                "sqlite:///:memory:",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        assert len(out._slave_engines) == 1

    def test_append_slave_unknownorm(self):
        out = d.MasterSlaveManager()
        with pytest.raises(ValueError):
            out.append_slave(
                    "sqlite:///:memory:",
                    -1
                    )

    def test_dispose_all(self):
        out = d.MasterSlaveManager()
        out.append_master(
                "sqlite:///:memory:",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        out.append_slave(
                "sqlite:///:memory:",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        out.dispose_all()
        assert len(out._master_engines) == 0
        assert len(out._slave_engines) == 0

    def test_dispose_masters(self):
        out = d.MasterSlaveManager()
        out.append_master(
                "sqlite:///:memory:",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        out.append_slave(
                "sqlite:///:memory:",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        out.dispose_masters()
        assert len(out._master_engines) == 0
        assert len(out._slave_engines) == 1

    def test_dispose_slaves(self):
        out = d.MasterSlaveManager()
        out.append_master(
                "sqlite:///:memory:",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        out.append_slave(
                "sqlite:///:memory:",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        out.dispose_slaves()
        assert len(out._master_engines) == 1
        assert len(out._slave_engines) == 0

    def test_get_random_master_engine(self):
        out = d.MasterSlaveManager(2)
        out.append_master(
                "sqlite:///:memory:/1",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        out.append_master(
                "sqlite:///:memory:/2",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        engines = []
        for _ in range(20):
            engine = out.get_random_master_engine()
            if engine not in engines:
                engines.append(engine)
        assert len(engines) == 2

    def test_get_random_master_engine_none(self):
        out = d.MasterSlaveManager()
        with pytest.raises(d.DBClusterException):
            out.get_random_master_engine()

    def test_get_random_slave_engine(self):
        out = d.MasterSlaveManager()
        out.append_slave(
                "sqlite:///:memory:/1",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        out.append_slave(
                "sqlite:///:memory:/2",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        engines = []
        for _ in range(20):
            engine = out.get_random_slave_engine()
            if engine not in engines:
                engines.append(engine)
        assert len(engines) == 2

    def test_master_session_ctx(self):
        out = d.MasterSlaveManager()
        out.append_master(
                "sqlite:///:memory:/1",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        with out.master_session_ctx() as db_session:
            assert True

    def test_master_session_ctx_none(self):
        out = d.MasterSlaveManager()
        with pytest.raises(d.DBClusterException):
            with out.master_session_ctx() as db_session:
                assert True

    def test_master_session_ctx_idx(self):
        out = d.MasterSlaveManager(2)
        out.append_master(
                "sqlite:///:memory:/1",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        out.append_master(
                "sqlite:///:memory:/2",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        with out.master_session_ctx(0) as db_session:
            assert str(db_session.get_bind().url) == "sqlite:///:memory:/1"
        with out.master_session_ctx(1) as db_session:
            assert str(db_session.get_bind().url) == "sqlite:///:memory:/2"

    def test_slave_session_ctx(self):
        out = d.MasterSlaveManager()
        out.append_slave(
                "sqlite:///:memory:/1",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        with out.slave_session_ctx() as db_session:
            assert True

    def test_slave_session_ctx_noslave(self):
        out = d.MasterSlaveManager()
        out.append_master(
                "sqlite:///:memory:/1",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        with out.slave_session_ctx() as db_session:
            assert True

    def test_slave_session_ctx_idx(self):
        out = d.MasterSlaveManager()
        out.append_slave(
                "sqlite:///:memory:/1",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        out.append_slave(
                "sqlite:///:memory:/2",
                d.MasterSlaveManager.ORM_SQLALCHEMY
                )
        with out.slave_session_ctx(0) as db_session:
            assert str(db_session.get_bind().url) == "sqlite:///:memory:/1"
        with out.slave_session_ctx(1) as db_session:
            assert str(db_session.get_bind().url) == "sqlite:///:memory:/2"

