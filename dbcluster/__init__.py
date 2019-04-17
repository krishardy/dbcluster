"""
Database Cluster Session Manager

author: Kris Hardy <kris@abqsoft.com>

This database engine manager manages master/slave database connections and
session management.

Example usage:

```python
import sqlalchemy
from dbcluster import MasterSlaveManager
manager = MasterSlaveManager()

manager.append_master(
    sqlalchemy.create_engine('postgresql://127.0.0.1:5432'),
    orm=MasterSlaveManager.ORM_SQLALCHEMY
)

manager.append_slave(
    sqlalchemy.create_engine('postgresql://127.0.0.1:5433'),
    orm=MasterSlaveManager.ORM_SQLALCHEMY
)

manager.append_slave(
    sqlalchemy.create_engine('postgresql://127.0.0.1:5435'),
    orm=MasterSlaveManager.ORM_SQLALCHEMY
)

with manager.master_session_ctx() as db_session:
    # Do something with the session ...
    db_session.commit()

with manager.slave_session_ctx() as db_session:
    # Returns a random slave session
    # Do something with the session ...
    db_session.commit()

with manager.slave_session_ctx(0) as db_session:
    # Returns a session to the first slave
    # Do something with the session ...
    db_session.commit()
```
"""
__VERSION__ = "0.1"

import random
import logging

import dbcluster.sacontainer as ds

logger = logging.getLogger(__name__)


class MasterSlaveManager(object):
    ORM_SQLALCHEMY = 0

    def __init__(self, master_limit=1):
        """
        ctor
        """
        self._master_limit = master_limit
        self._master_engines = []
        self._slave_engines = []

    def append_master(self, uri, orm=ORM_SQLALCHEMY, **kwargs):
        if len(self._master_engines) >= self._master_limit:
            raise DBClusterException("The master engine has already "
                    "configured.  Cowardly refusing to replace it.")
        if orm == self.ORM_SQLALCHEMY:
            self._master_engines.append((
                ds.SQLAlchemyEngineContainer(uri, **kwargs),
                orm))
        else:
            raise ValueError("Unknown orm type: {}".format(orm))

    def append_slave(self, uri, orm=ORM_SQLALCHEMY, **kwargs):
        if orm == self.ORM_SQLALCHEMY:
            self._slave_engines.append((
                ds.SQLAlchemyEngineContainer(uri, slave=True, **kwargs),
                orm))
        else:
            raise ValueError("Unknown orm type: {}".format(orm))

    def dispose_all(self):
        self.dispose_slaves()
        self.dispose_masters()

    def dispose_masters(self):
        while len(self._master_engines) > 0:
            engine, orm = self._master_engines.pop()
            if orm == self.ORM_SQLALCHEMY:
                engine.engine.dispose()

    def dispose_slaves(self):
        while len(self._slave_engines) > 0:
            engine, orm = self._slave_engines.pop()
            if orm == self.ORM_SQLALCHEMY:
                engine.engine.dispose()

    def get_random_master_engine(self):
        if len(self._master_engines) == 0:
            raise DBClusterException("No master engines are available.")
        else:
            return random.choice(self._master_engines)[0]

    def get_random_slave_engine(self):
        if len(self._slave_engines) == 0:
            logger.warning("No slave engines are available.  "
                    "Using master engine instead.")
            return self.get_random_master_engine()
        else:
            return random.choice(self._slave_engines)[0]

    def master_session_ctx(self, idx=None):
        if len(self._master_engines) == 0:
            raise DBClusterException("No master engines have been configured.")
        if idx is not None and 0 <= idx < len(self._master_engines):
            # Return the first engine
            return self._master_engines[idx][0].get_controlled_session()
        else:
            return self.get_random_master_engine().get_controlled_session()

    def slave_session_ctx(self, idx=None):
        if idx is not None and 0 <= idx < len(self._slave_engines):
            return self._slave_engines[idx][0].get_controlled_session()
        else:
            return self.get_random_slave_engine().get_controlled_session()


class DBClusterException(Exception):
    pass
