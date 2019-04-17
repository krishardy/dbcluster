dbcluster: Database Cluster Session Manager

Kris Hardy <kris@abqsoft.com>

NOTE: This code is in early beta and is under active development.  Please use carefully until the code stabalizes into version 1.0 (target: May 2019)

This database engine manager manages master/slave database connections and
session management.  This is useful for applications which need to manage sqlalchemy database engines and sessionmakers for master/slave database clusters.

Example usage:

```python
import sqlalchemy
from dbcluster import MasterSlaveManager
manager = MasterSlaveManager()

# Set up the master(s)
manager.append_master(
    sqlalchemy.create_engine('postgresql://127.0.0.1:5432'),
    orm=MasterSlaveManager.ORM_SQLALCHEMY
)

# Set up slaves
manager.append_slave(
    sqlalchemy.create_engine('postgresql://127.0.0.1:5433'),
    orm=MasterSlaveManager.ORM_SQLALCHEMY
)

manager.append_slave(
    sqlalchemy.create_engine('postgresql://127.0.0.1:5435'),
    orm=MasterSlaveManager.ORM_SQLALCHEMY
)

# Use the database sessions
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

