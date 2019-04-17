"""
Database Container

author: Kris Hardy <kris@abqsoft.com>
"""

import dbcluster.container as c

class SQLAlchemyEngineContainer(c.IEngineContainer):
    """
    Database engine wrapper
    """

    def __init__(self, uri, slave=False, **kwargs):
        import sqlalchemy
        import sqlalchemy.orm
        self.slave = slave
        self.engine = sqlalchemy.create_engine(uri, **kwargs)
        self.sessionmaker = sqlalchemy.orm.sessionmaker(self.engine)

    def get_new_session(self):
        """
        Creates a new SQLAlchemy session
        :return: sqlalchemy.orm.Session
        """
        session = self.sessionmaker()
        session.slave = self.slave
        return session

    def get_controlled_session(self):
        return SQLAlchemyControlledSession(self.get_new_session())

    def destroy(self):
        self.engine.dispose()


class SQLAlchemyControlledSession(object):
    """
    This creates a controlled database session which self-closes when it goes
    out of scope.
    """
    def __init__(self, session):
        self._session = session

    def __enter__(self):
        return self._session

    def __exit__(self, type_, value, traceback):
        self._session.close()


