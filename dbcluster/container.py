import abc

class IEngineContainer:
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        pass

    @abc.abstractmethod
    def get_new_session(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_controlled_session(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()
