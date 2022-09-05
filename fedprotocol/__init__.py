from fedprotocol.base.base_client import BaseClient
from fedprotocol.base.base_state_saver import BaseStateSaver
from fedprotocol.envs import Env


def set_env(name: str = 'Local'):
    """ Get environment by name """
    return Env.set(name)


__all__ = ['BaseClient', 'BaseStateSaver', 'Env', 'set_env']
