from fedprotocol.base.base_worker import BaseWorker
from fedprotocol.base.base_state_manager import BaseStateManager
from fedprotocol.envs import Env


def set_env(name: str = 'Local'):
    """ Get environment by name """
    return Env.set(name)


__all__ = ['BaseWorker', 'BaseStateManager', 'Env', 'set_env']
