import typing
from typing import TYPE_CHECKING
from logging import Logger
if TYPE_CHECKING:  # 防止使用类型检查导致的循环导入
    # Pycharm会警告下面的导入是无用的，但是不要删除
    # 否则Pycharm的类型补全会无法使用
    # Pycharm版本需要升级到2021.3
    from fedprototype import BaseClient
    from fedprototype.envs.base_comm import BaseComm
    from fedprototype.envs.base_env import BaseEnv

RoleName = str
RoleNamePrefix = str
TrackName = str
SubRoleName = str
UpperRoleName = str
SubMessageSpaceName = str
Receiver = str
Sender = str
MessageName = str
MessageID = typing.Tuple[Sender, Receiver, MessageName]
MessageObj = typing.Any
Client = typing.TypeVar('Client', bound='BaseClient')
Comm = typing.TypeVar('Comm', bound='BaseComm')
Env = typing.TypeVar('Env', bound='BaseEnv')

__all__ = ['RoleName',
           'RoleNamePrefix',
           'TrackName',
           'SubRoleName',
           'UpperRoleName',
           'SubMessageSpaceName',
           'Receiver',
           'Sender',
           'MessageName',
           'MessageID',
           'MessageObj',
           'Client',
           'Comm',
           'Env',
           'Logger']
