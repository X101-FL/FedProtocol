import typing
from logging import Logger

RoleName = str
RoleNamePrefix = str
SubRoleName = str
UpperRoleName = str
Receiver = str
Sender = str
MessageName = str
MessageID = typing.Tuple[Sender, Receiver, MessageName]
MessageObj = typing.Any
Client = typing.TypeVar('Client', bound='fedprototype.BaseClient')
Comm = typing.TypeVar('Comm', bound='fedprototype.envs.base_comm.BaseComm')
Env = typing.TypeVar('Env', bound='fedprototype.envs.base_env.BaseEnv')

__all__ = ['RoleName',
           'RoleNamePrefix',
           'SubRoleName',
           'UpperRoleName',
           'Receiver',
           'Sender',
           'MessageName',
           'MessageID',
           'MessageObj',
           'Client',
           'Comm',
           'Env',
           'Logger']
