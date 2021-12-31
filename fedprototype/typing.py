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
    from fedprototype.tools.log import BaseLoggerFactory

RoleName = str
RoleNamePrefix = str
TrackPath = str
SubRoleName = str
UpperRoleName = str
MessageSpace = str
Receiver = str
Sender = str
MessageName = str
MessageID = typing.Tuple[Sender, Receiver, MessageName]
MessageObj = typing.Any
Client = typing.TypeVar('Client', bound='BaseClient')
Comm = typing.TypeVar('Comm', bound='BaseComm')
Env = typing.TypeVar('Env', bound='BaseEnv')

FileDir = str  # 文件夹路径
FileName = str  # 单纯文件名，不带文件路径
FilePath = str  # 完整文件路径，文件夹路径 + 文件名

LoggerFactory = typing.TypeVar('LoggerFactory', bound='BaseLoggerFactory')
__all__ = ['RoleName',
           'RoleNamePrefix',
           'TrackPath',
           'SubRoleName',
           'UpperRoleName',
           'MessageSpace',
           'Receiver',
           'Sender',
           'MessageName',
           'MessageID',
           'MessageObj',
           'Client',
           'Comm',
           'Env',
           'Logger',
           'FileDir',
           'FileName',
           'FilePath',
           'LoggerFactory']
