import typing
from typing import TYPE_CHECKING


if TYPE_CHECKING:  # 防止使用类型检查导致的循环导入
    # Pycharm会警告下面的导入是无用的，但是不要删除
    # 否则Pycharm的类型补全会无法使用
    # Pycharm版本需要升级到2021.3
    from fedprototype.base.base_client import BaseClient
    from fedprototype.base.base_comm import BaseComm
    from fedprototype.base.base_env import BaseEnv
    from fedprototype.base.base_state_saver import BaseStateSaver

ProtocolName = str
RoleName = str
RootRoleName = str
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
MessageBytes = bytes
PartitionID = int
PartitionNum = int
StageID = int
TaskAttemptNum = int
JobID = str
StateDict = typing.Dict[str, typing.Any]

##################################################################
# for pycharm
##################################################################

# Client = 'BaseClient'
# Comm = 'BaseComm'
# Env = 'BaseEnv'

# StateKey = str
# StateSaver = 'BaseStateSaver'
# LoggerFactory = 'BaseLoggerFactory'

##################################################################
# for vscode
##################################################################

Client = typing.TypeVar('Client', bound='BaseClient')
Comm = typing.TypeVar('Comm', bound='BaseComm')
Env = typing.TypeVar('Env', bound='BaseEnv')

StateKey = str
StateSaver = typing.TypeVar('StateSaver', bound='BaseStateSaver')

FileDir = str  # 文件夹路径
FileName = str  # 单纯文件名，不带文件路径
FilePath = str  # 完整文件路径，文件夹路径 + 文件名

Host = str
Port = int
Url = str
