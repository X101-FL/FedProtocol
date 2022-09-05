import typing as T
from typing import TYPE_CHECKING, DefaultDict


if TYPE_CHECKING:  # 防止使用类型检查导致的循环导入
    # Pycharm会警告下面的导入是无用的，但是不要删除
    # 否则Pycharm的类型补全会无法使用
    # Pycharm版本需要升级到2021.3
    from fedprotocol.base.base_worker import BaseWorker
    from fedprotocol.base.base_comm import BaseComm
    from fedprotocol.base.base_env import BaseEnv
    from fedprotocol.base.base_state_manager import BaseStateManager

ProtocolName = str
RoleName = str
RootRoleName = str
RoleNamePrefix = str
TrackPath = str
SubRoleName = str
UpperRoleName = str

Receiver = str
Sender = str

MessageName = str
MessageSpace = str
MessageID = T.Tuple[Sender, Receiver, MessageName]
MessageObj = T.Any
MessageBytes = bytes
MessageBuffer = T.DefaultDict[Receiver, T.List[T.Tuple[MessageName, MessageObj]]]

PartitionID = int
PartitionNum = int
StageID = int
TaskAttemptNum = int
JobID = str
StateDict = T.Dict[str, T.Any]

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

Worker = T.TypeVar('Worker', bound='BaseWorker')
Comm = T.TypeVar('Comm', bound='BaseComm')
Env = T.TypeVar('Env', bound='BaseEnv')

StateKey = str
StateManager = T.TypeVar('StateManager', bound='BaseStateManager')

FileDir = str  # 文件夹路径
FileName = str  # 单纯文件名，不带文件路径
FilePath = str  # 完整文件路径，文件夹路径 + 文件名

Host = str
Port = int
Url = str
