from typing import Dict, List, Tuple, Optional, Generator

from fedprototype.base.base_comm import BaseComm
from fedprototype.typing import Comm, RoleName, RoleNamePrefix, Receiver, \
    Sender, MessageName, MessageObj, SubRoleName, UpperRoleName, MessageSpace


class CommRenameWrapper(BaseComm):
    def __init__(self,
                 role_name: SubRoleName,
                 comm: Comm,
                 rename_dict: Dict[SubRoleName, UpperRoleName]):
        super(CommRenameWrapper, self).__init__()
        self.role_name = role_name
        self.comm = comm
        self.rename_dict = rename_dict
        self.reversed_dict = {v: k for k, v in rename_dict.items()}
        self.other_role_name_set = (set(rename_dict.keys()) - {role_name})

    def _send(self, receiver: Receiver, message_name_obj_list: List[Tuple[MessageName, MessageObj]]) -> None:
        role_name = self.rename_dict[receiver]
        self.comm._send(role_name, message_name_obj_list)

    def receive(self, sender: Sender, message_name: MessageName, timeout: Optional[int] = None) -> MessageObj:
        sender = self.rename_dict[sender]
        return self.comm.receive(sender, message_name, timeout)

    def watch_(self,
               sender_message_name_tuple_list: List[Tuple[Sender, MessageName]],
               timeout: Optional[int] = None
               ) -> Generator[Tuple[Sender, MessageName, MessageObj], None, None]:
        sender_message_name_tuple_list = [(self.rename_dict[role], message) for role, message in
                                          sender_message_name_tuple_list]
        for sender, message_name, message_obj in self.comm.watch_(sender_message_name_tuple_list, timeout):
            yield self.reversed_dict[sender], message_name, message_obj

    def list_role_name(self, role_name_prefix: RoleNamePrefix) -> List[RoleName]:
        return [role_name for role_name in self.other_role_name_set if role_name.startswith(role_name_prefix)]

    def clear(self, sender: Sender = None, message_name: MessageName = None) -> None:
        if sender is not None:
            sender = self.rename_dict[sender]
        self.comm.clear(sender, message_name)

    def _sub_comm(self, message_space: MessageSpace) -> Comm:
        return CommRenameWrapper(self.role_name,
                                 self.comm._sub_comm(message_space),
                                 self.rename_dict)
