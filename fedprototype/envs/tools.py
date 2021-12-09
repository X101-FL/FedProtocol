import typing as T

from fedprototype.envs.base_comm import BaseComm


class CommRenameWrapper(BaseComm):
    def __init__(self, role_name: str, comm: BaseComm, rename_dict: T.Dict[str, str]):
        super(CommRenameWrapper, self).__init__()
        self.role_name = role_name
        self.comm = comm
        self.rename_dict = rename_dict
        self.reversed_dict: T.Dict[str, str] = {v: k for k, v in rename_dict.items()}
        self.other_role_name_set: T.Set[str] = (set(rename_dict.keys()) - {role_name})

    def _send(
        self, receiver: str, message_name_obj_list: T.List[T.Tuple[str, T.Any]]
    ) -> None:
        role_name = self.rename_dict[receiver]
        self.comm._send(role_name, message_name_obj_list)

    def receive(
        self, sender: str, message_name: str, timeout: T.Optional[int] = None
    ) -> T.Any:
        role_name = self.rename_dict[sender]
        return self.comm.receive(role_name, message_name, timeout)

    def watch_(
        self,
        sender_message_name_tuple_list: T.List[T.Tuple[str, str]],
        timeout: T.Optional[int] = None,
    ) -> T.Generator[T.Tuple[str, str, T.Any], None, None]:
        sender_message_name_tuple_list = [
            (self.rename_dict[role], message)
            for role, message in sender_message_name_tuple_list
        ]
        for sender, message_name, data in self.comm.watch_(
            sender_message_name_tuple_list, timeout
        ):
            yield self.reversed_dict[sender], message_name, data

    def get_role_name_list(self, role_name_prefix: str) -> T.List[str]:
        return [
            role_name
            for role_name in self.other_role_name_set
            if role_name.startswith(role_name_prefix)
        ]
