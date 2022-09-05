import os

from fedprotocol.base.base_state_manager import BaseStateManager
from fedprotocol.tools.io import load_pkl, save_pkl
from fedprotocol.typing import FileDir, FilePath, StateDict, StateKey


class LocalStateManager(BaseStateManager):
    def __init__(self):
        self.home_dir = ''

    def exists(self, state_key: StateKey) -> bool:
        file_path = self._to_file_path(state_key)
        return os.path.exists(file_path)

    def _save(self, state_key: StateKey, state_dict: StateDict) -> None:
        file_path = self._to_file_path(state_key)
        save_pkl(state_dict, file_path)

    def _load(self, state_key: StateKey) -> StateDict:
        file_path = self._to_file_path(state_key)
        return load_pkl(file_path)

    def set_home_dir(self, home_dir: FileDir) -> 'LocalStateManager':
        self.home_dir = home_dir
        return self

    def _to_file_path(self, state_key: StateKey) -> FilePath:
        return os.path.join(self.home_dir, f"{state_key}.pkl")
