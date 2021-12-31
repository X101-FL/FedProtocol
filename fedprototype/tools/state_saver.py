import os

from fedprototype.base.base_state_saver import BaseStateSaver
from fedprototype.tools.io import save_pkl, load_pkl
from fedprototype.typing import FilePath, FileDir, StateDict


class LocalStateSaver(BaseStateSaver):
    def __init__(self):
        self.home_dir = ''

    def exists(self, file_path: FilePath) -> bool:
        file_path = self._abs_file_path(file_path)
        return os.path.exists(file_path)

    def _save(self, file_path: FilePath, state_dict: StateDict) -> None:
        file_path = self._abs_file_path(file_path)
        save_pkl(state_dict, file_path)

    def _load(self, file_path: FilePath) -> StateDict:
        file_path = self._abs_file_path(file_path)
        return load_pkl(file_path)

    def _abs_file_path(self, file_path: FilePath):
        return os.path.join(self.home_dir, file_path)

    def set_home_dir(self, home_dir: FileDir):
        self.home_dir = home_dir
        return self
