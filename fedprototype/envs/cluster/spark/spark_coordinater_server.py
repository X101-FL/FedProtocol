from fedprototype.envs.cluster.spark.typing import JobID, PartitionID, StageID, TaskAttemptID
import pickle
import time
from collections import defaultdict, namedtuple
from queue import Empty
from typing import Callable, Dict, List, Tuple, Set
from multiprocessing import Process
from fedprototype.tools.func import get_free_ip_port
import numpy as np
import requests
import uvicorn
from fastapi import Body, FastAPI, File, Form
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from requests import Response as ReqResponse
from requests.exceptions import ConnectionError

from fedprototype.base.base_logger_factory import BaseLoggerFactory
from fedprototype.envs.cluster.tcp.tcp_message_hub import MessageHub
from fedprototype.tools.log import LocalLoggerFactory
from fedprototype.typing import (
    Host,
    MessageBytes,
    MessageName,
    MessageSpace,
    Port,
    Receiver,
    RoleName,
    RootRoleName,
    Sender,
    Url,
)

SUCCESS_CODE = 200
SUCCESS_RESPONSE = PlainTextResponse(content='OK', status_code=SUCCESS_CODE)


class FedRole:
    def __init__(self, server_url: Url, stage_id: StageID, task_attempt_id: TaskAttemptID) -> None:
        self.server_url = server_url
        self.stage_id = stage_id
        self.task_attempt_id = task_attempt_id


class FedGroupManager:
    def __init__(self, root_role_name_set: Set[RootRoleName]) -> None:
        self.root_role_name_set = root_role_name_set
        self.registed_role_dict: Dict[RootRoleName, FedRole] = {}
        self.is_started = False

    def register_role(self,
                      root_role_name: RootRoleName,
                      server_url: Url,
                      stage_id: StageID,
                      task_attempt_id: TaskAttemptID
                      ) -> None:
        self.registed_role_dict[root_role_name] = FedRole(server_url=server_url,
                                                          stage_id=stage_id,
                                                          task_attempt_id=task_attempt_id)

    def is_all_registed(self):
        return self.root_role_name_set == set(self.registed_role_dict.keys())

    def mark_started(self):
        self.is_started = True


class JobManager:
    def __init__(self) -> None:
        self.fed_group_dict: Dict[PartitionID, FedGroupManager] = {}

    def get_or_set_fed_group(self,
                             partition_id: PartitionID,
                             root_role_name_set: Set[RootRoleName]
                             ) -> FedGroupManager:
        if partition_id not in self.fed_group_dict:
            self.fed_group_dict[partition_id] = FedGroupManager(root_role_name_set)
        fed_group = self.fed_group_dict[partition_id]

        if fed_group.root_role_name_set != root_role_name_set:
            raise HTTPException(detail=f"conflict between role name set {fed_group.root_role_name_set} and {root_role_name_set}",
                                status_code=428)

        return fed_group


def _start_server(host: Host, port: Port):
    job_manager_dict: Dict[JobID, JobManager] = defaultdict(JobManager)

    app = FastAPI()

    @app.post("/register_task")
    def register_task(job_id: JobID = Body(...),
                      stage_id: StageID = Body(...),
                      partition_id: PartitionID = Body(...),
                      task_attempt_id: TaskAttemptID = Body(...),
                      root_role_name: RootRoleName = Body(...),
                      root_role_name_set: Set[RootRoleName] = Body(...),
                      task_server_url: Url = Body(...)):
        job_manager = job_manager_dict[job_id]
        group_manager = job_manager.get_or_set_fed_group(partition_id, root_role_name_set)

        if group_manager.is_started:
            _fail_group_manager(group_manager)

        group_manager.register_role(root_role_name=root_role_name,
                                    server_url=task_server_url,
                                    stage_id=stage_id,
                                    task_attempt_id=task_attempt_id)
        if group_manager.is_all_registed():
            _start_group_manager(group_manager)

        return SUCCESS_RESPONSE

    def _start_group_manager(group_manager: FedGroupManager) -> None:
        root_role_name_url_dict = {root_role_name: fed_role.server_url
                                   for root_role_name, fed_role in group_manager.registed_role_dict.items()}
        for root_role_name,server_url in root_role_name_url_dict.items():
            _post(url=f"{server_url}/")
        

    @app.post("/test")
    def test():
        return SUCCESS_RESPONSE

    def _post(**kwargs) -> ReqResponse:
        try:
            _res = requests.post(**kwargs)
            if _res.status_code != SUCCESS_CODE:
                raise HTTPException(detail=f"internal post error : {_res.text}", status_code=500)
        except ConnectionError as e:
            raise HTTPException(detail=f"internal post error : {e}", status_code=500)

    def _try_func(func: Callable, detail: str, status_code: int, **kwargs) -> None:
        try:
            func(**kwargs)
        except HTTPException as e:
            detail = f"{detail}. {e.detail}"
            raise HTTPException(detail=detail, status_code=status_code)

    uvicorn.run(app=app, host=host, port=port, debug=True,
                access_log=True, log_level='DEBUG', use_colors=True)


if __name__ == "__main__":
    _start_server('127.0.0.1', 6609)
