import logging
import time
from datetime import datetime, timedelta
from threading import Thread
from typing import Callable, Dict, Set

import requests
import uvicorn
from fastapi import Body, FastAPI
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
from requests import Response as ReqResponse
from requests.exceptions import ConnectionError

from fedprototype.typing import (
    Host,
    JobID,
    PartitionID,
    PartitionNum,
    Port,
    RootRoleName,
    StageID,
    TaskAttemptID,
    Url,
)

SUCCESS_CODE = 200
SUCCESS_RESPONSE = PlainTextResponse(content='OK', status_code=SUCCESS_CODE)
DRIVER_HEARTBEAT_EXPIRED_TIMEOUT = timedelta(seconds=40)
STATE_CHECKING_INTERVAL = 10


class Task:
    def __init__(self,
                 root_role_name: RootRoleName,
                 server_url: Url,
                 stage_id: StageID,
                 task_attempt_id: TaskAttemptID) -> None:
        self.root_role_name = root_role_name
        self.server_url = server_url
        self.stage_id = stage_id
        self.task_attempt_id = task_attempt_id


class TaskGroup:
    NORMAL_STATE = "normal"
    FAILED_STATE = "failed"

    def __init__(self) -> None:
        self.task_dict: Dict[RootRoleName, Task] = {}

    def register_role(self,
                      root_role_name: RootRoleName,
                      server_url: Url,
                      stage_id: StageID,
                      task_attempt_id: TaskAttemptID
                      ) -> None:
        self.task_dict[root_role_name] = Task(root_role_name=root_role_name,
                                              server_url=server_url,
                                              stage_id=stage_id,
                                              task_attempt_id=task_attempt_id)


class Driver:
    def __init__(self,
                 root_role_name: RootRoleName,
                 ) -> None:
        self.root_role_name: RootRoleName = root_role_name
        self.last_heartbeat: datetime = None
        self.refresh_heartbeat()

    def refresh_heartbeat(self) -> None:
        self.last_heartbeat: datetime = datetime.now()

    def is_heartbeat_expired(self) -> bool:
        return (datetime.now() - self.last_heartbeat) > DRIVER_HEARTBEAT_EXPIRED_TIMEOUT


class Job:
    NORMAL_STATE = "normal"
    FAILED_STATE = "failed"

    job_dict: Dict[JobID, 'Job'] = {}

    def __init__(self,
                 job_id: JobID,
                 root_role_name_set: Set[RootRoleName],
                 partition_num: PartitionNum
                 ) -> None:
        self.job_id = job_id
        self.root_role_name_set = root_role_name_set
        self.partition_num = partition_num
        self.job_state = Job.NORMAL_STATE
        self.driver_dict: Dict[RootRoleName, Driver] = {}
        self.task_group_dict: Dict[PartitionID, TaskGroup] = {}

    @staticmethod
    def get_or_create_job(job_id: JobID,
                          root_role_name_set: Set[RootRoleName],
                          partition_num: PartitionNum) -> 'Job':
        if job_id not in Job.job_dict:
            Job.job_dict[job_id] = Job(job_id, root_role_name_set, partition_num)
        _job = Job.job_dict[job_id]

        if _job.root_role_name_set != root_role_name_set:
            raise HTTPException(detail=f"registed role name set: {_job.root_role_name_set} != {root_role_name_set}",
                                status_code=434)

        if _job.partition_num != partition_num:
            raise HTTPException(detail=f"registed partition_num: {_job.partition_num} != {partition_num}",
                                status_code=434)

        return _job

    @staticmethod
    def get_job(job_id: JobID) -> 'Job':
        if job_id not in Job.job_dict:
            raise HTTPException(detail=f"no such job_id : {job_id}",
                                status_code=435)
        return Job.job_dict[job_id]

    def register_driver(self,
                        root_role_name: RootRoleName,
                        ) -> None:
        if self.job_state != Job.NORMAL_STATE:
            raise HTTPException(detail=f"can't register driver under '{self.job_state}' state",
                                status_code=431)

        if root_role_name in self.driver_dict:
            self.mark_failed()
            self.drop_driver(root_role_name)
            raise HTTPException(detail=f"'{root_role_name}' was registed already, "
                                       f"the last driver may failed without inform to coordinater, "
                                       f"so killing all other drivers",
                                status_code=432)

        if root_role_name not in self.root_role_name_set:
            raise HTTPException(detail=f"unknown role_name : {root_role_name}, "
                                       f"expected role_name : {self.root_role_name_set}",
                                status_code=433)

        self.driver_dict[root_role_name] = Driver(root_role_name)

    def get_driver(self, root_role_name: RootRoleName) -> Driver:
        if root_role_name not in self.driver_dict:
            raise HTTPException(detail=f"no such driver : {root_role_name}",
                                status_code=435)
        return self.driver_dict[root_role_name]

    def drop_driver(self, root_role_name: RootRoleName) -> None:
        print(f"drop driver of job_id:{self.job_id}, root_role_name:{root_role_name}")
        del self.driver_dict[root_role_name]
        if not len(self.driver_dict):
            print(f"clear job : {self.job_id}")
            del Job.job_dict[self.job_id]

    def mark_failed(self) -> None:
        self.job_state = Job.FAILED_STATE

    def is_failed(self) -> bool:
        return self.job_state == Job.FAILED_STATE


class DriverStateMonitor(Thread):
    def __init__(self) -> None:
        super().__init__(daemon=True)

    def run(self) -> None:
        while True:
            for _job in list(Job.job_dict.values()):
                for _driver in list(_job.driver_dict.values()):
                    if _driver.is_heartbeat_expired():
                        print(f"driver expired job_id:{_job.job_id}, root_role_name:{_driver.root_role_name}")
                        _job.mark_failed()
                        _job.drop_driver(_driver.root_role_name)

            time.sleep(STATE_CHECKING_INTERVAL)


def _start_server(host: Host, port: Port):

    app = FastAPI()

    @app.post("/driver_heartbeat")
    def driver_heartbeat(job_id: JobID = Body(...),
                         root_role_name: RootRoleName = Body(...)):
        _job = Job.get_job(job_id)
        if _job.is_failed():
            _job.drop_driver(root_role_name)
        else:
            _driver = _job.get_driver(root_role_name)
            _driver.refresh_heartbeat()
            print(f"driver:{root_role_name} heartbeat refreshed to {_driver.last_heartbeat}")
        return JSONResponse(content={'job_state': _job.job_state})

    @app.post("/register_driver")
    def register_driver(job_id: JobID = Body(...),
                        partition_num: PartitionNum = Body(...),
                        root_role_name_set: Set[RootRoleName] = Body(...),
                        root_role_name: RootRoleName = Body(...)):
        root_role_name_set = set(root_role_name_set)
        print(f"register job_id:{job_id}, partition_num:{partition_num}, "
              f"root_role_name:{root_role_name}, root_role_name_set:{root_role_name_set}")
        _job = Job.get_or_create_job(job_id=job_id,
                                     root_role_name_set=root_role_name_set,
                                     partition_num=partition_num)
        _job.register_driver(root_role_name=root_role_name)
        return SUCCESS_RESPONSE

    # @app.post("/register_task")
    # def register_task(job_id: JobID = Body(...),
    #                   stage_id: StageID = Body(...),
    #                   partition_id: PartitionID = Body(...),
    #                   task_attempt_id: TaskAttemptID = Body(...),
    #                   root_role_name: RootRoleName = Body(...),
    #                   root_role_name_set: Set[RootRoleName] = Body(...),
    #                   task_server_url: Url = Body(...)):
    #     job_manager = job_manager_dict[job_id]
    #     group_manager = job_manager.get_or_set_fed_group(partition_id, root_role_name_set)

    #     if group_manager.is_started:
    #         _fail_group_manager(group_manager)

    #     group_manager.register_role(root_role_name=root_role_name,
    #                                 server_url=task_server_url,
    #                                 stage_id=stage_id,
    #                                 task_attempt_id=task_attempt_id)
    #     if group_manager.is_all_registed():
    #         _start_group_manager(group_manager)

    #     return SUCCESS_RESPONSE

    # def _start_group_manager(group_manager: TaskGroupManager) -> None:
    #     root_role_name_url_dict = {root_role_name: fed_role.server_url
    #                                for root_role_name, fed_role in group_manager.registed_role_dict.items()}
    #     print(f"root_role_name_url_dict : {root_role_name_url_dict}")
    #     for root_role_name, server_url in root_role_name_url_dict.items():
    #         print(f"start task : {root_role_name}@{server_url}")
    #         _post(url=f"{server_url}/start_task",
    #               data={'root_role_name_url_dict': root_role_name_url_dict})

    #     group_manager.mark_started()

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

    DriverStateMonitor().start()
    uvicorn.run(app=app, host=host, port=port, debug=True,
                access_log=True, log_level=logging.DEBUG, use_colors=True)


if __name__ == "__main__":
    _start_server('127.0.0.1', 6609)
