import logging
import time
from datetime import datetime, timedelta
from threading import Thread
from typing import Any, Callable, Dict, Set

import uvicorn
from fastapi import Body, FastAPI
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse

from fedprototype.tools.io import post_pro
from fedprototype.typing import (
    Host,
    JobID,
    PartitionID,
    PartitionNum,
    Port,
    RootRoleName,
    StageID,
    TaskAttemptNum,
    Url,
)

SUCCESS_CODE = 200
SUCCESS_RESPONSE = JSONResponse(content={'msg': 'OK'})
NO_OP_RESPONSE = JSONResponse(content={'msg': 'NO_OP'})
DRIVER_HEARTBEAT_EXPIRED_TIMEOUT = timedelta(seconds=40)
STATE_CHECKING_INTERVAL = 10
FAIL_TASK_RETRY_TIMES = 3
FAIL_TASK_RETRY_IINTERVAL = 3


class Task:
    def __init__(self,
                 job_id: JobID,
                 partition_id: PartitionID,
                 root_role_name: RootRoleName,
                 server_url: Url,
                 stage_id: StageID,
                 task_attempt_num: TaskAttemptNum) -> None:
        self.job_id = job_id
        self.partition_id = partition_id
        self.root_role_name = root_role_name
        self.server_url = server_url
        self.stage_id = stage_id
        self.task_attempt_num = task_attempt_num

    def __repr__(self) -> str:
        return f"Task >> job_id:{self.job_id}, partition_id:{self.partition_id}, " \
               f"role_name:{self.root_role_name}, server_url:{self.server_url} " \
               f"stage_id:{self.stage_id}, task_attempt_num:{self.task_attempt_num}"


class TaskGroup:
    def __init__(self, job_id: JobID, partition_id: PartitionID) -> None:
        self.job_id = job_id
        self.partition_id = partition_id
        self.task_dict: Dict[RootRoleName, Task] = {}

    def __repr__(self) -> str:
        return f"TaskGroup >> job_id:{self.job_id}, partition_id:{self.partition_id}, " \
               f"registed_tasks:{set(self.task_dict.keys())}"


class Driver:
    def __init__(self, job_id: JobID, root_role_name: RootRoleName) -> None:
        self.job_id = job_id
        self.root_role_name: RootRoleName = root_role_name
        self.last_heartbeat: datetime = None
        self.refresh_heartbeat()

    def refresh_heartbeat(self) -> None:
        self.last_heartbeat: datetime = datetime.now()

    def is_heartbeat_expired(self) -> bool:
        return (datetime.now() - self.last_heartbeat) > DRIVER_HEARTBEAT_EXPIRED_TIMEOUT

    def __repr__(self) -> str:
        return f"Driver >> job_id:{self.job_id}, role_name:{self.root_role_name}, is_expired:{self.is_heartbeat_expired()}"


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
            Job.job_dict[job_id] = Job(job_id=job_id,
                                       root_role_name_set=root_role_name_set,
                                       partition_num=partition_num)
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
            raise HTTPException(detail=f"no such job_id : {job_id}", status_code=435)
        return Job.job_dict[job_id]

    def register_driver(self,
                        root_role_name: RootRoleName,
                        ) -> None:
        if self.job_state != Job.NORMAL_STATE:
            raise HTTPException(detail=f"can't register driver under '{self.job_state}' state",
                                status_code=431)

        if root_role_name in self.driver_dict:
            _expired_driver = self.get_driver(root_role_name)
            self.drop_driver(_expired_driver, success=False)
            raise HTTPException(detail=f"'{root_role_name}' was registed already, "
                                       f"the last driver may failed without inform to coordinater, "
                                       f"so killing all other drivers",
                                status_code=432)

        if root_role_name not in self.root_role_name_set:
            raise HTTPException(detail=f"unknown role_name : {root_role_name}, "
                                       f"expected role_name : {self.root_role_name_set}",
                                status_code=433)

        self.driver_dict[root_role_name] = Driver(job_id=self.job_id, root_role_name=root_role_name)

    def get_driver(self, root_role_name: RootRoleName) -> Driver:
        if root_role_name not in self.driver_dict:
            raise HTTPException(detail=f"no such driver : {root_role_name}",
                                status_code=435)
        return self.driver_dict[root_role_name]

    def drop_driver(self, driver: Driver, success: bool = True) -> None:
        print(f"drop {driver}")
        root_role_name = driver.root_role_name
        for task_group in list(self.task_group_dict.values()):
            if root_role_name in task_group.task_dict:
                task = task_group.task_dict[root_role_name]
                self.drop_task(task, success=success)

        del self.driver_dict[root_role_name]
        if not success:
            self.mark_failed()

        if not len(self.driver_dict):
            print(f"clear job : {self.job_id}")
            del Job.job_dict[self.job_id]

    def drop_task(self, task: Task, success: bool = True) -> None:
        task_group = self.task_group_dict[task.partition_id]
        del task_group.task_dict[task.root_role_name]
        if not success:
            self._fail_task_group(task_group)

    def register_task(self,
                      partition_id: PartitionID,
                      root_role_name: RootRoleName,
                      server_url: Url,
                      stage_id: StageID,
                      task_attempt_num: TaskAttemptNum
                      ) -> None:
        if not self.is_normal():
            raise HTTPException(detail=f"unable to register task, because job:{self.job_id} is failed",
                                status_code=437)

        if not (0 <= partition_id < self.partition_num):
            raise HTTPException(detail=f"partition_id:{partition_id} out of scope:[0, {self.partition_num})",
                                status_code=439)

        if root_role_name not in self.root_role_name_set:
            raise HTTPException(detail=f"unknown role_name : {root_role_name}, "
                                       f"expected role_name : {self.root_role_name_set}",
                                status_code=433)

        if partition_id not in self.task_group_dict:
            self.task_group_dict[partition_id] = TaskGroup(job_id=self.job_id, partition_id=partition_id)
        _task_group = self.task_group_dict[partition_id]

        if root_role_name in _task_group.task_dict:
            del _task_group.task_dict[root_role_name]
            self._fail_task_group(_task_group)
            raise HTTPException(detail=f"task was registed already, "
                                f"the last task may failed without inform to coordinater, "
                                f"so killing all other tasks in same fed group",
                                status_code=440)

        _task_group.task_dict[root_role_name] = Task(job_id=self.job_id,
                                                     partition_id=partition_id,
                                                     root_role_name=root_role_name,
                                                     server_url=server_url,
                                                     stage_id=stage_id,
                                                     task_attempt_num=task_attempt_num)

        self._distribute_task_server_url(_task_group)

    def _fail_task_group(self, task_group: TaskGroup) -> None:
        for _task in list(task_group.task_dict.values()):
            print(f"post to kill {_task}")
            post_pro(check_status_code=False,
                     convert_content_type=False,
                     retry_times=FAIL_TASK_RETRY_TIMES,
                     retry_interval=FAIL_TASK_RETRY_IINTERVAL,
                     error='None',
                     url=f"{_task.server_url}/fail_task")
        print(f"clear {task_group}")
        del self.task_group_dict[task_group.partition_id]

    def _distribute_task_server_url(self, task_group: TaskGroup) -> None:
        root_role_name_url_dict = {_role_name: _task.server_url
                                   for _role_name, _task in task_group.task_dict.items()}
        for task in list(task_group.task_dict.values()):
            print(f"distribute task server url to {task} root_role_name_url_dict:{root_role_name_url_dict}")
            _try_func(func=post_pro,
                      detail=f"lost connect with {task}",
                      status_code=441,
                      retry_times=3,
                      url=f"{task.server_url}/update_task_server_url",
                      json=root_role_name_url_dict)

    def mark_failed(self) -> None:
        self.job_state = Job.FAILED_STATE

    def is_failed(self) -> bool:
        return self.job_state == Job.FAILED_STATE

    def is_normal(self) -> bool:
        return self.job_state == Job.NORMAL_STATE

    def __repr__(self) -> str:
        return f"Job >> job_id:{self.job_id}, job_state:{self.job_state}"


def _try_func(func: Callable, detail: str, status_code: int, **kwargs) -> Any:
    try:
        return func(**kwargs)
    except Exception as e:
        if isinstance(e, HTTPException):
            err_detail = e.detail
        else:
            err_detail = str(e)
        raise HTTPException(detail=f"{detail}. {err_detail}", status_code=status_code)


class DriverStateMonitor(Thread):
    def __init__(self) -> None:
        super().__init__(daemon=True)

    def run(self) -> None:
        while True:
            for _job in list(Job.job_dict.values()):
                for _driver in list(_job.driver_dict.values()):
                    if _driver.is_heartbeat_expired():
                        print(f"driver expired {_driver}")
                        _job.drop_driver(_driver, success=False)

            time.sleep(STATE_CHECKING_INTERVAL)


def _start_server(host: Host, port: Port):

    app = FastAPI()

    @app.post("/driver_heartbeat")
    def driver_heartbeat(job_id: JobID = Body(...),
                         root_role_name: RootRoleName = Body(...)):
        _job = Job.get_job(job_id)
        _driver = _job.get_driver(root_role_name)
        if _job.is_failed():
            _job.drop_driver(_driver, success=False)
        else:
            _driver.refresh_heartbeat()
            print(f"heartbeat refreshed {_driver}")
        return JSONResponse(content={'job_state': _job.job_state})

    @app.post("/register_driver")
    def register_driver(job_id: JobID = Body(...),
                        partition_num: PartitionNum = Body(...),
                        root_role_name_set: Set[RootRoleName] = Body(...),
                        root_role_name: RootRoleName = Body(...)):
        root_role_name_set = set(root_role_name_set)
        print(f"register driver job_id:{job_id}, partition_num:{partition_num}, "
              f"root_role_name:{root_role_name}, root_role_name_set:{root_role_name_set}")
        _job = Job.get_or_create_job(job_id=job_id,
                                     root_role_name_set=root_role_name_set,
                                     partition_num=partition_num)
        _job.register_driver(root_role_name=root_role_name)
        return SUCCESS_RESPONSE

    @app.post("/register_task")
    def register_task(job_id: JobID = Body(...),
                      partition_id: PartitionID = Body(...),
                      root_role_name: RootRoleName = Body(...),
                      stage_id: StageID = Body(...),
                      task_attempt_num: TaskAttemptNum = Body(...),
                      task_server_url: Url = Body(...)):
        print(f"register tesk job_id:{job_id}, partition_id:{partition_id}, "
              f"root_role_name:{root_role_name}, task_server_url:{task_server_url}")
        _job = Job.get_job(job_id)
        _job.register_task(partition_id=partition_id,
                           root_role_name=root_role_name,
                           server_url=task_server_url,
                           stage_id=stage_id,
                           task_attempt_num=task_attempt_num)
        return SUCCESS_RESPONSE

    @app.post("/cancel_driver")
    def cancel_driver(job_id: JobID = Body(...),
                      root_role_name: RootRoleName = Body(...),
                      success: bool = Body(...)):
        print(f"cancel driver job_id:{job_id}, root_role_name:{root_role_name}, success:{success}")
        _job = Job.get_job(job_id)
        _driver = _job.get_driver(root_role_name)
        _job.drop_driver(_driver, success=success)
        return JSONResponse(content={'job_state': _job.job_state})

    @app.post("/cancel_task")
    def cancel_task(job_id: JobID = Body(...),
                    root_role_name: RootRoleName = Body(...),
                    partition_id: PartitionID = Body(...),
                    stage_id: StageID = Body(...),
                    task_attempt_num: TaskAttemptNum = Body(...),
                    success: bool = Body(...)):
        _job = Job.get_job(job_id)

        if partition_id not in _job.task_group_dict:
            return NO_OP_RESPONSE
        _task_group = _job.task_group_dict[partition_id]

        if root_role_name not in _task_group.task_dict:
            return NO_OP_RESPONSE
        _task = _task_group.task_dict[root_role_name]

        if (_task.stage_id != stage_id) or (_task.task_attempt_num != task_attempt_num):
            return NO_OP_RESPONSE
        print(f"cancel {_task}, success:{success}")
        _job.drop_task(_task, success=success)
        return SUCCESS_RESPONSE

    DriverStateMonitor().start()
    uvicorn.run(app=app, host=host, port=port, debug=True,
                access_log=True, log_level=logging.DEBUG, use_colors=True)


if __name__ == "__main__":
    _start_server('127.0.0.1', 6609)
