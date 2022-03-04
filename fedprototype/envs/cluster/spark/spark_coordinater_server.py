import logging
import time
from datetime import datetime, timedelta
from threading import Thread
from typing import Any, Callable, Dict, Set

import uvicorn
from fastapi import Body, FastAPI
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse
from fedprototype.envs.cluster.spark.constants import *
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
                 root_role_name: RootRoleName) -> None:
        self.job_id = job_id
        self.partition_id = partition_id
        self.root_role_name = root_role_name
        self.server_url: Url = None
        self.stage_id: StageID = None
        self.task_attempt_num: TaskAttemptNum = None
        self.state = UNREGISTER
        self.is_successed = False

    def register(self, server_url: Url, stage_id: StageID, task_attempt_num: TaskAttemptNum):
        if self.state != UNREGISTER:
            raise HTTPException(detail=f"cann't register task under state:{self.state}",
                                status_code=444)
        self.server_url = server_url
        self.stage_id = stage_id
        self.task_attempt_num = task_attempt_num
        self.state = STANDBY_FOR_RUN

    def __repr__(self) -> str:
        return f"Task >> job_id:{self.job_id}, partition_id:{self.partition_id}, " \
               f"role_name:{self.root_role_name}, server_url:{self.server_url} " \
               f"stage_id:{self.stage_id}, task_attempt_num:{self.task_attempt_num}" \
               f"state:{self.state}, is_successed:{self.is_successed}"


class TaskGroup:
    def __init__(self,
                 job_id: JobID,
                 partition_id: PartitionID,
                 root_role_name_set: Set[RootRoleName]) -> None:
        self.job_id = job_id
        self.partition_id = partition_id
        self.root_role_name_set = root_role_name_set
        self.task_dict: Dict[RootRoleName, Task] = {}
        self._init_tasks()
        self.state = UNREGISTER
        self.is_successed = False

    def is_all_state(self, state):
        return all(map(lambda _t: _t.state == state, self.task_dict.values()))

    def set_all_state(self, state):
        self.state = state
        for _task in self.task_dict.values():
            _task.state = state

    def distribute_task_server_url(self) -> None:
        root_role_name_url_dict = {_role_name: _task.server_url
                                   for _role_name, _task in self.task_dict.items()}
        for task in list(self.task_dict.values()):
            print(f"distribute task server url to {task} root_role_name_url_dict:{root_role_name_url_dict}")
            _try_func(func=post_pro,
                      detail=f"lost connect with {task}",
                      status_code=441,
                      retry_times=3,
                      url=f"{task.server_url}/update_task_server_url",
                      json=root_role_name_url_dict)

    def task_group_success(self):
        for task in list(self.task_dict.values()):
            print(f"inform task group success : {task.server_url}")
            _try_func(func=post_pro,
                      detail=f"lost connect with {task}",
                      status_code=441,
                      retry_times=3,
                      url=f"{task.server_url}/task_group_success")

    def _init_tasks(self):
        self.task_dict = {_rrn: Task(self.job_id, self.partition_id, _rrn) for _rrn in self.root_role_name_set}

    def __repr__(self) -> str:
        return f"TaskGroup >> job_id:{self.job_id}, partition_id:{self.partition_id}, " \
               f"registed_tasks:{set(self.task_dict.keys())}" \
               f"state:{self.state}, is_successed:{self.is_successed}"


class Driver:
    def __init__(self, job_id: JobID, root_role_name: RootRoleName) -> None:
        self.job_id = job_id
        self.root_role_name: RootRoleName = root_role_name
        self.last_heartbeat: datetime = None
        self.state = UNREGISTER
        self.is_successed = False

    def refresh_heartbeat(self) -> None:
        self.last_heartbeat: datetime = datetime.now()

    def is_heartbeat_expired(self) -> bool:
        if self.state == UNREGISTER:
            return False
        else:
            return (datetime.now() - self.last_heartbeat) > DRIVER_HEARTBEAT_EXPIRED_TIMEOUT

    def __repr__(self) -> str:
        return f"Driver >> job_id:{self.job_id}, role_name:{self.root_role_name}, is_expired:{self.is_heartbeat_expired()}" \
               f"state:{self.state}, is_successed:{self.is_successed}"


class Job:
    job_dict: Dict[JobID, 'Job'] = {}

    def __init__(self,
                 job_id: JobID,
                 root_role_name_set: Set[RootRoleName],
                 partition_num: PartitionNum
                 ) -> None:
        self.job_id = job_id
        self.root_role_name_set = root_role_name_set
        self.partition_num = partition_num

        self.driver_dict: Dict[RootRoleName, Driver] = {}
        self._init_drivers()

        self.task_group_dict: Dict[PartitionID, TaskGroup] = {}
        self._init_task_groups()

        self.state = UNREGISTER
        self.is_successed = False

    def _init_drivers(self):
        self.driver_dict = {_rrn: Driver(self.job_id, _rrn) for _rrn in self.root_role_name_set}

    def _init_task_groups(self):
        self.task_group_dict = {_pi: TaskGroup(self.job_id, _pi, self.root_role_name_set) for _pi in range(self.partition_num)}

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

    def register_driver(self, root_role_name: RootRoleName) -> None:
        if self.state != UNREGISTER:
            raise HTTPException(detail=f"can't register driver under '{self.state}' state",
                                status_code=431)

        _driver = self.get_driver(root_role_name)

        if _driver.state != UNREGISTER:
            self.drop_driver(_driver, success=False)
            raise HTTPException(detail=f"'{root_role_name}' was registed already, "
                                       f"the last driver may failed without inform to coordinater, "
                                       f"so killing all other drivers",
                                status_code=432)

        _driver.state = STANDBY_FOR_RUN
        _driver.refresh_heartbeat()
        if self.is_all_state(STANDBY_FOR_RUN):
            self.set_all_state(RUNNING)

    def is_all_state(self, state):
        return all(map(lambda _d: _d.state == state, self.driver_dict.values()))

    def set_all_state(self, state):
        self.state = state
        for _driver in self.driver_dict.values():
            _driver.state = state

    def set_all_successed(self, success):
        self.is_successed = success
        for _driver in self.driver_dict.values():
            _driver.is_successed = success

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
        if self.state != RUNNING:
            raise HTTPException(detail=f"unable to register task under state:{self.state}",
                                status_code=437)

        if not (0 <= partition_id < self.partition_num):
            raise HTTPException(detail=f"partition_id:{partition_id} out of scope:[0, {self.partition_num})",
                                status_code=439)

        if root_role_name not in self.root_role_name_set:
            raise HTTPException(detail=f"unknown role_name : {root_role_name}, "
                                       f"expected role_name : {self.root_role_name_set}",
                                status_code=433)

        _task_group = self.task_group_dict[partition_id]
        _task = _task_group.task_dict[root_role_name]
        if _task.state == RUNNING:
            _task.state = EXITED
            _task.is_successed = False
            self._fail_task_group(_task_group)
            raise HTTPException(detail=f"task was registed already, "
                                f"the last task may failed without inform to coordinater, "
                                f"so killing all other tasks in same fed group",
                                status_code=440)
        _task.register(server_url, stage_id, task_attempt_num)
        if _task_group.is_all_state(STANDBY_FOR_RUN):
            _task_group.set_all_state(RUNNING)
            _task_group.distribute_task_server_url()

    def task_success(self,
                     partition_id: PartitionID,
                     root_role_name: RootRoleName,
                     server_url: Url,
                     stage_id: StageID,
                     task_attempt_num: TaskAttemptNum
                     ) -> None:
        _task_group = self.task_group_dict[partition_id]
        _task = _task_group.task_dict[root_role_name]
        if _task.state != RUNNING:
            raise HTTPException(detail=f"unable to success task under state:{self.state}",
                                status_code=446)
        _task.state = STANDBY_FOR_EXIT
        _task.is_successed = True
        if _task_group.is_all_state(STANDBY_FOR_EXIT):
            _task_group.set_all_state(EXITED)
            _task_group.task_group_success()

    def driver_finish(self, root_role_name: RootRoleName, success: bool):
        _driver = self.get_driver(root_role_name)
        if _driver.state == EXITED:
            return
        elif _driver.state != RUNNING:
            raise HTTPException(detail=f"unacceptable driver state:{_driver.state}",
                                status_code=441)
        elif success:
            if self.state == RUNNING:
                _driver.state = STANDBY_FOR_EXIT
                _driver.is_successed = True
                if self.is_all_state(STANDBY_FOR_EXIT):
                    self.set_all_state(EXITED)
            elif self.state == EXITED:
                _driver.state = EXITED
                _driver.is_successed = self.is_successed
            else:
                raise HTTPException(detail=f"unacceptable under state:{self.state}",
                                    status_code=441)
        else:
            self.set_all_state(EXITED)
            self.set_all_successed(False)

    def _fail_task_group(self, task_group: TaskGroup) -> None:
        pass
        # for _task in list(task_group.task_dict.values()):
        #     print(f"post to kill {_task}")
        #     post_pro(check_status_code=False,
        #              convert_content_type=False,
        #              retry_times=FAIL_TASK_RETRY_TIMES,
        #              retry_interval=FAIL_TASK_RETRY_IINTERVAL,
        #              error='None',
        #              url=f"{_task.server_url}/fail_task")
        # print(f"clear {task_group}")
        # del self.task_group_dict[task_group.partition_id]

    # def mark_failed(self) -> None:
    #     self.job_state = Job.FAILED

    # def is_failed(self) -> bool:
    #     return self.job_state == Job.FAILED

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

    @app.post("/driver/register")
    def driver_register(job_id: JobID = Body(...),
                        partition_num: PartitionNum = Body(...),
                        root_role_name_set: Set[RootRoleName] = Body(...),
                        root_role_name: RootRoleName = Body(...)):
        print(f"register driver job_id:{job_id}, partition_num:{partition_num}, "
              f"root_role_name:{root_role_name}, root_role_name_set:{root_role_name_set}")
        _job = Job.get_or_create_job(job_id=job_id,
                                     root_role_name_set=root_role_name_set,
                                     partition_num=partition_num)
        _job.register_driver(root_role_name)
        return SUCCESS_RESPONSE

    @app.post("/driver/wait_for_running")
    def driver_wait_for_running(job_id: JobID = Body(...),
                                root_role_name: RootRoleName = Body(...)):
        _job = Job.get_job(job_id)
        _driver = _job.get_driver(root_role_name)
        _unregisted_driver = [_d.root_role_name for _d in _job.driver_dict.values() if _d.state == UNREGISTER]
        return JSONResponse(content={'state': _driver.state,
                                     'unregisted_driver': _unregisted_driver})

    @app.post("/driver/heartbeat")
    def driver_heartbeat(job_id: JobID = Body(...),
                         root_role_name: RootRoleName = Body(...)):
        _job = Job.get_job(job_id)
        _driver = _job.get_driver(root_role_name)
        if _driver.state == EXITED:
            return JSONResponse(content={'state': EXITED, 'is_successed': _job.is_successed})
        elif _driver.state in {STANDBY_FOR_RUN, RUNNING, STANDBY_FOR_EXIT}:
            _driver.refresh_heartbeat()
            print(f"heartbeat refreshed {_driver}")
            return JSONResponse(content={'state': _driver.state})
        else:
            raise HTTPException(detail=f"unacceptable heartbeat under state:{_driver.state}",
                                status_code=441)

    @app.post("/driver/finish")
    def driver_finish(job_id: JobID = Body(...),
                      root_role_name: RootRoleName = Body(...),
                      success: bool = Body(...)):
        _job = Job.get_job(job_id)
        _job.driver_finish(root_role_name, success)
        return SUCCESS_RESPONSE

    @app.post("/driver/wait_for_finish")
    def driver_wait_for_finish(job_id: JobID = Body(...),
                               root_role_name: RootRoleName = Body(...)):
        _job = Job.get_job(job_id)
        _driver = _job.get_driver(root_role_name)
        return JSONResponse(content={'state': _driver.state, 'is_successed': _driver.is_successed})

    @app.post("/task/register")
    def task_register(job_id: JobID = Body(...),
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

    @app.post("/task/success")
    def task_success(job_id: JobID = Body(...),
                     partition_id: PartitionID = Body(...),
                     root_role_name: RootRoleName = Body(...),
                     stage_id: StageID = Body(...),
                     task_attempt_num: TaskAttemptNum = Body(...),
                     task_server_url: Url = Body(...)):
        print(f"finish tesk job_id:{job_id}, partition_id:{partition_id}, "
              f"root_role_name:{root_role_name}, task_server_url:{task_server_url}")
        _job = Job.get_job(job_id)
        _job.task_success(partition_id=partition_id,
                          root_role_name=root_role_name,
                          server_url=task_server_url,
                          stage_id=stage_id,
                          task_attempt_num=task_attempt_num)
        return SUCCESS_RESPONSE

    # @app.post("/cancel_driver")
    # def cancel_driver(job_id: JobID = Body(...),
    #                   root_role_name: RootRoleName = Body(...),
    #                   success: bool = Body(...)):
    #     print(f"cancel driver job_id:{job_id}, root_role_name:{root_role_name}, success:{success}")
    #     _job = Job.get_job(job_id)
    #     _driver = _job.get_driver(root_role_name)
    #     _job.drop_driver(_driver, success=success)
    #     return JSONResponse(content={'job_state': _job.job_state})

    # @app.post("/cancel_task")
    # def cancel_task(job_id: JobID = Body(...),
    #                 root_role_name: RootRoleName = Body(...),
    #                 partition_id: PartitionID = Body(...),
    #                 stage_id: StageID = Body(...),
    #                 task_attempt_num: TaskAttemptNum = Body(...),
    #                 success: bool = Body(...)):
    #     _job = Job.get_job(job_id)

    #     if partition_id not in _job.task_group_dict:
    #         return NO_OP_RESPONSE
    #     _task_group = _job.task_group_dict[partition_id]

    #     if root_role_name not in _task_group.task_dict:
    #         return NO_OP_RESPONSE
    #     _task = _task_group.task_dict[root_role_name]

    #     if (_task.stage_id != stage_id) or (_task.task_attempt_num != task_attempt_num):
    #         return NO_OP_RESPONSE
    #     print(f"cancel {_task}, success:{success}")
    #     _job.drop_task(_task, success=success)
    #     return SUCCESS_RESPONSE

    # DriverStateMonitor().start()
    uvicorn.run(app=app, host=host, port=port, debug=True,
                access_log=True, log_level=logging.DEBUG, use_colors=True)


if __name__ == "__main__":
    _start_server('127.0.0.1', 6609)
