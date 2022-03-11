import time
from datetime import datetime, timedelta
from threading import Lock, Thread
from typing import Any, Callable, Dict, Set

import uvicorn
from fastapi import Body, FastAPI
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse

from fedprototype.envs.cluster.spark.constants import *
from fedprototype.tools.io import post_pro
from fedprototype.tools.log import getLogger
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

DRIVER_HEARTBEAT_EXPIRED_TIMEOUT = timedelta(seconds=40)
STATE_CHECKING_INTERVAL = 10

logger = getLogger('Frame.Coor')


class _Locker:
    def __init__(self) -> None:
        self._lock = Lock()

    def __enter__(self) -> '_Locker':
        self._lock.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._lock.release()


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
                                status_code=440)
        self.server_url = server_url
        self.stage_id = stage_id
        self.task_attempt_num = task_attempt_num
        self.state = STANDBY_FOR_RUN

    def clear(self):
        self.server_url: Url = None
        self.stage_id: StageID = None
        self.task_attempt_num: TaskAttemptNum = None
        self.state = UNREGISTER
        self.is_successed = False

    def __repr__(self) -> str:
        return f"Task >> job_id:{self.job_id}, partition_id:{self.partition_id}, " \
               f"role_name:{self.root_role_name}, server_url:{self.server_url}, " \
               f"task_attempt_num:{self.task_attempt_num}, state:{self.state}, " \
               f"is_successed:{self.is_successed}"


class TaskGroup(_Locker):
    def __init__(self,
                 job_id: JobID,
                 partition_id: PartitionID,
                 root_role_name_set: Set[RootRoleName]) -> None:
        super().__init__()
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

    def set_all_successed(self, success):
        self.is_successed = success
        for _task in self.task_dict.values():
            _task.is_successed = success

    def distribute_task_server_url(self) -> None:
        root_role_name_url_dict = {_role_name: _task.server_url
                                   for _role_name, _task in self.task_dict.items()}
        for task in self.task_dict.values():
            logger.debug(f"distribute task server url to {task} root_role_name_url_dict:{root_role_name_url_dict}")
            _try_func(func=post_pro,
                      detail=f"lost connect with {task}",
                      status_code=441,
                      retry_times=3,
                      url=f"{task.server_url}/update_task_server_url",
                      json=root_role_name_url_dict)

    def task_group_successed(self):
        for task in self.task_dict.values():
            logger.debug(f"inform task group success : {task.server_url}")
            _try_func(func=post_pro,
                      detail=f"lost connect with {task}",
                      status_code=441,
                      retry_times=3,
                      url=f"{task.server_url}/task_group_successed")
        self.set_all_state(EXITED)
        self.set_all_successed(True)

    def task_group_failed(self):
        for task in self.task_dict.values():
            if task.state != UNREGISTER:
                logger.debug(f"inform task group failed : {task.server_url}")
                post_pro(check_status_code=False,
                         convert_content_type=False,
                         error='None',
                         url=f"{task.server_url}/task_group_failed")
        self.clear()

    def register_task(self,
                      root_role_name: RootRoleName,
                      server_url: Url,
                      stage_id: StageID,
                      task_attempt_num: TaskAttemptNum) -> None:
        _task = self.task_dict[root_role_name]
        if _task.state != UNREGISTER:
            self.task_group_failed()
        _task.register(server_url, stage_id, task_attempt_num)
        if self.is_all_state(STANDBY_FOR_RUN):
            self.set_all_state(RUNNING)
            self.distribute_task_server_url()

    def task_successed(self,
                       root_role_name: RootRoleName,
                       server_url: Url,
                       stage_id: StageID,
                       task_attempt_num: TaskAttemptNum):
        _task = self.task_dict[root_role_name]
        if _task.state != RUNNING:
            raise HTTPException(detail=f"unable to success task under state:{self.state}",
                                status_code=442)
        _task.state = STANDBY_FOR_EXIT
        _task.is_successed = True
        if self.is_all_state(STANDBY_FOR_EXIT):
            self.task_group_successed()

    def clear(self):
        for task in self.task_dict.values():
            task.clear()
        self.state = UNREGISTER
        self.is_successed = False

    def _init_tasks(self):
        self.task_dict = {_rrn: Task(self.job_id, self.partition_id, _rrn) for _rrn in self.root_role_name_set}

    def __repr__(self) -> str:
        return f"TaskGroup >> job_id:{self.job_id}, partition_id:{self.partition_id}, " \
               f"state:{self.state}, is_successed:{self.is_successed}"


class Driver:
    def __init__(self, job_id: JobID, root_role_name: RootRoleName) -> None:
        self.job_id = job_id
        self.root_role_name: RootRoleName = root_role_name
        self.last_heartbeat: datetime = None
        self.refresh_heartbeat()

        self.state = UNREGISTER
        self.is_successed = False

    def refresh_heartbeat(self) -> None:
        self.last_heartbeat: datetime = datetime.now()

    def is_heartbeat_expired(self) -> bool:
        return (datetime.now() - self.last_heartbeat) > DRIVER_HEARTBEAT_EXPIRED_TIMEOUT

    def __repr__(self) -> str:
        return f"Driver >> job_id:{self.job_id}, role_name:{self.root_role_name}, is_expired:{self.is_heartbeat_expired()}, " \
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
                                status_code=443)

        if _job.partition_num != partition_num:
            raise HTTPException(detail=f"registed partition_num: {_job.partition_num} != {partition_num}",
                                status_code=443)

        return _job

    @staticmethod
    def get_job(job_id: JobID) -> 'Job':
        if job_id not in Job.job_dict:
            raise HTTPException(detail=f"no such job_id : {job_id}", status_code=445)
        return Job.job_dict[job_id]

    @staticmethod
    def drop_job(job_id: JobID) -> None:
        logger.info(f"del job:{job_id}")
        del Job.job_dict[job_id]

    def register_driver(self, root_role_name: RootRoleName) -> None:
        if self.state != UNREGISTER:
            raise HTTPException(detail=f"can't register driver under '{self.state}' state",
                                status_code=446)

        _driver = self.get_driver(root_role_name)

        if _driver.state != UNREGISTER:
            self.fail_job()
            raise HTTPException(detail=f"'{root_role_name}' was registed already, "
                                       f"the last driver may failed without inform to coordinater, "
                                       f"so killing all other drivers",
                                status_code=446)

        _driver.state = STANDBY_FOR_RUN
        _driver.refresh_heartbeat()
        if self.is_all_state(STANDBY_FOR_RUN):
            self.set_all_state(RUNNING)

    def get_driver(self, root_role_name: RootRoleName) -> Driver:
        if root_role_name not in self.driver_dict:
            raise HTTPException(detail=f"no such driver : {root_role_name}",
                                status_code=447)
        return self.driver_dict[root_role_name]

    def is_all_state(self, state):
        return all(map(lambda _d: _d.state == state, self.driver_dict.values()))

    def set_all_successed(self, success):
        self.is_successed = success
        for _driver in self.driver_dict.values():
            _driver.is_successed = success

    def set_all_state(self, state):
        self.state = state
        for _driver in self.driver_dict.values():
            _driver.state = state

    def driver_finish(self, root_role_name: RootRoleName, success: bool):
        _driver = self.get_driver(root_role_name)
        if _driver.state != RUNNING:
            raise HTTPException(detail=f"unacceptable driver state:{_driver.state}",
                                status_code=448)
        elif success:
            if self.state == RUNNING:
                _driver.state = STANDBY_FOR_EXIT
                _driver.is_successed = True
                if self.is_all_state(STANDBY_FOR_EXIT):
                    self.set_all_state(EXITING)
                    self.set_all_successed(True)
            elif self.state == EXITING:
                _driver.state = EXITING
                _driver.is_successed = self.is_successed
            else:
                raise HTTPException(detail=f"unacceptable under state:{self.state}",
                                    status_code=448)
        else:
            self.fail_job()

    def fail_job(self) -> None:
        self.set_all_state(EXITING)
        self.set_all_successed(False)

    def register_task(self,
                      partition_id: PartitionID,
                      root_role_name: RootRoleName,
                      server_url: Url,
                      stage_id: StageID,
                      task_attempt_num: TaskAttemptNum
                      ) -> None:
        if self.state != RUNNING:
            raise HTTPException(detail=f"unable to register task under job state:{self.state}",
                                status_code=450)

        if not (0 <= partition_id < self.partition_num):
            raise HTTPException(detail=f"partition_id:{partition_id} out of scope:[0, {self.partition_num})",
                                status_code=450)

        if root_role_name not in self.root_role_name_set:
            raise HTTPException(detail=f"unknown role_name : {root_role_name}, "
                                       f"expected role_name : {self.root_role_name_set}",
                                status_code=450)

        with self.task_group_dict[partition_id] as _task_group:
            _task_group.register_task(root_role_name, server_url, stage_id, task_attempt_num)

    def task_success(self,
                     partition_id: PartitionID,
                     root_role_name: RootRoleName,
                     server_url: Url,
                     stage_id: StageID,
                     task_attempt_num: TaskAttemptNum
                     ) -> None:
        with self.task_group_dict[partition_id] as _task_group:
            _task_group.task_successed(root_role_name, server_url, stage_id, task_attempt_num)

    def __repr__(self) -> str:
        return f"Job >> job_id:{self.job_id}, job_state:{self.state}"


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
                for _driver in _job.driver_dict.values():
                    if (_driver.state != EXITED) and (_driver.is_heartbeat_expired()):
                        logger.info(f"driver expired {_driver}")
                        if _job.state != EXITING:
                            _job.fail_job()
                        _driver.state = EXITED
                        if _job.is_all_state(EXITED):
                            Job.drop_job(_job.job_id)
                            break

            time.sleep(STATE_CHECKING_INTERVAL)


def _start_server(host: Host, port: Port):

    app = FastAPI()

    @app.post("/driver/register")
    def driver_register(job_id: JobID = Body(...),
                        partition_num: PartitionNum = Body(...),
                        root_role_name_set: Set[RootRoleName] = Body(...),
                        root_role_name: RootRoleName = Body(...)):
        logger.debug(f"driver_register job_id:{job_id}, partition_num:{partition_num}, "
                     f"root_role_name:{root_role_name}, root_role_name_set:{root_role_name_set}")
        _job = Job.get_or_create_job(job_id=job_id,
                                     root_role_name_set=root_role_name_set,
                                     partition_num=partition_num)
        _job.register_driver(root_role_name)
        return SUCCESS_RESPONSE

    @app.post("/driver/wait_for_running")
    def driver_wait_for_running(job_id: JobID = Body(...),
                                root_role_name: RootRoleName = Body(...)):
        logger.debug(f"driver_wait_for_running job_id:{job_id}, root_role_name:{root_role_name}")
        _job = Job.get_job(job_id)
        _driver = _job.get_driver(root_role_name)
        _unregisted_driver = [_d.root_role_name for _d in _job.driver_dict.values() if _d.state == UNREGISTER]
        return JSONResponse(content={'state': _driver.state,
                                     'unregisted_driver': _unregisted_driver})

    @app.post("/driver/heartbeat")
    def driver_heartbeat(job_id: JobID = Body(...),
                         root_role_name: RootRoleName = Body(...)):
        logger.debug("driver_heartbeat job_id:{job_id}, root_role_name:{root_role_name}")
        _job = Job.get_job(job_id)
        _driver = _job.get_driver(root_role_name)
        res_content = {'state': _driver.state}
        if _driver.state == EXITING:
            res_content['is_successed'] = _job.is_successed
            _driver.state = EXITED
            if _job.is_all_state(EXITED):
                Job.drop_job(job_id)
        elif _driver.state in {STANDBY_FOR_RUN, RUNNING, STANDBY_FOR_EXIT}:
            _driver.refresh_heartbeat()
            logger.debug(f"heartbeat refreshed {_driver}")
        else:
            raise HTTPException(detail=f"unacceptable heartbeat under state:{_driver.state}",
                                status_code=441)
        return JSONResponse(content=res_content)

    @app.post("/driver/finish")
    def driver_finish(job_id: JobID = Body(...),
                      root_role_name: RootRoleName = Body(...),
                      success: bool = Body(...)):
        logger.debug(f"driver_finish job_id:{job_id}, root_role_name:{root_role_name}, success:{success}")
        _job = Job.get_job(job_id)
        _job.driver_finish(root_role_name, success)
        return SUCCESS_RESPONSE

    @app.post("/driver/wait_for_finish")
    def driver_wait_for_finish(job_id: JobID = Body(...),
                               root_role_name: RootRoleName = Body(...)):
        logger.debug(f"driver_wait_for_finish job_id:{job_id}, root_role_name:{root_role_name}")
        _job = Job.get_job(job_id)
        _driver = _job.get_driver(root_role_name)
        res_content = {'state': _driver.state}
        if _driver.state == EXITING:
            res_content['is_successed'] = _driver.is_successed
            _driver.state = EXITED
            if _job.is_all_state(EXITED):
                Job.drop_job(job_id)
        return JSONResponse(content=res_content)

    @app.post("/task/register")
    def task_register(job_id: JobID = Body(...),
                      partition_id: PartitionID = Body(...),
                      root_role_name: RootRoleName = Body(...),
                      stage_id: StageID = Body(...),
                      task_attempt_num: TaskAttemptNum = Body(...),
                      task_server_url: Url = Body(...)):
        logger.debug(f"task_register tesk job_id:{job_id}, partition_id:{partition_id}, "
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
        logger.debug(f"task_success job_id:{job_id}, partition_id:{partition_id}, "
                     f"root_role_name:{root_role_name}, task_server_url:{task_server_url}")
        _job = Job.get_job(job_id)
        _job.task_success(partition_id=partition_id,
                          root_role_name=root_role_name,
                          server_url=task_server_url,
                          stage_id=stage_id,
                          task_attempt_num=task_attempt_num)
        return SUCCESS_RESPONSE

    logger.info(f"starting server: http://{host}:{port}")
    DriverStateMonitor().start()
    uvicorn.run(app=app, host=host, port=port, debug=True,
                access_log=True, log_level=logger.level, use_colors=True)


def _get_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default='127.0.0.1')
    parser.add_argument('--port', type=int, default=6609)
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = _get_args()
    _start_server(host=args.host, port=args.port)
