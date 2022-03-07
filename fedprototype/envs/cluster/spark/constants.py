from fastapi.responses import JSONResponse

UNREGISTER = 'unregister'
STANDBY_FOR_RUN = 'standby_for_run'
RUNNING = 'running'
STANDBY_FOR_EXIT = 'standby_for_exit'
EXITING = 'exiting'
EXITED = 'exited'

SUCCESS_CODE = 200
SUCCESS_RESPONSE = JSONResponse(content={'msg': 'OK'})