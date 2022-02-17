import os
import pickle
from typing import Any
import time
import requests
from requests.exceptions import RequestException


def mf(file_path):  # mkdir for file_path
    _dir = os.path.dirname(file_path)
    _dir and os.makedirs(_dir, exist_ok=True)
    return file_path


def md(dirname):  # mkdir at dirname
    dirname and os.makedirs(dirname, exist_ok=True)
    return dirname


def save_pkl(obj, file_path):
    with open(mf(file_path), "wb") as _f:
        pickle.dump(obj, _f)


def load_pkl(file_path, non_exist='raise'):
    """
    从文件系统中加载python对象
    :param file_path: 文件路径
    :param non_exist: 文件不存在时如何处理：
        str：可选值{'raise', 'None'}
            'raise'：抛出异常
            'None'：返回None
        Any: 可传入任何python对象，文件不存在时返回该对象
    :return: pyton对象
    """
    if os.path.exists(file_path):
        with open(file_path, "rb") as _f:
            return pickle.load(_f)
    elif non_exist == 'raise':
        raise FileNotFoundError(f"not such file : {file_path}")
    elif non_exist == 'None':
        return None
    else:
        return non_exist


def post_pro(check_status_code=True,
             convert_content_type=True,
             retry_times=0,
             retry_interval=3,
             error='raise',
             **kwargs) -> Any:
    try:
        res = requests.post(**kwargs)
        if res.status_code == 200:
            if convert_content_type:
                content_type = res.headers.get('content-type', None)
                if content_type is None:
                    return pickle.loads(res.content)
                elif 'json' in content_type:
                    return res.json()
                elif 'text' in content_type:
                    return res.text
                else:
                    raise Exception(f"unknown content type : {content_type}")
        else:
            if check_status_code:
                raise Exception(f"post status code:{res.status_code} != 200, res text: {res.text}")
            if convert_content_type:
                raise Exception(f"post status code:{res.status_code} != 200, unable to convert type")
        return res
    except Exception as e:
        if isinstance(e, RequestException) and (retry_times > 0):
            time.sleep(retry_interval)
            return post_pro(check_status_code=check_status_code,
                            convert_content_type=convert_content_type,
                            retry_times=retry_times-1,
                            retry_interval=retry_interval,
                            error=error,
                            **kwargs)
        elif error == 'None':
            return None
        else:
            raise e
