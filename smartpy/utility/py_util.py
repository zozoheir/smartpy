import time
from datetime import datetime, timedelta
from functools import wraps


import pandas as pd

import smartpy.utility.os_util as os_util
import venv

def installRequirements(env_path, requirements_file_path):
    if os_util.getOS() == 'Darwin':
        os_util.runCommand(f"{env_path}/bin/pip install -r {requirements_file_path}")
    else :
        os_util.runCommand(f"{env_path}/Scripts/pip.exe install -r {requirements_file_path}")

def createVenvFromRequirements(env_path, requirements_file_path):
    venv.create(env_path, with_pip=True)
    installRequirements(env_path, requirements_file_path)


def getUserAnswer(question:str, choices: list = [], default=""):
    choices_str = '/'.join([str(i) for i in choices]) if choices==[] else ""
    while True:
        continue_or_no = input(f'{question} {choices_str}' )
        if continue_or_no.lower() in [i.lower() for i in choices]:
            return continue_or_no
        elif continue_or_no == "":
            return default

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        if isinstance(lst, pd.DataFrame):
            yield lst.loc[i:i + n]
        else:
            yield lst[i:i + n]



class throttle(object):
    """
    Decorator that prevents a function from being called more than once every
    time period.
    To create a function that cannot be called more than once a minute:
        @throttle(minutes_add=1)
        def my_fun():
            pass
    """

    def __init__(self, seconds=0, minutes=0, hours=0):
        self.throttle_period = timedelta(
            seconds=seconds, minutes=minutes, hours=hours
        )
        self.time_of_last_call = datetime.min

    def __call__(self, fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            now = datetime.now()
            time_since_last_call = now - self.time_of_last_call

            if time_since_last_call > self.throttle_period:
                self.time_of_last_call = now
                return fn(*args, **kwargs)

        return wrapper


def keepTrying(exceptions):
    """
    Retry Decorator
    Retries the wrapped function/method `times` times if the exceptions listed
    in ``exceptions`` are thrown
    :param times: The number of times to repeat the wrapped function/method
    :type times: Int
    :param Exceptions: Lists of exceptions that trigger a keepTrying attempt
    :type Exceptions: Tuple of Exceptions
    """
    def decorator(func):
        def newfn(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if type(e) in exceptions:
                        print(f"Exception of type {type(e)} was raised in {str(func)}")
                        time.sleep(1)
                    else:
                        print(f"Exception type to add is : {type(e)}")
                        raise(e)
            return func(*args, **kwargs)
        return newfn
    return decorator
