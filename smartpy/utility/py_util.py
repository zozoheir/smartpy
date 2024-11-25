import csv
import os
import time
import zipfile
from datetime import datetime, timedelta
from functools import wraps
from io import BytesIO, TextIOWrapper

import pandas as pd
import psutil
import requests

import smartpy.utility.os_util as os_util
import venv

from smartpy.utility.log_util import getLogger

logger = getLogger(__name__)


def installRequirements(env_path, requirements_file_path):
    if os_util.getOS() == 'Darwin':
        os_util.runCommand(f"{env_path}/bin/pip install -r {requirements_file_path}")
    else:
        os_util.runCommand(f"{env_path}/Scripts/pip.exe install -r {requirements_file_path}")


def createVenvFromRequirements(env_path, requirements_file_path):
    venv.create(env_path, with_pip=True)
    installRequirements(env_path, requirements_file_path)


def getUserAnswer(question: str, choices: list = [], default=""):
    choices_str = '/'.join([str(i) for i in choices]) if choices == [] else ""
    while True:
        continue_or_no = input(f'{question} {choices_str}')
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



def keep_trying(exceptions, retries=3):
    """
    Retry Decorator
    Retries the wrapped function/method `times` times if the exceptions listed
    in ``exceptions`` are thrown
    :param times: The number of times to repeat the wrapped function/method
    :types times: Int
    :param Exceptions: Lists of exceptions that trigger a keep_trying attempt
    :types Exceptions: Tuple of Exceptions
    """
    def decorator(func):
        def newfn(*args, **kwargs):
            attempts = 0
            while attempts < retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if type(e) in exceptions:
                        attempts += 1
                        print(f"Exception of types {type(e)} was raised in {str(func)}")
                        time.sleep(1)
                    else:
                        raise (e)

            return func(*args, **kwargs)

        return newfn

    return decorator





def get_memory_usage():
    process = psutil.Process(os.getpid())
    memory_usage_bytes = process.memory_info().rss
    memory_usage_mb = memory_usage_bytes / (1024 ** 2)
    return memory_usage_mb


def timeit(f):

    def timed(*args, **kw):

        ts = time.time()
        result = f(*args, **kw)
        te = time.time()
        logger.info(f"Function {f.__name__} took {round(te - ts,1)} seconds to execute")
        return result

    return timed

def get_unique(list_of_vars):
    unique_vars = []
    for i in list_of_vars:
        if i not in unique_vars:
            unique_vars.append(i)
    return unique_vars


def stringify_values_recursively(d):
    for key, value in d.items():
        if isinstance(value, dict):
            d[key] = stringify_values_recursively(value)
        elif isinstance(value, list):
            if type(value[0]) == dict:
                d[key] = [stringify_values_recursively(v) for v in value]
        else:
            d[key] = str(value)
    return d



def unzip_file(zip_path, extract_to):
    if not os.path.exists(extract_to):
        os.makedirs(extract_to)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)


def download_zip(zip_url):
    response = requests.get(zip_url)
    response.raise_for_status()
    zip_file = BytesIO(response.content)
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        csv_filename = zip_ref.namelist()[0]
        with zip_ref.open(csv_filename) as csv_file:
            csv_reader = csv.reader(TextIOWrapper(csv_file, 'utf-8'))
            df = pd.DataFrame([row for row in csv_reader])
            df.columns = df.iloc[0]
            df = df[1:]
            return df


def add_row_to_file(file_path,
                    row: list):
    fifo_fd = os.open(file_path, os.O_WRONLY | os.O_NONBLOCK)
    with os.fdopen(fifo_fd, 'w', newline='') as fifo:
        writer = csv.writer(fifo)
        writer.writerow(row)