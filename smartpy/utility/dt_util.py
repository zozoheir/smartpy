import datetime as dt
import time
from typing import Union
import pandas as pd
import dateutil.parser
import pytz
from dateutil.tz import tzlocal
from pandas import Timestamp as pdTimestamp

# Datetime formats
FMT_DAY = "%d"
FMT_HR = "%H"
FMT_MIN = "%M"
FMT_SEC = "%M"
FMT_YYYY = "%Y"
FMT_MM = "%m"

FMT_WEEK_OF_YEAR = "%V"
FMT_MONTH_NAME = "%B"
FMT_YMD = "%Y%m%d"
FMT_YMD_DOT = "%Y.%m.%d"
FMT_YMD_DASH = "%Y-%m-%d"
FMT_YMD_SLASH = "%Y/%m/%d"
FMT_YMD_UNDERSCORE = "%Y_%m_%d"
FMT_YMD_HMS = "%Y-%m-%d %H:%M:%S"
FMT_UNIX = "UNIX"
STANDARD_FORMAT = FMT_YMD_HMS

# Timezones
IS_UTC = (dt.datetime.utcnow() - dt.datetime.now()).total_seconds() < 1
CURRENT_TIMEZONE = dt.datetime.now(tzlocal()).tzname()


def now():
    return dt.datetime.now()


def toDatetime(date_time: Union[str, float, dt.datetime, pdTimestamp]) -> dt.datetime:
    if isinstance(date_time, dt.datetime):
        toreturn = date_time
    elif isinstance(date_time, str):
        toreturn = dateutil.parser.parse(date_time)
    elif isinstance(date_time, int):
        # For Unix timestamps
        toreturn = dt.datetime.fromtimestamp(date_time)
    elif isinstance(date_time, pdTimestamp):
        toreturn = date_time.to_pydatetime()
    else:
        raise Exception("Date time input has to be of types datetime, str, float or pandas Timestamp")
    return toreturn


def formatDatetime(date_time: Union[str, float, dt.datetime] = now(), format=STANDARD_FORMAT):
    if format == FMT_UNIX:
        return time.mktime(toDatetime(date_time).timetuple()) + (date_time.microsecond / 1000000.0)
    else:
        return dt.datetime.strftime(toDatetime(date_time), format)


def convertDatetimeTZ(date_time, to_timezone):
    return date_time.astimezone(tz=pytz.timezone(to_timezone))


def localizeDatetime(date_time, to_timezone):
    return pytz.timezone(to_timezone).localize(date_time)


def getCurrentDatetime(wanted_timezone, current_timezone) -> dt.datetime:
    now_current_timezone = localizeDatetime(toDatetime(int(time.time())), current_timezone)
    now_desired_timezone = convertDatetimeTZ(now_current_timezone, wanted_timezone)
    return now_desired_timezone


def getCurrentTimeMicrosUTC() -> float:
    if IS_UTC:
        return time.time()
    else:
        return dt.datetime.utcnow().timestamp()


def toUnixTimestamp(date_time):
    date_time = toDatetime(date_time)
    return int(time.mktime(date_time.timetuple()))


def getPaginationIntervals(start,
                           end,
                           freq):
    start = toDatetime(start)
    end = toDatetime(end)
    ranges = pd.date_range(start=start,
                           end=end,
                           freq=freq)
    intervals = [(toUnixTimestamp(ranges[i]), toUnixTimestamp(ranges[i + 1])) for i in range(len(ranges) - 1)]
    return intervals

