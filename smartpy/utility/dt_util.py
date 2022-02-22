import datetime as dt
import time
from typing import Union

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
    elif isinstance(date_time, float):
        toreturn = dt.datetime.fromtimestamp(date_time)
    elif isinstance(date_time, pdTimestamp):
        toreturn = date_time.to_pydatetime()
    else:
        raise Exception("Date time input has to be of type datetime, str, float or pandas Timestamp")
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


def getCurrentDatetime(timezone='UTC') -> dt.datetime:
    now = localizeDatetime(toDatetime(time.time()), CURRENT_TIMEZONE)
    return convertDatetimeTZ(now, timezone)

def getCurrentTimeMicrosUTC() -> float:
    if IS_UTC:
        return time.time()
    else:
        return dt.datetime.utcnow().timestamp()

