import re
import math

ISNUM_REGEXP = re.compile(r'^[-+]?([0-9]+|[0-9]*\.[0-9]+)([eE][-+]?[0-9]+)?[ij]?$')


def isNumber(str):
    if ISNUM_REGEXP.match(str) or str == "NaN" or str == "inf":
        return True
    else:
        return False


def numeralWithPrecision(f, n):
    """Returns numeral f with desired precision n. E.g. f = 10.367 and n = 0.01 should return 10.36"""
    n = int(math.log10(1 / float(n)))
    f = math.floor(float(f) * 10 ** n) / 10 ** n

    return int(f) if int(n) == 0 else f

def roundToNearest(x, base=5):
    return base * round(x/base)