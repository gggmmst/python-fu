from functools import partial
import pytz
from pytz import timezone
from datetime import datetime

## ===================================================================
## shortcuts
##

tz_utc = pytz.utc
tz_et = timezone('US/Eastern')
tz_ct = timezone('US/Central')
tz_hk = timezone('Asia/Hong_Kong')

now = datetime.now
now_utc = datetime.utcnow
now_et = partial(now, tz_et)

from_timestamp = datetime.fromtimestamp
fromtimestamp_utc = partial(from_timestamp, tz=tz_utc)
fromtimestamp_et = partial(from_timestamp, tz=tz_et)

# ===================================================================

#def tz_convert(timeobj):
#    return utc.localize(timeobj).astimezone(tz_et).replace(tzinfo=None)


#def tzconvert(timeobj, tz_from, tz_to):
#    return timezone(tz_from).localize(timeobj).astimezone(timezone(tz_to))
#
#
#def tz_timestr_to_utc_timestr(timestr, tz, fmt='%Y-%m-%d %H:%M:%S'):
#    from datetime import datetime
#    dt = datetime.strptime(timestr, fmt)
#    dt = timezone(tz).localize(dt).astimezone(tz_utc)
#    return dt.strftime(fmt)


# resource for timezone MESS!
# http://www.saltycrane.com/blog/2009/05/converting-time-zones-datetime-objects-python/


def dtobj_tzconvert(dtobj, tzstr_from, tzstr_to):
    """\
    Given dtobj, convert timezone from tzstr_from, to tzstr_to
       dtobj means 'datetime object'
       tzstr means 'timezone string'
    Returns dtobj
    """
    tz_from = timezone(tzstr_from)
    tz_to = timezone(tzstr_to)
    # https://docs.python.org/2/library/datetime.html
    # A datetime object d is aware if d.tzinfo is not None and
    # d.tzinfo.utcoffset(d) does not return None. If d.tzinfo is None, or if
    # d.tzinfo is not None but d.tzinfo.utcoffset(d) returns None, d is naive.
    i = dtobj.tzinfo
    aware = (i is not None) and (i.utcoffset(dtobj) is not None)
    if aware:
        return dtobj.replace(tzinfo=tz_from).astimezone(tz_et)
    else:
        return tz_from.localize(dtobj).astimezone(tz_to)
#    try:
#        return tz_from.localize(dtobj).astimezone(tz_to)
#    except ValueError:
#        return dtobj.replace(tzinfo=tz_from).astimezone(tz_et)


def dtstr_tzconvert(dtstr, tzstr_from, tzstr_to, fmt='%Y-%m-%d %H:%M:%S'):
    """\
    Given dtstr, convert timezone from tzstr_from, to tzstr_to
       dtstr means 'datetime string'
    Returns dtstr
    """
    dtobj = datetime.strptime(dtstr, fmt)
    dtobj = dtobj_tzconvert(dtobj, tzstr_from, tzstr_to)
    return dtobj.strftime(fmt)


# TODO need a better name
def _trading_clocks():
    from datetime import timedelta
    return dict(
        usequity = lambda: now(tz_et)-timedelta(hours=9, minutes=30),
        sehk= lambda: now(tz_hk)-timedelta(hours=9, minutes=30),
        cme = lambda: now(tz_ct)-timedelta(hours=8, minutes=30),
        idealpro = lambda: now(tz_et)-timedelta(days=-1, hours=17),
#        fx   = lambda: now(tz_et)-timedelta(days=-1, hours=17, minutes=15),
    )


def trading_date_by_exchange(exchange):
    '''Today's trading date by exhange'''
    f = _trading_clocks().get(exchange)
    return f().date()


def list_exchanges():
    return _trading_clocks().keys()


def list_all_timezones():
    for tz in pytz.all_timezones:
        print tz


def test():
    for k, v in _trading_clocks().iteritems():
        print k, v().date(), v()
    print trading_date_by_exchange('cme')


if __name__ == '__main__':
    list_all_timezones()
    test()
