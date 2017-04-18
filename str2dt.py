import logging
from itertools import product
from datetime import datetime
from pytz import timezone
from functools import partial

#logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)


#prototype = lambda: partial(datetime.now, timezone('US/Eastern'))().strftime('%Y.%m.%d')
def datestr_today(tz, fmt='%Y.%m.%d'):
    return partial(datetime.now, timezone(tz))().strftime(fmt)
datestr_today_et = partial(datestr_today, 'US/Eastern')


date_seps = '.-'
date_dirs = [('%Y', '%m'), ('%Y', '%m', '%d')]
date_fmts = [sep.join(t) for sep, t in product(date_seps, date_dirs)]
#fmt_date = ['%Y-%m-%d', '%m-%d', '%Y.%m.%d', '%m.%d']

time_seps = '.:'
time_dirs = [('%H', '%M'), ('%H', '%M', '%S'), ('%H', '%M', '%S', '%f')]
time_fmts = [sep.join(t) for sep, t in product(time_seps, time_dirs)]
time_fmts.append('%H:%M:%S.%f')
#fmt_time = ['%H:%M:%S', '%H:%M', '%H.%M.%S', '%H.%M']

def _timestr2datetime(str):
    datetime_fmts = [' '.join(x) for x in product(date_fmts, time_fmts)]
#    fmts_datetime = [' '.join(x) for x in product(fmt_date, fmt_time)]
#    LOG.debug('Possible time str formats %s', repr(fmts_datetime))

    td = datestr_today_et()     # today in eastern time

#    for fmt in fmts_datetime:
    for fmt in datetime_fmts:
        # try-block 1: str with both date and time
        try:
            t = datetime.strptime(str, fmt)
        except ValueError:
            pass
        else:
            return t
        # try-block 2: str with only time, assume date is today
        try:
            x = '%s %s' % (td, str)
            t = datetime.strptime(x, fmt)
        except ValueError:
            pass
        else:
            return t

    LOG.warn('Unknown time string format, failed to convert %s', str)
    return None


str2dt = _timestr2datetime


def str2dtstr(str, fmt='%Y-%m-%d %H:%M:%S'):
    dt = str2dt(str)
    return dt.strftime(fmt)


def test():
    f = str2dt
    print f('2015.03.03 10:01')
    print f('2015.03.03 10.01.22.999')
    print f('2015.03.03 10:01:22.777')
    print f('09:09')
    print f('09:09:11')
    print f('13.11')
    print type(f('13.11'))

    f = str2dtstr
    print f('13.11')
    print type(f('13.11'))

    print f('13.11.00.123', fmt='%Y-%m-%d %H:%M:%S.%f')


if __name__ == '__main__':
    test()
