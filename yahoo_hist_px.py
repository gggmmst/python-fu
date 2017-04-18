#!/usr/bin/python

# resources
# http://www.gummy-stuff.org/Yahoo-data.htm
# https://code.google.com/p/yahoo-finance-managed/wiki/YahooFinanceAPIs
# https://code.google.com/p/yahoo-finance-managed/wiki/enumQuoteProperty

import urllib
import re

##
## api call
##

def hist_px(symbol, from_date, to_date):
    """\
    :param str symbol: yahoo finance symbol
    :param str from_date: yyyymmdd format
    :param str to_date: yyyymmdd format
    :return: Date, Open, High, Low, Close, Volume, Adj Close
    :Example:

    hist_px('goog', '20120101', '20130101')
    """
    q = dict(s=symbol, g='d', ignore='.csv')
    q.update(fromdate_to_querydict(from_date))
    q.update(todate_to_querydict(to_date))
    url = baseurl + urllib.urlencode(q)
    return urllib.urlopen(url).read()

##
## internal functions
##

baseurl = 'http://ichart.yahoo.com/table.csv?'

def parse_date(date_str):
    yyyy, mm, dd = map(int, re.split(r'''(....)(..)(..)''', date_str)[1:-1])
    return mm-1, dd, yyyy

def fromdate_to_querydict(date_str):
    f = ('a', 'b', 'c')
    return dict(zip(f, parse_date(date_str)))

def todate_to_querydict(date_str):
    f = ('d', 'e', 'f')
    return dict(zip(f, parse_date(date_str)))

##
## main
##

if __name__ == '__main__':
    print hist_px('goog', '20120101', '20121231')
