from urllib import urlopen
from urllib import urlencode

baseurl = 'http://finance.yahoo.com/d/quotes.csv?'

## https://code.google.com/p/yahoo-finance-managed/wiki/enumQuoteProperty
## http://www.gummy-stuff.org/Yahoo-data.htm
big_list_of_yahoo_quote_flags = [['AfterHoursChangeRealtime', 'After Hours Change (Realtime)', 'c8'],
                                 ['AnnualizedGain', 'Annualized Gain', 'g3'],
                                 ['Ask', 'Ask', 'a0'],
                                 ['AskRealtime', 'Ask (Realtime)', 'b2'],
                                 ['AskSize', 'Ask Size', 'a5'],
                                 ['AverageDailyVolume', 'Average Daily Volume', 'a2'],
                                 ['Bid', 'Bid', 'b0'],
                                 ['BidRealtime', 'Bid (Realtime)', 'b3'],
                                 ['BidSize', 'Bid Size', 'b6'],
                                 ['BookValuePerShare', 'Book Value Per Share', 'b4'],
                                 ['Change', 'Change', 'c1'],
                                 ['Change_ChangeInPercent', 'Change Change In Percent', 'c0'],
                                 ['ChangeFromFiftydayMovingAverage', 'Change From Fiftyday Moving Average', 'm7'],
                                 ['ChangeFromTwoHundreddayMovingAverage', 'Change From Two Hundredday Moving Average', 'm5'],
                                 ['ChangeFromYearHigh', 'Change From Year High', 'k4'],
                                 ['ChangeFromYearLow', 'Change From Year Low', 'j5'],
                                 ['ChangeInPercent', 'Change In Percent', 'p2'],
                                 ['ChangeInPercentRealtime', 'Change In Percent (Realtime)', 'k2'],
                                 ['ChangeRealtime', 'Change (Realtime)', 'c6'],
                                 ['Commission', 'Commission', 'c3'],
                                 ['Currency', 'Currency', 'c4'],
                                 ['DaysHigh', 'Days High', 'h0'],
                                 ['DaysLow', 'Days Low', 'g0'],
                                 ['DaysRange', 'Days Range', 'm0'],
                                 ['DaysRangeRealtime', 'Days Range (Realtime)', 'm2'],
                                 ['DaysValueChange', 'Days Value Change', 'w1'],
                                 ['DaysValueChangeRealtime', 'Days Value Change (Realtime)', 'w4'],
                                 ['DividendPayDate', 'Dividend Pay Date', 'r1'],
                                 ['TrailingAnnualDividendYield', 'Trailing Annual Dividend Yield', 'd0'],
                                 ['TrailingAnnualDividendYieldInPercent', 'Trailing Annual Dividend Yield In Percent', 'y0'],
                                 ['DilutedEPS', 'Diluted E P S', 'e0'],
                                 ['EBITDA', 'E B I T D A', 'j4'],
                                 ['EPSEstimateCurrentYear', 'E P S Estimate Current Year', 'e7'],
                                 ['EPSEstimateNextQuarter', 'E P S Estimate Next Quarter', 'e9'],
                                 ['EPSEstimateNextYear', 'E P S Estimate Next Year', 'e8'],
                                 ['ExDividendDate', 'Ex Dividend Date', 'q0'],
                                 ['FiftydayMovingAverage', 'Fiftyday Moving Average', 'm3'],
                                 ['SharesFloat', 'Shares Float', 'f6'],
                                 ['HighLimit', 'High Limit', 'l2'],
                                 ['HoldingsGain', 'Holdings Gain', 'g4'],
                                 ['HoldingsGainPercent', 'Holdings Gain Percent', 'g1'],
                                 ['HoldingsGainPercentRealtime', 'Holdings Gain Percent (Realtime)', 'g5'],
                                 ['HoldingsGainRealtime', 'Holdings Gain (Realtime)', 'g6'],
                                 ['HoldingsValue', 'Holdings Value', 'v1'],
                                 ['HoldingsValueRealtime', 'Holdings Value (Realtime)', 'v7'],
                                 ['LastTradeDate', 'Last Trade Date', 'd1'],
                                 ['LastTradePriceOnly', 'Last Trade Price Only', 'l1'],
                                 ['LastTradeRealtimeWithTime', 'Last Trade (Realtime) With Time', 'k1'],
                                 ['LastTradeSize', 'Last Trade Size', 'k3'],
                                 ['LastTradeTime', 'Last Trade Time', 't1'],
                                 ['LastTradeWithTime', 'Last Trade With Time', 'l0'],
                                 ['LowLimit', 'Low Limit', 'l3'],
                                 ['MarketCapitalization', 'Market Capitalization', 'j1'],
                                 ['MarketCapRealtime', 'Market Cap (Realtime)', 'j3'],
                                 ['MoreInfo', 'More Info', 'i0'],
                                 ['Name', 'Name', 'n0'],
                                 ['Notes', 'Notes', 'n4'],
                                 ['OneyrTargetPrice', 'Oneyr Target Price', 't8'],
                                 ['Open', 'Open', 'o0'],
                                 ['OrderBookRealtime', 'Order Book (Realtime)', 'i5'],
                                 ['PEGRatio', 'P E G Ratio', 'r5'],
                                 ['PERatio', 'P E Ratio', 'r0'],
                                 ['PERatioRealtime', 'P E Ratio (Realtime)', 'r2'],
                                 ['PercentChangeFromFiftydayMovingAverage', 'Percent Change From Fiftyday Moving Average', 'm8'],
                                 ['PercentChangeFromTwoHundreddayMovingAverage', 'Percent Change From Two Hundredday Moving Average', 'm6'],
                                 ['ChangeInPercentFromYearHigh', 'Change In Percent From Year High', 'k5'],
                                 ['PercentChangeFromYearLow', 'Percent Change From Year Low', 'j6'],
                                 ['PreviousClose', 'Previous Close', 'p0'],
                                 ['PriceBook', 'Price Book', 'p6'],
                                 ['PriceEPSEstimateCurrentYear', 'Price E P S Estimate Current Year', 'r6'],
                                 ['PriceEPSEstimateNextYear', 'Price E P S Estimate Next Year', 'r7'],
                                 ['PricePaid', 'Price Paid', 'p1'],
                                 ['PriceSales', 'Price Sales', 'p5'],
                                 ['Revenue', 'Revenue', 's6'],
                                 ['SharesOwned', 'Shares Owned', 's1'],
                                 ['SharesOutstanding', 'Shares Outstanding', 'j2'],
                                 ['ShortRatio', 'Short Ratio', 's7'],
                                 ['StockExchange', 'Stock Exchange', 'x0'],
                                 ['Symbol', 'Symbol', 's0'],
                                 ['TickerTrend', 'Ticker Trend', 't7'],
                                 ['TradeDate', 'Trade Date', 'd2'],
                                 ['TradeLinks', 'Trade Links', 't6'],
                                 ['TradeLinksAdditional', 'Trade Links Additional', 'f0'],
                                 ['TwoHundreddayMovingAverage', 'Two Hundredday Moving Average', 'm4'],
                                 ['Volume', 'Volume', 'v0'],
                                 ['YearHigh', 'Year High', 'k0'],
                                 ['YearLow', 'Year Low', 'j0'],
                                 ['YearRange', 'Year Range', 'w0']]

# build dicts from big_list_of_yahoo_quote_flags
d = dict((e[0], e[-1]) for e in big_list_of_yahoo_quote_flags)
xd = dict((v, k) for k, v in d.iteritems())

flags = ('Symbol',
         'LastTradePriceOnly', 'AskRealtime', 'BidRealtime',
         'Volume', 'AskSize', 'BidSize')
         # 'StockExchange')
         # 'Name', 'LastTradeDate', 'LastTradeTime')

# class YhooQuoteException(Exception):
#     pass

def _url(symbols):
    if not isinstance(symbols, (list, tuple)):
        symbols = (symbols,)
    query = dict(s='+'.join(symbols), f=''.join(d[f] for f in flags))
    return baseurl + urlencode(query)

def _quote(url, header=False):
    """\
    :param str url: query url
    :param bool header: output with header or not; default False
    """
    z = urlopen(url).read()
    if header:
        return ','.join(flags) + '\n' + z
    return z

def quote(symbols, header=False):
    """\
    Quote prices from Yahoo finance webservice; quote once and exit.

    :param list symbols: a list of symbols
    :param bool header: output with header or not; default False
    :return str: price quote

    >>> from yahoo_quote import quote
    >>> print quote('spy', header=True)
    Symbol,LastTradePriceOnly,Volume,AskRealtime,AskSize,BidRealtime,BidSize,StockExchange,Name,LastTradeDate,LastTradeTime
    "SPY",155.48,149687552,155.19,8,155.18,6,"PCX","SPDR S&P 500","4/19/2013","4:00pm"
    >>> print quote('qqq')
    "QQQ",68.09,43066868,68.71,100,0.00,N/A,"NIM","PowerShares QQQ T","4/19/2013","4:00pm"
    """
    url = _url(symbols)
    try:
        q = _quote(url, header)
        q = q.strip()
    except Exception as e:
        q = None
        ## raise
        log.error('Failed to get quote for %s: %s', repr(symbols), e, exc_info=True)
    return q

if __name__ == '__main__':
    from datetime import datetime
    from time import sleep
    now = datetime.now
    while 1:
        print now(), quote('bac', header=True)
        sleep(1)
