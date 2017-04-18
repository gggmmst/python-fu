#!/usr/bin/python

# ------------------------------------------------------------
# imports

## gevent monkey patch
from gevent import monkey; monkey.patch_all()

## python standard packages
import logging
import re
from functools import partial
from datetime import datetime, timedelta
from pytz import timezone

# non standard packages
from gevent import sleep
from gevent.pool import Pool

## project packages
from yahoo_quote import quote, flags

#from util.database import mysql_handle
# fake class
class mysql_handle(object):
    def __init__(self, *a, **kw):
        pass
    @property
    def conn(self):
        pass
    @property
    def cursor(self):
        pass

# ------------------------------------------------------------
# global stuff

DEBUG = 0

# logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M')
log = logging.getLogger(__name__)

## current new york time
now_in_nytz = partial(datetime.now, timezone('US/Eastern'))

tau15m = timedelta(minutes=-15)     ## quotes from yahoo finance is 15 minutes delayed

pool = Pool(1000)

def _header():
    """return column headers"""
    return flags


# ------------------------------------------------------------
# classes

class QuoteBase(object):
    """Quote abstract base class"""

    symbols = None
    interval = 5  ## quote frequency in seconds

    def on_recv(self, quote, *a, **kw):
        raise NotImplementedError()

    def run(self):
        if self.symbols is None:
            log.warn('No symbols to quote')
            return
        while 1:
            q = quote(self.symbols)
            if DEBUG:
                log.debug(q)
            self.on_recv(q)
            sleep(self.interval)


class QuoteToStdout(QuoteBase):
    """Toy"""
    def on_recv(self, quote, *a, **kw):
        if quote is None:
            print '*** Quote Error ***'
        # print nytnow() + tau15m
        print now_in_nytz() + tau15m
        print quote


class QuoteToMySql(QuoteBase):
    """Base class that record quotes to MySQL db"""

    db = None

    def __init__(self):
        assert self.db is not None
        h = mysql_handle(self.db)
        self.conn = h.conn
        self.cursor = h.cursor


class QuoteToMySqlUSStock(QuoteToMySql):
    """Record US stock quotes to MySQL db"""

    db = 'stock_us_seconds'

    table_create = """\
    CREATE TABLE IF NOT EXISTS {table} (
      date      DATE,
      datetime  DATETIME PRIMARY KEY,
      px        FLOAT,
      ask       FLOAT,
      bid       FLOAT,
      volume    INT,
      ask_sz    INT,
      bid_sz    INT
    )"""

    table_exist = """SHOW TABLES LIKE '{table}'"""

    table_insert = """\
    INSERT INTO {table}
      (date, datetime, px, ask, bid, volume, ask_sz, bid_sz)
    VALUES
      (%s, %s, %s, %s, %s, %s, %s, %s)"""

    tables = {}

    _re_comma = re.compile(r',')
    _re_mess = re.compile(r'(\d{1,3}(?:,\d\d\d)+|\d{1,3}|N/A),(\d{1,3}(?:,\d\d\d)+|\d{1,3}|N/A)')

    def _messy_split(self, mess):
        """Split mess to (ask_sz, bid_sz)

        first step: split
        '100,300'         -> ['100', '300']
        '0,N/A'           -> ['0', 'N/A']
        '1,000,N/A'       -> ['1,000', 'N/A']
        'N/A,3,300'       -> ['N/A', '3,300']
        '2,000,5,500'     -> ['2,000', '5,000']
        '1,000,0'         -> ['2,000', '0']
        '100,000,200,000' -> ['100,000,200', '000']   TODO fix!!! should be ['100,000', '200,000']

        second step: remove comma
        '100'             -> '100'
        '2,000'           -> '2000'
        '3,000,000'       -> '3000000'
        """

        # nice and clean, just one comma
        clean = mess.split(',')
        if len(clean) == 2:
            return [self._re_comma.sub('', x) for x in clean]

        # messy
        poop = self._re_mess.split(mess)[1:-1]
        if len(poop) != 2:
            log.error('Failed to split: %s -> %s', mess, repr(poop), exc_info=True)
            raise Exception('Failed to split string.')
        return [self._re_comma.sub('', x) for x in poop]

    def _raw_quote_to_tablename_and_values(self, quote):
        """Parse quote to (tablename, insert_vaules)"""

        symbol, px, ask, bid, volume, mess = quote.strip().split(',', 5)
        symbol = symbol.strip('"').lower()
        ask_sz, bid_sz = self._messy_split(mess)

        # >>> print now_in_nytz()
        # >>> 2013-04-20 11:03:19.009426-04:00
        t = str(now_in_nytz()+tau15m)

        # prefix values with date and datetime
        values = [t[:10], t[:19], px, ask, bid, volume, ask_sz, bid_sz]

        return symbol, values

    def _create(self, table):
        """Create table in db"""
        stmt = self.table_create.format(table=table)
        if DEBUG:
            log.debug(stmt)
        self.cursor.execute(stmt)
        # commit: create table immediately
        self.conn.commit()
        # log.info('Table %s created', table)

    def _insert(self, table, values):
        """Insert values into table"""
        stmt = self.table_insert.format(table=table)
        if DEBUG:
            log.debug(stmt)
            log.debug(values)
        self.cursor.execute(stmt, values)
        # TODO need to commmit every time?
        self.conn.commit()
        # log.info('Inserted %s to %s', values, table)

    def _handle_quote(self, quote):
        """A function to be spawned by greenlets"""

        table, values = self._raw_quote_to_tablename_and_values(quote)

        # check if table exists in cache
        if self.tables.get(table) is None:

            # check if table exists in db
            stmt = self.table_exist.format(table=table)
            if DEBUG:
                log.debug(stmt)
            self.cursor.execute(stmt)

            # if table does not exist, then create table
            if self.cursor.fetchone() is None:
                self._create(table)

            # add table to cache
            self.tables[table] = 1

        # insert values to db
        self._insert(table, values)

    def on_recv(self, raw_quotes, *a, **kw):
        quotes = raw_quotes.strip().split('\r\n')
        for quote in quotes:
            # spawn greenlets to write to db
            pool.spawn(self._handle_quote, quote)
            # pool.spawn(self.conn.commit)


# class QuoteETF(QuoteToMySqlUSStock):
class QuoteETF(QuoteToStdout):
    symbols = ('spy', 'qqq', 'dia', 'iwm', 'vxx', 'gld', 'iau', 'slv')

# class QuoteETF(QuoteToMySqlUSStock):
class QuoteSpyder(QuoteToStdout):
    symbols = ('xlf', 'xlk', 'xke', 'xlv', 'xlu', 'xly', 'xlp', 'xli')

# class QuoteEnergy(QuoteToMySqlUSStock):
class QuoteEnergy(QuoteToStdout):
    symbols = ['apa', 'apc', 'bhi', 'bjs', 'btu', 'cam', 'chk', 'cnx', 'cog', 'cop', 'cvx',
        'dnr', 'do', 'dvn', 'eog', 'ep', 'esv', 'fti', 'hal', 'hes', 'mee', 'mro', 'mur',
        'nbl', 'nbr', 'nov', 'oxy', 'pxd', 'rdc', 'rrc', 'se', 'sii', 'slb', 'sun', 'swn',
        'tso', 'vlo', 'wmb', 'xom', 'xto']

# class QuoteMaterials(QuoteToMySqlUSStock):
class QuoteMaterials(QuoteToStdout):
    symbols = ['aa', 'aks', 'apd', 'ati', 'bll', 'bms', 'cf', 'dd', 'dow',
        'ecl', 'emn', 'fcx', 'fmc', 'iff', 'ip', 'mon', 'mwv', 'nem', 'nue',
        'oi', 'ppg', 'ptv', 'px', 'see', 'sial', 'tie', 'vmc', 'wy', 'x']

# class QuoteIndustrials(QuoteToMySqlUSStock):
class QuoteIndustrials(QuoteToStdout):
    symbols = ['avy', 'ba', 'bni', 'cat', 'cbe', 'chrw', 'cmi', 'col', 'csx', 'ctas',
        'de', 'dhr', 'dnb', 'dov', 'efx', 'emr', 'etn', 'expd', 'fast', 'fdx', 'flr', 'fls',
        'gd', 'ge', 'gr', 'gww', 'hon', 'irm', 'itt', 'itw', 'jec', 'lll', 'lmt', 'luv',
        'mas', 'mmm', 'mtw', 'mww', 'noc', 'nsc', 'pbi', 'pcar', 'pcp', 'ph', 'pll', 'pwr',
        'r', 'rhi', 'rok', 'rrd', 'rsg', 'rtn', 'srcl', 'txt', 'unp', 'ups', 'utx', 'wm']

# class QuoteConsumerDiscretionary(QuoteToMySqlUSStock):
class QuoteConsumerDiscretionary(QuoteToStdout):
    symbols = ['amzn', 'an', 'anf', 'apol', 'azo', 'bbby', 'bby', 'bdk', 'big', 'cbs',
        'ccl', 'cmcsa', 'coh', 'dhi', 'dis', 'dri', 'dtv', 'dv', 'ek', 'expe',
        'f', 'fdo', 'fo', 'gci', 'gme', 'gpc', 'gps', 'gt', 'har', 'has', 'hd', 'hog', 'hot', 'hrb',
        'igt', 'ipg', 'jci', 'jcp', 'jwn', 'kbh', 'kss', 'leg', 'len', 'low', 'ltd',
        'm', 'mar', 'mat', 'mcd', 'mdp', 'mhp', 'nke', 'nwl', 'nwsa', 'nyt', 'odp', 'omc', 'orly',
        'phm', 'rl', 'rsh', 'sbux', 'shld', 'shw', 'sna', 'sni', 'spls', 'swk',
        'tgt', 'tif', 'tjx', 'twc', 'twx', 'vfc', 'viab', 'whr', 'wpo', 'wyn', 'wynn', 'yum']

# class QuoteConsumerStaples(QuoteToMySqlUSStock):
class QuoteConsumerStaples(QuoteToStdout):
    # removed 'bfb' from symbols; it has symbol "BF-B" from yahoo webservice, which contain invalid character "-" for tablename
    symbols = ['adm', 'avp', 'cag', 'cce', 'cl', 'clx', 'cost', 'cpb', 'cvs',
        'df', 'dps', 'el', 'gis', 'hnz', 'hrl', 'hsy', 'k', 'kft', 'kmb', 'ko', 'kr',
        'lo', 'mkc', 'mo', 'pbg', 'pep', 'pg', 'pm', 'rai', 'sjm', 'sle', 'stz', 'svu', 'swy', 'syy',
        'tap', 'tsn', 'wag', 'wfmi', 'wmt']

# class QuoteHealthCare(QuoteToMySqlUSStock):
class QuoteHealthCare(QuoteToStdout):
    symbols = ['abc', 'abt', 'aet', 'agn', 'amgn', 'bax', 'bcr', 'bdx', 'biib', 'bmy', 'bsx',
        'cah', 'celg', 'ceph', 'ci', 'cvh', 'dgx', 'dva', 'esrx', 'frx', 'genz', 'gild',
        'hsp', 'hum', 'isrg', 'jnj', 'kg', 'lh', 'life', 'lly', 'mck', 'mdt', 'mhs', 'mil', 'mrk', 'myl',
        'pdco', 'pfe', 'pki', 'rx', 'sgp', 'stj', 'syk', 'thc', 'tmo', 'unh', 'var',
        'wat', 'wlp', 'wpi', 'wye', 'xray', 'zmh']

# class QuoteFinancials(QuoteToMySqlUSStock):
class QuoteFinancials(QuoteToStdout):
    # removed 'key' and 'all' from symbols, they are keywords in mysql
    symbols = ['afl', 'aig', 'aiv', 'aiz', 'amp', 'aoc', 'avb', 'axp',
        'bac', 'bbt', 'ben', 'bk', 'bxp', 'c', 'cb', 'cbg', 'cinf', 'cma', 'cme', 'cof', 'dfs',
        'eqr', 'etfc', 'fhn', 'fii', 'fitb', 'gnw', 'gs', 'hban', 'hcbk', 'hcn', 'hcp', 'hig', 'hst',
        'ice', 'ivz', 'jns', 'jpm', 'kim', 'l', 'lm', 'lnc', 'luk',
        'mbi', 'mco', 'met', 'mi', 'mmc', 'ms', 'mtb', 'ndaq', 'ntrs', 'nyx',
        'pbct', 'pcl', 'pfg', 'pgr', 'pld', 'pnc', 'pru', 'psa', 'rf',
        'schw', 'slm', 'spg', 'sti', 'stt', 'tmk', 'trow', 'trv', 'unm', 'usb',
        'vno', 'vtr', 'wfc', 'xl', 'zion']

# class QuoteInformationTechnology(QuoteToMySqlUSStock):
class QuoteInformationTechnology(QuoteToStdout):
    symbols = ['a', 'aapl', 'acs', 'adbe', 'adi', 'adp', 'adsk', 'akam', 'altr', 'amat', 'amd', 'aph',
        'bmc', 'brcm', 'ca', 'cien', 'cpwr', 'crm', 'csc', 'csco', 'ctsh', 'ctxs', 'cvg',
        'dell', 'ebay', 'emc', 'erts', 'fis', 'fisv', 'flir', 'glw', 'goog', 'hpq', 'hrs',
        'ibm', 'intc', 'intu', 'java', 'jbl', 'jdsu', 'jnpr', 'klac', 'lltc', 'lsi', 'lxk',
        'ma', 'mchp', 'mfe', 'molx', 'mot', 'msft', 'mu', 'novl', 'nsm', 'ntap', 'nvda', 'nvls',
        'orcl', 'payx', 'qcom', 'qlgc', 'rht', 'sndk', 'symc', 'tdc', 'ter', 'tlab', 'tss', 'txn',
        'vrsn', 'wdc', 'wfr', 'wu', 'xlnx', 'xrx', 'yhoo']

# class QuoteTelecommunicationServices(QuoteToMySqlUSStock):
class QuoteTelecommunicationServices(QuoteToStdout):
    symbols = ['amt', 'ctl', 'ftr', 'pcs', 'q', 's', 't', 'vz', 'win']

# class QuoteUtilities(QuoteToMySqlUSStock):
class QuoteUtilities(QuoteToStdout):
    symbols = ['aee', 'aep', 'aes', 'aye', 'ceg', 'cms', 'cnp', 'd', 'dte', 'duk', 'dyn',
            'ed', 'eix', 'eqt', 'etr', 'exc', 'fe', 'fpl', 'gas', 'ni', 'nu',
            'pcg', 'peg', 'pgn', 'pnw', 'pom', 'ppl', 'scg', 'so', 'sre', 'str',
            'te', 'teg', 'wec', 'xel']

# ------------------------------------------------------------
# testing functions

def test1():
    from gevent import joinall, spawn

    etf = QuoteETF()
    spyder = QuoteSpyder()
    financial = QuoteFinancials()
    tech = QuoteInformationTechnology()

    joinall([
        spawn(etf.run),
        spawn(spyder.run),
        spawn(financial.run),
        spawn(tech.run),
    ])

def test2():
    from gevent import joinall, spawn

    etf = QuoteETF()
    spyder = QuoteSpyder()

    e = QuoteEnergy()
    m = QuoteMaterials()
    i = QuoteIndustrials()
    cd = QuoteConsumerDiscretionary()
    cs = QuoteConsumerStaples()
    hc = QuoteHealthCare()
    it = QuoteInformationTechnology()
    f = QuoteFinancials()
    ts = QuoteTelecommunicationServices()
    u = QuoteUtilities()

    joinall([
        spawn(etf.run),
        spawn(spyder.run),
        spawn(e.run),
        spawn(m.run),
        spawn(i.run),
        spawn(cd.run),
        spawn(cs.run),
        spawn(hc.run),
        spawn(it.run),
        spawn(f.run),
        spawn(ts.run),
        spawn(u.run),
    ])

# ------------------------------------------------------------
# main

if __name__ == '__main__':
    test1()
    # test2()

