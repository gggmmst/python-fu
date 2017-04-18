import logging
import psycopg2 as pg
from contextlib import contextmanager


LOG = logging.getLogger(__name__)


# ============================================================
# api

def pgsql_handle(db, **kw):
    return _PgSqlConn(db, **kw)


@contextmanager
def pgsql_cursor(db, **kw):
    '''a read-only handle'''
    db = db
    h = _PgSqlConn(db, **kw)
    try:
        yield h.cursor
    finally:
        h.cursor.close()
        h.conn.close()
        LOG.info('Postgresql connection to db [%s] closed cleanly', db)


def ifetch(cursor):
    while 1:
        y = cursor.fetchone()
        if y is None:
            raise StopIteration
        yield y


# ============================================================
# classes

class DBConn(object):
    """Abstract database connection class"""

    host = None
    port = None
    user = None
    pswd = None
    db = None

    def __init__(self, db, **kw):
        self.db = db or self.db
        assert self.db is not None
        self.host = kw.get('host') or self.host
        self.port = kw.get('port') or self.port
        self.user = kw.get('user') or self.user
        self.pswd = kw.get('pswd') or self.pswd
        self._conn = self._connect()

    def _connect(self):
        raise NotImplementedError()

    @property
    def conn(self):
        return self._conn

    @property
    def cursor(self):
        return self._conn.cursor()


#
# PostgreSQL
# by
# psycopg2
# http://initd.org/psycopg/docs/
#
class PgSqlConnException(Exception):
    pass


class _PgSqlConn(DBConn):
    """Postgresql implementation of database connection"""

    host = 'localhost'
    port = 5432

    def _connect(self):
        conn_string = '''host={host} port={port} user={user} password={pswd} dbname={db}'''.format(
                host=self.host,
                port=self.port,
                user=self.user,
                pswd=self.pswd,
                db=self.db,
            )
        try:
            conn = pg.connect(conn_string)
        except Exception as e:
            LOG.error('Failed to connect to database', exc_info=True)
            raise PgSqlConnException()
        return conn


# ============================================================
# main/testings

if __name__ == '__main__':
    from pprint import pprint
    h = pgsql_handle('testdb', user='pgsql', pswd='supersecretwithsalt')
    c = h.cursor
    conn = h.conn
    tablename = 'dummy'
    tbl_create = '''\
    CREATE TABLE IF NOT EXISTS {table} (
      "id"   serial NOT NULL,
      "name" varchar DEFAULT NULL,
      "age"  integer DEFAULT NULL,
      PRIMARY KEY ("id")
    )'''.format(table=tablename)
    tbl_insert = """\
    INSERT INTO {table}
      (name, age)
    VALUES
      (%s, %s)""".format(table=tablename)
    c.execute(tbl_create)
    c.execute(tbl_insert, ('Ada', 28))
    c.execute(tbl_insert, ('Jac', 29))
    c.execute(tbl_insert, ('Jim', 30))
    conn.commit()
    c.execute("""SELECT * from {table}""".format(table=tablename))
    pprint(c.fetchall())
