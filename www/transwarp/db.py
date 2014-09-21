#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Hao Li'

'''
Database operation module.
'''

import time, uuid, functool, threading, logging

# Dict object:

class Dict(dict):
    '''
    Simple dict but support access as x.y style.

    >>> d1 = Dict()
    >>> d1['x'] = 100
    >>> d1.x
    100
    >>> d1.y = 200
    >>> d1['y']
    200
    >>> d2 = Dict(a=1, b=2, c='3')
    >>> d2.c
    '3'
    >>> d2['empty']
    Traceback (most recent call last):
        ...
    KeyError: 'empty'
    >>> d2.empty
    Traceback (most recent call last):
        ...
    AttributeError: 'Dict' object has no attribute 'empty'
    >>> d3 = Dict(('a', 'b', 'c'), (1, 2, 3))
    >>> d3.a
    1
    >>> d3.b
    2
    >>> d3.c
    3
    '''
    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key): #can get Dict.x 
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):#can set Dict.x = a; 
        self[key] = value

def next_id(t=None):
    '''
    Return next id as 50-char string.

    Args:
        t: unix timestamp, default to None and using time.time().
    '''
    if t is None:
        t = time.time()
    return '%015d%s000' % (int(t * 1000), uuid.uuid4().hex)
     #%[flags][width][.precision]type 
     # flag 0: zero padded; 15: width; d: integer; uuid return 32 hex digits

def _profiling(start, sql=''):
	#info log
    t = time.time() - start
    if t > 0.1:
        logging.warning('[PROFILING] [DB] %s: %s' % (t, sql))
    else:
        logging.info('[PROFILING] [DB] %s: %s' % (t, sql))

class DBError(Exception): #define error type
    pass

class MultiColumnsError(DBError):
    pass

class _LasyConnection(object): #put connection and db operation here, don't care transaction

    def __init__(self):
        self.connection = None # have a connection property

    def cursor(self):
        if self.connection is None:
            connection = engine.connect() 
            # now connect to db; engine is global; call engine.connect() to connect db 
            logging.info('open connection <%s>...' % hex(id(connection)))
            #print log of connection id
            self.connection = connection
        return self.connection.cursor() # return a cursor

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback() #undo all changes in current transaction

    def cleanup(self):
        if self.connection:
            connection = self.connection
            self.connection = None # set connection property back to default when quitting
            logging.info('close connection <%s>...' % hex(id(connection)))
            connection.close()

class _DbCtx(threading.local): #manage db connection and also "transaction"!!!
    '''
    Thread local object that holds connection info.
    '''
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return not self.connection is None

    def init(self):
        logging.info('open lazy connection...')
        self.connection = _LasyConnection() #call lasyconnection, return a db connection instance
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup() #self.connection is a _LasyConnection instance, which is a connection instance
        self.connection = None

    def cursor(self):
        '''
        Return cursor
        '''
        return self.connection.cursor()

# thread-local db context:
_db_ctx = _DbCtx()

# global engine object:
engine = None

class _Engine(object): # have a connect property = a function, call _Engine(connect1).connect() = connect1()
#设计此类，从而可以提前设置db引擎，在任意时间调用connect，即可进行db连接，无需重复调用mysql.connector
    def __init__(self, connect):
        self._connect = connect

    def connect(self):
        return self._connect()

def create_engine(user, password, database, host='127.0.0.1', port=3306, **kw):
    import mysql.connector
    global engine
    if engine is not None:
        raise DBError('Engine is already initialized.')
    params = dict(user=user, password=password, database=database, host=host, port=port)
    defaults = dict(use_unicode=True, charset='utf8', collation='utf8_general_ci', autocommit=False)
    for k, v in defaults.iteritems(): 
    #将kw里的设置参数传给param，如果没有，则选择默认值
        params[k] = kw.pop(k, v)
       #kw.pop: if k is in kw, return kw[k] and remove it, else return v
       #by iteration, move setup keys and corrsponding values from kw or defaults into params
    params.update(kw) #将kw里的剩余参数传递给params
    params['buffered'] = True
    engine = _Engine(lambda: mysql.connector.connect(**params))
    #设置engine instance，调用engine.connect()时进行db连接
    # test connection...
    logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))
    
class _ConnectionCtx(object):
    '''
    _ConnectionCtx object that can open and close connection context. _ConnectionCtx object can be nested and only the most 
    outer connection has effect.

    with connection():
        pass
        with connection():
            pass
    '''
    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()

