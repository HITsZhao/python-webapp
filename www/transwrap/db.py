#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Zhao'

'''
Database operation module
'''

import time, uuid, functools, threading, logging

class Dict(dict):
	'''
	Simple dict but support access as x.y style
	1. same with setting dict attribute
	>>> d1 = Dict()
	>>> d1['x'] = 100
	>>> d1.x
	100
	2. __SetAttr__ of Dict
	>>>d1.y = 200
	>>>d1['y']
	200
	3. __init__(**kw) of Dict
	>>>d2 = Dict(a=1, b=2, c='3')
	>>>d2.c
	'3'
	4. __init__(*arg) of Dict
	>>>d3 = Dict(('a', 'b', 'c'),(1,2,3))
	>>>d3.a
	1
	5. Exception:
	>>>d3['empty']
	Traceback(most recent call last):
	...
	KeyError: 'empty'
	'''
	
	def __init__(self,names=(),values=(),**kw):
		super(Dict,self).__init__(**kw)
		for k, v in zip(names,values):
			self[k] = v

	def __getattr__(self,key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"Dict object has no attribute '%s'" % key)

	def __setattr__(self,key,value):
		self[key] = value

def _profiling(start, sql=''):
	'''
	AOP: aspect of log
	'''
	t = time.time() - start
	if t > 0.1:
		logging.warning('[PROFILING] [DB] %s: %s' % (t, sql))
	else:
		logging.info('[PROFILING] [DB] %s: %s' % (t, sql))


class DBError(Exception):
	pass

class MultiColomnsError(DBError):
	pass

# golbal engine object
engine = None
# _Engine类的目的在于保存mysql.connector.connect返回的连接对象，对于该进程而言，只有一个连接
# 可以使用全局变量engine来保存该连接,由于creat_engine表示建立与数据库的连接,因而，只是在建立连接时调用一次

class _Engine(object):
	def __init__(self, connect):
		self.__connect = connect
	
	def connect(self):
		return self.connect()

def creat_engine(user, password, database, host='127.0.0.1', port=3306,**kw):
	import mysql.connector
	global engine
	if engine is not None:
		raise DBError('engine is already initialized')
	params = dict(user=user,password=password,database=database,host=host,port=port)
	defaults = dict(use_unicode=True, charset='utf-8', collation='utf8_general_ci', autocommit=False)
	for k,v in defaults.iteritems():
		params[k] = kw.pop(k,v)			# add defaults items in kw in to dict params
	params.update(kw)					# update params item with kw
	params['buffered'] = True
	engine = _Engine(lambda: mysql.connector.connect(**params))

	logging.info('Init mysql engine <%s> ok' %hex(id(engine)))

# _LazyConnection 类属于行为类：首先通过调用全局变量engine获得连接，通过连接获取到该连接
# 的cursor（_LazyConnection.cursor()）,同时，通过该类还能进行commit,rollback,cleanup操作
# 因此，该类属于对数据库基本操作的封装类

class _LazyConnection(object):
	'''
	lazyConnection with method: cursor, commit, rollback, and cleanup

	>>>_LazyConnection.cursor()
	get database connection from global engine, and return connection.cursor
	>>>_LazyConnection.commit()
	excecute the method connection.commit() to commit changes to database
	>>>_LazyConnection.rollback()
	excecute the method connnection.rollback() to rollback of database
	>>>_LazyConnection.cleanup()
	excecute the method connection.close() to close connection to database
	'''
	def __init__(self):
		self.connection = None
	
	def cursor(self):
		if self.connnection is None:
			connection = engine.connection()
			logging.info("open connection <%s>..." % hex(id(connection)))
			self.connection = connection
		return self.connection.cursor()

	def commit(self):
		return self.connection.commit()
	
	def rollback(self):
		return self.connection.rollback()

	def cleanup(self):
		if self.connection:
			connection = self.connection
			self.connection = None
			logging.info("close connection <%s>..." % connection)
			connection.close()

# thread local db context
_db_ctx = _DbCtx()

# _DbCtx()类是线程局部对象, 该类中对数据库的连接和事务进行了封装，定义了基本操作:
# init(): 通过_LazyConnection获取connection的操作方法，
# cleanup(): 操作结束(即关闭连接)
# cursor(): 获取connection的cursor

class _DbCtx(threading.local):
	'''
	Thread local object that holds connection info
	'''
	def __init__(self):
		self.connection 	= None
		self.transactions	= 4

	def is_init(self):
		return not self.connection is None

	def init(self):
		logging.info("open lazy connection...");
		self.connection 	= _LazyConnection()
		self.transactions 	= 0
	
	def cleanup(self):
		self.connection.cleanup()
		self.connection = None
	
	def cursor(self):
		return self.connection.cursor()

# _ConnectionCtx() 类是对线程全局变量_db_ctx的进一步封装：
# 可以用with 语句使用_db_ctx, 自动完成开始时_db_ctx的初始化和结束时的清理工作
class _ConnectionCtx(object):
	'''
	_ConnectionCtx object can be used with 'with'
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
	
	def __exit__(self,exctype,excvalue,traceback):
		global _db_ctx
		if self.should_cleanup:
			_db_ctx.cleanup()

#	定义_ConnectionCtx的操作方法（因为_ConnectionCtx为私有）
def connection()
	'''
	return _ConnectionCtx that can be used with "with"
	>>> connect = connection()
	>>> with connect:
			pass
	'''
	return _ConnectionCtx()


# 定义装饰器: 即对于某个特定的数据库操作函数都按照如下形式进行：
# with _ConnectCtx():
#	foo();
#

def with_connection(func):
	'''
	Decorator for reuse connection

	@with_connection
	def foo(*args, **kw):
		f1()
		f2()
		f3()
	'''
	@functools.wraps(func)
	def _wrapper(*args,**kw):
		with _ConnectionCtx():
			return func(*args, **kw)
	return _wrapper


















































