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
	>>> d1 = Dict()
	>>> d1['x'] = 100
	>>> d1.x
	100
	>>>d1.y = 200
	>>>d1['y']
	200
	>>>d2 = Dict(a=1, b=2, c='3')
	>>>d2.c
	'3'
	>>>d3 = Dict(('a', 'b', 'c'),(1,2,3))
	>>>d3.a
	1
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
		self.transactions	= 0

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
	connect = connection()
	with connect:
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


# 对select操作的封装: 输入sql语句和tuple形式的参数, first == True 表示取下一个值，first == False 表示取所有值

def _select(sql, first, *args):
	'''
	execute select SQL with args
	if first:
		fetchone()
	else:
		fetchall()
	'''
	global _db_ctx
	cursor = None
	sql = sql.replace('?', '%s') #使用?占位符，防止注入攻击
	logging.info('SQL: %s, ARGS: %s', % (sql, args))
	try:
		cursor = _db_ctx.connection.cursor()
		cursor.execute(sql,args)
		if cursor.description:
			names = [x[0] for x in cursor.description]
		if first:
			values = cursor.fetchone()
			if not values:
				return None
			return Dict(names,values)
		return [Dict(names,x) for x in cursor.fetchall()]
	finally:
		if cursor:
			cursor.close()

# 执行sql语句，并返回一个值
@with_connection
def select_one(sql, *args):
	'''
	excecute sql with args and expected one result
	if no result found, return None
	if multi results found, return one
	
	i.e.
	>>> u1 = dict(id=100,name='Alice',email='alice@org.net',passwd='Abc-12345',last_modified=time.time())
	>>> u2 = dict(id=101,name='Sarah',email='Sarah@org.net',passwd='Abc-12345',last_modified=time.time())
	>>> insert('user',**u1)
	1
	>>> insert('user',**u2)
	1
	>>> u = select_one('select * from user where id=?',100)
	>>> u.name
	u'Alice'
	'''
	return _select(sql,True,*args)

@with_connection
def select(sql,*args):
	'''
	Execute select sql and return list, if no result,return empty list
	'''
	return _select(sql,False,*args)

@with_connection
def _update(sql, *args):
	global _db_ctx
	cursor = None
	sql = sql.replace('?', '%s')
	logging.info('SQL:%s, ARGS: %s' % (sql,args))
	try:
		cursor = _db_ctx.connection.cursor()
		cursor.execute(sql,args)
		r = cursor.rowcount
		if _db_ctx.transaction == 0:
			logging.info('auto commit')
			_db_ctx.connection.commit()
		return r
	finally:
		if cursor:
			cursor.close()

def insert(table, **kw):
	'''
	Execute insert SQL.
	>>> u1 = dict(id=2000, name='Bob', email='bob@test.org', passwd='bobobob', last_modified=time.time())
	>>> insert('user',**u1)
	1
	>>> u2 = select_one('select * from user where id=?',2000)
	>>> u2.name
	Bob
	'''
	cols,args = zip(*kw.iteritems())
	sql = 'insert into `%s` (%s) values (%s)' % (table, ','.join(['`%s`' % col for col in cols]), ','.join(['?' for i in range(len(cols))]))
	return _update(sql, *args)

def update(sql, *args):
	return _update(sql, *args)


class _TransactionCtx(object):
	'''
	_TransactionCtx object that can handle transactions

	with _TransactionCtx():
		pass
	'''

	def __enter__(self):
		global _db_ctx
		self.should_close_conn = False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_close_conn = True
		_db_ctx.transactions = _db_transactions + 1
		logging.info('begin transaction...' if _db_ctx.transaction == 1 else 'join current transaction')
		return self

	def __exit__(self, exctype, excvalue, traceback):
		global _db_ctx
		_db_ctx.transactions = _db_ctx.transactions - 1
		try:
			if _db_ctx.tansactions == 0:
				if exctype is None:
					self.commit()
				else:
					self.rollback()

		finally:
			if self.should_close_conn:
				_db_ctx.cleanup()



	def commit(self):
		global _db_ctx
		logging.info('commit transaction...')
		try:
			_db_ctx.connection.commit()
			logging.info('commit ok.')
		except:
			logging.warning('commit failed, try rollback...')
			_db_ctx.rollback()
			logging.warning('rollback ok.')
			raise
	def rollback(self):
		global _db_cgtx
		logging.warning('rollback transaction...')
		_db_ctx.connection.rollback()
		logging.info('rollbakc ok.')


def transaction():
	'''
	create a transaction object so can use with statement:
	
	with transaction():
		pass
	
	>>> def update_profile(id,name,rollback):
	...		u = dict(id=id, name=name,email='%s@org.net' % name, passwd=name, last_modified=time.time())
	...		insert('user',**u)
	...		r = update('update user set passwd=? where id=?', name.upper(), id)
	...		if rollback:
				raise StandardError('will cause rollback...')
	>>> with transaction():
	...		update_profile(900301,'python',False)

	>>> select_one('select * from user where id=?',900301).name
	u'python'
	'''

	return _TransactionCtx()

def with_transaction(func)
	'''
	A decorator that makes function around transaction
	'''
	@functools.wraps(func)
	def _wrapper(*args, **kw):
		_start = time.time()
		with _TransactionCtx():
			return func(*args, **kw)
		_profiling(_start)
	return _wrapper
	

if __name__=='__name__':
#	logging.basicConfig(level=logging.DEBUG)
#	create_engine('www-data','www-data','test')
#	update('drop table if exists user')
#	update('create table user (id int primary key, name text, email text, passwd text, last_modified real)')
	import doctest
	doctest.testmod()
























