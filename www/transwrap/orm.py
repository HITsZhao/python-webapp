#!/usr/bin/env python
#-*- coding: utf-8 -*-

__author__ = 'Zhao'

'''
Database operation module. This module is independent with web module
'''

import time, logging
import db

class Field(object):
	_count = 0
	
	def __init__(self, **kw):
		self.name		= kw.get('name', None)
		self._default	= kw.get('default', None)
		self.primary_key= kw.get('primary_key', False)
		self.nullable	= kw.get('nullable', False)
		self.updatable	= kw.get('updatable', True)
		self.insertable	= kw.get('insertable', True)
		self.ddl		= kw.get('ddl','')
		self._order		= Field._count
		Field._count	+= 1

	#定义只读属性default
	@property
	def default(self):
		d = self._default
		return d() if callable(d) else d

#定义Model元类。Model对应数据库表，所以ModelMetaclass对应数据库DDL
class ModelMetaclass(type):
	'''
	Metaclass for model object
	'''
	def __new__(cls,name,bases,attrs):
		# Skip base Model object
		if name == 'Model':
			return type.__new__(cls,name,bases,attrs)

		logging.info("Scan ORM mapping %s...", %name)
		mappings = dict()
		primary_key = None
		for k,v in attrs.iteritems():
			if isinstance(v,Field):
				if not v.name:
					v.name = k
				logging.info('Found mapping %s => %s',% (k, v))
				#如果定义字段v是主键
				if v.primary_key:
					if primary_key:
						raise TypeError("Cann't define one more primary key in class %s", % name)
					if v.updatable:
						logging.info("NOTE: change primary key to non-updatable")
						v.updatable = False
					if v.nullable:
						logging.warning("NOTE: change primary key to non-nullable")
						v.nullable = False
					primary_key = v
					mappings[k] = v
		if not primary_key:
			raise TypeError("Primary key not defined in class: %s" % name)
		for k in mappings.iterkeys():
			attrs.pop(k)
		if not '__table__' in attrs:
			attrs['__table__'] = name.lower()
		attrs['__mappings__'] = mappings
		attrs['__primary_key__'] = primary_key

		return type.__new__(cls,name,bases,attrs)
