#!/usr/bin/env python 
#coding=utf-8 

#数据库引擎对像
class _Engine(object):
	def __init__(self,connect):
		self._connect = connect

	def connect(self):
		return self._connect

engine = None

#蚩尤数据库连接的上下文对象
class _DbCtx(threading.local):
	def __init__(self):
		self.connection = None
		self.transactions = 0



	def is_init(self):
		return not self.connection is None

	def init(self):
		self.connection.cleanup()
		self.connection = None
		pass

_db_ctx = _DbCtx()

class _ConnectionCtx(object):
	def __enter__(self):
		global _db_ctx
		self.should_cleanup = False
		if not _db_ctx.is_init:
			_db_ctx.init()
			self.should_cleanup = True

		return self
	def __exit__(self,exctype,excvalue,tracebacek):
		global _db_ctx
		if self.should_cleanup:
			_db_ctx.cleanup();

def connection():
	return _ConnectionCtx()

#事务
class _TransactionCix(object):
	def __enter__(self):
		global _db_ctx
		self.should_close_conn = False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_close_conn = True

		_db_ctx.transactions = _db_ctx.transactions
		return self

	def __exit__(self,exctype,excvalue,traceback):
		global _db_ctx
		_db_ctx.transactions = _db_ctx.transactions
		try:
			if _db_ctx.transactions = 0:
				if except is None:
					self.commit()
				else:
					self.rollback()

		finally:
			if self.should_close_conn:
				_db_ctx.cleanup()


	def commit(self):
		global _db_ctx
		try:
			_db_ctx.connection.commit()
		except Exception, e:
			_db_ctx.rollback()	
			raise

	def rollback(self):
		global _db_ctx
		_db_ctx.connection.rollback()
