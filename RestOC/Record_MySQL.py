# coding=utf8
"""Record SQL Module

Extends Record module to add support for SQL tables
"""

__author__ = "Chris Nasr"
__copyright__ = "FUEL for the FIRE"
__version__ = "1.0.0"
__created__ = "2020-02-12"

# Python imports
from enum import IntEnum
from hashlib import md5
import sys
from time import sleep, time

# Pip imports
import pymysql

# Framework imports
from . import DictHelper, Record_Base

# List of available hosts
__mdHosts = {}

# List of available connection
__mdConnections = {}

# defines
MAX_RETRIES = 3

## ESelect
class ESelect(IntEnum):
	ALL			= 1
	CELL		= 2
	COLUMN		= 3
	HASH		= 4
	HASH_ROWS	= 5
	ROW			= 6

# Duplicate key exception
class DuplicateException(Exception):
	"""DuplicateException class

	Used for raising issues with duplicate records

	Extends:
		Exception
	"""
	pass

def __clearConnection(cls, host):
	"""Clear Connection

	Handles removing a connection from the module list

	Args:
		host {str} -- The host to clear

	Returns:
		None
	"""

	# If we have the connection
	if host in cls.__mdConnections:

		# Try to close the connection
		try:
			cls.__mdConnections[host].close()

			# Sleep for a second
			sleep(1)

		# Catch any exception
		except Exception as e:
			print('\n------------------------------------------------------------')
			print('Unknown exception in Record_MySQL.Raw.__clear')
			print('host = ' + str(host))
			print('exception = ' + str(e.__class__.__name__))
			print('args = ' + ', '.join([str(s) for s in e.args]))

		# Delete the connection
		del cls.__mdConnections[host]

def __connection(host, errcnt = 0):
	"""Connection

	Returns a connection to the given host

	Args:
		host {str} -- The name of the host to connect to
		errcnt {uint} -- The current error count

	Returns:
		Connection
	"""

	# If we already have the connection, return it
	if host in __mdConnections:
		return __mdConnections[host]

	# If no such host has been added
	if host not in __mdHosts:
		raise ValueError('no such host "%s"' % str(host))

	# Get the config
	dConf = __mdHosts[host]

	# Create a new connection
	try:
		oCon = pymysql.connect(**__mdHost[host])

		# Turn autocommit on
		oCon.autocommit(True)

		# Change conversions
		conv = oCon.converter.copy()
		for k in conv:
			if k in [7]: conv[k] = cls.converterTimestamp
			elif k in [10,11,12]: conv[k] = str
		oCon.converter = conv

	# Check for errors
	except pymysql.err.OperationalError as e:

		# Increment the error count
		errcnt += 1

		# If we've hit our max errors, raise an exception
		if errcnt == MAX_RETRIES:
			raise ConnectionError(*e.args)

		# Else just sleep for a second and try again
		else:
			sleep(1)
			return cls.__connection(host, errcnt)

	# Store the connection and return it
	__mdConnections[host] = oCon
	return oCon

def __cursor(host, dictCur = False):
	"""Cursor

	Returns a cursor for the given host

	Args:
		host {str} -- The name of the host
		dictCur {bool} -- If true, cursor will use dicts

	Return:
		Cursor
	"""

	# Get a connection to the host
	oCon = __connection(host)

	# Try to get a cursor on the connection
	try:
		if dictCursor:
			oCursor = oCon.cursor(pymysql.cursors.DictCursor)
		else:
			oCursor = oCon.cursor()

		# Make sure we're on UTF8
		oCursor.execute('SET NAMES utf8')

	except:
		# Clear the connection and try again
		__clearConnection(host)
		return __cursor(host, dictCur)

	# Return the cursor
	return oCursor

class __wcursor(object):
	"""_with

	Used with the special Python with method to create a connection that will
	always be closed regardless of exceptions

	Extends:
		object
	"""

	def __init__(self, host, dictCur = false):
		self.cursor = __cursor(host, dictCur);

	def __enter__(self):
		return self.cursor

	def __exit__(self, exc_type, exc_value, traceback):
		self.cursor.close()
		if exc_type is not None:
			return False

class connect(object):
	"""Connect

	Used with the special Python with method to create a connection that will
	always be closed regardless of exceptions

	Extends:
		object
	"""

	def __init__(self, host):
		self.raw = _Raw(host);

	def __enter__(self):
		return self.raw

	def __exit__(self, exc_type, exc_value, traceback):
		del self.raw
		if exc_type is not None:
			return False

def addHost(name, info, update=False):
	"""Add Host

	Add a host that can be used by Records

	Arguments:
		name {str} -- The name that will be used to fetch the host credentials
		info {dict} -- The necessary credentials to connect to the host

	Returns:
		bool
	"""

	# If the info isn't already stored, or we want to overwrite it, store it
	if name not in __mdHosts or update:
		__mdHosts[name] = info
		return True

	# Nothing to do, not OK
	return False

def dbCreate(name, host = 'primary'):
	"""DB Create

	Creates a DB on the given host

	Arguments:
		name {str} -- The name of the DB to create
		host {str} -- The name of the host the DB will be on

	Returns:
		bool
	"""

	try:

		# Fetch the connection
		with connect(host) as oRaw:

			# Create the DB
			oRaw.execute('CREATE DATABASE `%s%s`' % (Record_Base.dbPrepend(), name))
			return True

	# If the DB already exists
	except pymysql.err.ProgrammingError:
		return True

	# Unknown runtime error
	except rerrors.RqlRuntimeError:
		return False

	# Return OK
	return True

def dbDrop(name, host = 'primary'):
	"""DB Drop

	Drops a DB on the given host

	Arguments:
		name {str} -- The name of the DB to delete
		host {str} -- The name of the host the DB is on

	Returns:
		bool
	"""

	try:

		# Fetch the connection
		with connect(host) as oRaw:

			# Delete the DB
			oRaw.execute("DROP DATABASE `%s%s`" % (Record_Base.dbPrepend(), name))

	# If the DB doesn't exist
	except pymysql.err.InternalError:
		return False

	# Return OK
	return True

# Raw class
class _Raw(object):
	"""Raw class

	Used to directly interface with MySQL

	Extends:
		object
	"""

	def __init__(self, host):
		"""Constructor

		Initialises the instance and returns it

		Arguments:
			host {str} -- The name of the host to use with this instance

		Returns:
			Raw
		"""

		# Store the host
		self.__sHost = host

	@staticmethod
	def __converterTimestamp(ts):
		"""Converter Timestamp

		Converts timestamps received from MySQL into proper integers

		Args:
			ts (str): The timestamp to convert

		Returns:
			uint
		"""

		# If there is no time
		if ts == '0000-00-00 00:00:00':
			return 0

		# Get a datetime tuple
		tDT	= datetime.datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')

		# Convert it to a timestamp and return it
		return int(tDT.strftime('%s'))

	def escape(self, value):
		"""Escape

		Used to escape string values for the DB

		Args:
			host (str): The name of the instance to escape for
			value (str): The value to escape
			rel (str): The relationship of the server, master or slave

		Returns:
			str
		"""

		# Get a connection to the host
		oCon = __connection(self.__sHost)

		# Get the value
		try:
			sRet = oCon.escape_string(value)

		# Else there's an operational problem so close the connection and
		#	restart
		except pymysql.err.OperationalError as e:

			# Clear the connection and try again
			__clearConnection(self.__sHost)
			return self.escape(value)

		except Exception as e:
			print('\n------------------------------------------------------------')
			print('Unknown Error in Record_MySQL.Raw.escape')
			print('host = ' + self.__sHost)
			print('value = ' + str(value))
			print('exception = ' + str(e.__class__.__name__))
			print('args = ' + ', '.join([str(s) for s in e.args]))

			# Rethrow
			raise e

		# Return the escaped string
		return sRet

	def execute(self, sql):
		"""Execute

		Used to run SQL that doesn't return any rows

		Args:
			sql (str|tuple): The SQL (or SQL plus a list) statement to run

		Returns:
			uint
		"""

		# Fetch a cursor
		with __wcursor(self.__sHost) as oCursor:

			try:

				# If the sql arg is a tuple we've been passed a string with a list for the purposes
				#	of replacing parameters
				if isinstance(sql, tuple):
					iRet = oCursor.execute(sql[0], sql[1])
				else:
					iRet = oCursor.execute(sql)

				# Return the changed rows
				return iRet

			# If the SQL is bad
			except pymysql.err.ProgrammingError as e:

				# Raise an SQL Exception
				raise ValueError(e.args[0], 'SQL error (' + str(e.args[0]) + '): ' + str(e.args[1]) + '\n' + str(sql))

			# Else, a duplicate key error
			except pymysql.err.IntegrityError as e:

				# Raise an SQL Duplicate Exception
				raise DuplicateException(e.args[0], e.args[1])

			# Else there's an operational problem so close the connection and
			#	restart
			except pymysql.err.OperationalError as e:

				# If the error code is one that won't change
				if e.args[0] in [1054]:
					raise ValueError(e.args[0], 'SQL error (' + str(e.args[0]) + '): ' + str(e.args[1]) + '\n' + str(sql))

				# Clear the connection and try again
				__clearConnection(self.__sHost)
				return self.execute(sql)

			# Else, catch any Exception
			except Exception as e:
				print('\n------------------------------------------------------------')
				print('Unknown Error in Record_MySQL.Raw.execute')
				print('host = ' + self.__sHost)
				print('sql = ' + str(sql))
				print('exception = ' + str(e.__class__.__name__))
				print('args = ' + ', '.join([str(s) for s in e.args]))

				# Rethrow
				raise e

	def insert(self, sql):
		"""Insert

		Handles INSERT statements and returns the new ID. To insert records
		without auto_increment it's best to just stick to CSQL.execute()

		Args:
			sql (str): The SQL statement to run

		Returns:
			mixed
		"""

		# Fetch a cursor
		with __wcursor(self.__sHost) as oCursor:

			try:

				# If the sql arg is a tuple we've been passed a string with a list for the purposes
				#	of replacing parameters
				if isinstance(sql, tuple):
					oCursor.execute(sql[0], sql[1])
				else:
					oCursor.execute(sql)

				# Get the ID
				mInsertID	= oCursor.lastrowid

				# Return the last inserted ID
				return mInsertID

			# If the SQL is bad
			except pymysql.err.ProgrammingError as e:

				# Raise an SQL Exception
				raise ValueError(e.args[0], 'SQL error (' + str(e.args[0]) + '): ' + str(e.args[1]) + '\n' + str(sql))

			# Else, a duplicate key error
			except pymysql.err.IntegrityError as e:

				# Raise an SQL Duplicate Exception
				raise DuplicateException(e.args[0], e.args[1])

			# Else there's an operational problem so close the connection and
			#	restart
			except pymysql.err.OperationalError as e:

				# If the error code is one that won't change
				if e.args[0] in [1054]:
					raise SqlException(e.args[0], 'SQL error (' + str(e.args[0]) + '): ' + str(e.args[1]) + '\n' + str(sql))

				# Clear the connection and try again
				__clearConnection(self.__sHost)
				return self.insert(sql)

			# Else, catch any Exception
			except Exception as e:
				print('\n------------------------------------------------------------')
				print('Unknown Error in Record_MySQL.Raw.insert')
				print('host = ' + self.__sHost)
				print('sql = ' + str(sql))
				print('exception = ' + str(e.__class__.__name__))
				print('args = ' + ', '.join([str(s) for s in e.args]))

				# Rethrow
				raise e

	def select(self, sql, seltype=ESelect.ALL, field=None):
		"""Select

		Handles SELECT queries and returns the data

		Args:
			host (str): The name of the host
			sql (str): The SQL statement to run
			seltype (ESelect): The format to return the data in
			field (str): Only used by HASH_ROWS since MySQLdb has no ordereddict
				for associative rows

		Returns:
			mixed
		"""

		# Get a cursor
		bDictCursor	= seltype in (ESelect.ALL, ESelect.HASH_ROWS, ESelect.ROW)

		# Fetch a cursor
		with __wcursor(self.__sHost) as oCursor:

			try:
				# If the sql arg is a tuple we've been passed a string with a list for the purposes
				#	of replacing parameters
				if isinstance(sql, tuple):
					oCursor.execute(sql[0], sql[1])
				else:
					oCursor.execute(sql)

				# If we want all rows
				if seltype == ESelect.ALL:
					mData = list(oCursor.fetchall())

				# If we want the first cell 0,0
				elif seltype == ESelect.CELL:
					mData = oCursor.fetchone()
					if mData != None:
						mData = mData[0]

				# If we want a list of one field
				elif seltype == ESelect.COLUMN:
					mData = []
					mTemp = oCursor.fetchall()
					for i in mTemp:
						mData.append(i[0])

				# If we want a hash of the first field and the second
				elif seltype == ESelect.HASH:
					mData = {}
					mTemp = oCursor.fetchall()
					for n,v in mTemp:
						mData[n]	= v

				# If we want a hash of the first field and the entire row
				elif seltype == ESelect.HASH_ROWS:
					# If the field arg wasn't set
					if field == None:
						raise SqlException('Must specificy a field for the dictionary key when using HASH_ROWS')

					mData = {}
					mTemp = oCursor.fetchall()

					for o in mTemp:
						# Store the entire row under the key
						mData[o[field]]	= o

				# If we want just the first row
				elif seltype == ESelect.ROW:
					mData = oCursor.fetchone()

				# Close the cursor
				oCursor.close()

				# Return the results
				return mData

			# If the SQL is bad
			except pymysql.err.ProgrammingError as e:

				# Raise an SQL Exception
				raise SqlException(e.args[0], 'SQL error (' + str(e.args[0]) + '): ' + str(e.args[1]) + '\n' + str(sql))

			# Else, a duplicate key error
			except pymysql.err.IntegrityError as e:

				# Raise an SQL Duplicate Exception
				raise SqlDuplicateException(e.args[0], e.args[1])

			# Else there's an operational problem so close the connection and
			#	restart
			except pymysql.err.OperationalError as e:

				# If the error code is one that won't change
				if e.args[0] in [1054]:
					raise SqlException(e.args[0], 'SQL error (' + str(e.args[0]) + '): ' + str(e.args[1]) + '\n' + str(sql))

				# Clear the connection and try again
				__clearConnection(self.__sHost)
				return self.select(sql, seltype)

			# Else, catch any Exception
			except Exception as e:
				print('\n------------------------------------------------------------')
				print('Unknown Error in Record_MySQL.Raw.select')
				print('host = ' + self.__sHost)
				print('sql = ' + str(sql))
				print('exception = ' + str(e.__class__.__name__))
				print('errcnt = ' + str(errcnt))
				print('args = ' + ', '.join([str(s) for s in e.args]))

				# Rethrow
				raise e
