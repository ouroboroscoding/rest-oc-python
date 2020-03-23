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
			if k in [7]: conv[k] = __converterTimestamp
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
	tDT = datetime.datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')

	# Convert it to a timestamp and return it
	return int(tDT.strftime('%s'))

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

		# Create the DB
		Commands.execute(host, 'CREATE DATABASE `%s%s`' % (Record_Base.dbPrepend(), name))
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

		# Delete the DB
		Commands.execute(host, "DROP DATABASE `%s%s`" % (Record_Base.dbPrepend(), name))

	# If the DB doesn't exist
	except pymysql.err.InternalError:
		return False

	# Return OK
	return True

# Commands class
class Commands(object):
	"""Commands class

	Used to directly interface with MySQL

	Extends:
		object
	"""

	@classmethod
	def escape(cls, host, value):
		"""Escape

		Used to escape string values for the DB

		Args:
			host (str): The name of the connection to escape for
			value (str): The value to escape
			rel (str): The relationship of the server, master or slave

		Returns:
			str
		"""

		# Get a connection to the host
		oCon = __connection(host)

		# Get the value
		try:
			sRet = oCon.escape_string(value)

		# Else there's an operational problem so close the connection and
		#	restart
		except pymysql.err.OperationalError as e:

			# Clear the connection and try again
			__clearConnection(host)
			return cls.escape(host, value)

		except Exception as e:
			print('\n------------------------------------------------------------')
			print('Unknown Error in Record_MySQL.Raw.escape')
			print('host = ' + host)
			print('value = ' + str(value))
			print('exception = ' + str(e.__class__.__name__))
			print('args = ' + ', '.join([str(s) for s in e.args]))

			# Rethrow
			raise e

		# Return the escaped string
		return sRet

	@classmethod
	def execute(cls, host, sql):
		"""Execute

		Used to run SQL that doesn't return any rows

		Args:
			host (str): The name of the connection to execute on
			sql (str|tuple): The SQL (or SQL plus a list) statement to run

		Returns:
			uint
		"""

		# Fetch a cursor
		with __wcursor(host) as oCursor:

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
				__clearConnection(host)
				return cls.execute(host, sql)

			# Else, catch any Exception
			except Exception as e:
				print('\n------------------------------------------------------------')
				print('Unknown Error in Record_MySQL.Raw.execute')
				print('host = ' + host)
				print('sql = ' + str(sql))
				print('exception = ' + str(e.__class__.__name__))
				print('args = ' + ', '.join([str(s) for s in e.args]))

				# Rethrow
				raise e

	@classmethod
	def insert(cls, host, sql):
		"""Insert

		Handles INSERT statements and returns the new ID. To insert records
		without auto_increment it's best to just stick to CSQL.execute()

		Args:
			host (str): The name of the connection to into on
			sql (str): The SQL statement to run

		Returns:
			mixed
		"""

		# Fetch a cursor
		with __wcursor(host) as oCursor:

			try:

				# If the sql arg is a tuple we've been passed a string with a list for the purposes
				#	of replacing parameters
				if isinstance(sql, tuple):
					oCursor.execute(sql[0], sql[1])
				else:
					oCursor.execute(sql)

				# Get the ID
				mInsertID = oCursor.lastrowid

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
					raise ValueError(e.args[0], 'SQL error (' + str(e.args[0]) + '): ' + str(e.args[1]) + '\n' + str(sql))

				# Clear the connection and try again
				__clearConnection(host)
				return cls.insert(host, sql)

			# Else, catch any Exception
			except Exception as e:
				print('\n------------------------------------------------------------')
				print('Unknown Error in Record_MySQL.Raw.insert')
				print('host = ' + host)
				print('sql = ' + str(sql))
				print('exception = ' + str(e.__class__.__name__))
				print('args = ' + ', '.join([str(s) for s in e.args]))

				# Rethrow
				raise e

	@classmethod
	def select(cls, host, sql, seltype=ESelect.ALL, field=None):
		"""Select

		Handles SELECT queries and returns the data

		Args:
			host (str): The name of the host to select from
			sql (str): The SQL statement to run
			seltype (ESelect): The format to return the data in
			field (str): Only used by HASH_ROWS since MySQLdb has no ordereddict
				for associative rows

		Returns:
			mixed
		"""

		# Get a cursor
		bDictCursor = seltype in (ESelect.ALL, ESelect.HASH_ROWS, ESelect.ROW)

		# Fetch a cursor
		with __wcursor(host) as oCursor:

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
						mData[n] = v

				# If we want a hash of the first field and the entire row
				elif seltype == ESelect.HASH_ROWS:
					# If the field arg wasn't set
					if field == None:
						raise ValueError('Must specificy a field for the dictionary key when using HASH_ROWS')

					mData = {}
					mTemp = oCursor.fetchall()

					for o in mTemp:
						# Store the entire row under the key
						mData[o[field]] = o

				# If we want just the first row
				elif seltype == ESelect.ROW:
					mData = oCursor.fetchone()

				# Return the results
				return mData

			# If the SQL is bad
			except pymysql.err.ProgrammingError as e:

				# Raise an SQL Exception
				raise ValueError(e.args[0], 'SQL error (' + str(e.args[0]) + '): ' + str(e.args[1]) + '\n' + str(sql))

			# Else, a duplicate key error
			except pymysql.err.IntegrityError as e:

				# Raise an SQL Duplicate Exception
				raise SqlDuplicateException(e.args[0], e.args[1])

			# Else there's an operational problem so close the connection and
			#	restart
			except pymysql.err.OperationalError as e:

				# If the error code is one that won't change
				if e.args[0] in [1054]:
					raise ValueError(e.args[0], 'SQL error (' + str(e.args[0]) + '): ' + str(e.args[1]) + '\n' + str(sql))

				# Clear the connection and try again
				__clearConnection(host)
				return cls.select(host, sql, seltype)

			# Else, catch any Exception
			except Exception as e:
				print('\n------------------------------------------------------------')
				print('Unknown Error in Record_MySQL.Raw.select')
				print('host = ' + host)
				print('sql = ' + str(sql))
				print('exception = ' + str(e.__class__.__name__))
				print('errcnt = ' + str(errcnt))
				print('args = ' + ', '.join([str(s) for s in e.args]))

				# Rethrow
				raise e

class Record(Record_Base.Record):
	"""Record

	Extends the base Record class

	Extends:
		Record_Base.Record
	"""

	@classmethod
	def config(cls):
		"""Config

		Returns the configuration data associated with the record type

		Returns:
			dict
		"""
		raise NotImplementedError('Must implement the "config" method')

	@classmethod
	def count(cls, _id=None, filter=None, custom={}):
		"""Count

		Returns the number of records associated with index or filter

		Arguments:
			_id {mixed} -- The ID(s) to check
			filter {dict} -- Additional filter
			custom {dict} -- Custom Host and DB info
				'host' the name of the host to get/set data on
				'append' optional postfix for dynamic DBs

		Returns:
			bool
		"""

		# Fetch the record structure
		dStruct = cls.struct(custom)

		# Init possible WHERE values
		lWhere = []

		# If there's no primary key, we want all records
		if _id == None:
			pass

		# If we are using the primary key
		else:

			# Append the ID check
			lWhere.append('`%s` %s' % (
				dStruct['primary'],
				self.processValue(dStruct, dStruct['primary'], _id)
			))

		# If we want to filter the data further
		if filter:

			# Go through each value
			for n,v in filter.items():

				# Generate theSQL and append it to the list
				lWhere.append(
					'`%s` %s' % (
						n,
						cls.processValue(dStruct, n, v)
				))

		# Build the statement
		sSQL = 'SELECT COUNT(*) FROM `%s`.`%s` ' \
				'WHERE %s ' % (
					dStruct['db'],
					dStruct['tree']._name,
					' AND '.join(lWhere)
				)

		# Run the request and return the count
		return Commands.select(dStruct['host'], sSQL, ESelect.CELL)













	# escape method
	@classmethod
	def escape(cls, struct, field, value):
		"""Escape

		Takes a value and turns it into an acceptable string for SQL

		Args:
			struct {dict} -- The structure associated with the record
			field {str} -- The name of the field we are escaping
			value {mixed} -- The value to escape

		Returns:
			str
		"""

		# Get the type of field
		sType = struct['tree'].get(field).type();

		# If we're escaping a bool
		if sType == 'bool':

			# If it's already a bool or a valid int representation
			if isinstance(value, bool) or (isinstance(value, (int,long)) and value in [0,1,1L]):
				return (value and '1' or '0')

			# Else if it's a string
			elif isinstance(value, basestring):

				# If it's t, T, 1, f, F, or 0
				return (value in ('true', 'True', 'TRUE', 't', 'T', '1') and '1' or '0')

		# Else if it's a date, md5, or UUID, return as is
		elif sType in ('base64', 'date', 'datetime', 'md5', 'time', 'uuid'):
			return "'%s'" % value

		# Else if the value is a decimal value
		elif sType in ('decimal', 'float', 'price'):
			return str(float(value))

		# Else if the value is an integer value
		elif sType in ('int', 'uint'):
			return str(int(value))

		# Else if it's a timestamp
		elif sType == 'timestamp' and (isinstance(value, int) or re.match('^\d+$', value)):
			return 'FROM_UNIXTIME(%s)' % str(value)

		# Else it's a standard escape
		else:
			return "'%s'" % SQL.escape(struct['host'], value)

	# processValue static method
	@classmethod
	def processValue(cls, struct, field, value):
		"""Process Value

		Takes a field and a value or values and returns the proper SQL
		to look up the values for the field

		Args:
			struct {dict} -- The structure associated with the record
			field {str} -- The name of the field
			value {mixed} -- The value as a single item, list, or dictionary

		Returns:
			str
		"""

		# If the value is a list
		if isinstance(value, (list,tuple)):

			# Build the list of values
			lValues = []
			for i in value:
				# If it's None
				if i is None: lValues.append('NULL')
				else: lValues.append(cls.escape(struct, field, i))
			sRet = 'IN (%s)' % ','.join(lValues)

		# Else if the value is a dictionary
		elif isinstance(value, dict):

			# If it has a start and end
			if 'between' in value:
				sRet = 'BETWEEN %s AND %s' % (
							cls.escape(struct, field, value['between'][0]),
							cls.escape(struct, field, value['between'][1])
						)

			# Else if we have a less than
			elif 'lt' in value:
				sRet = '< ' + cls.escape(struct, field, value['lt'])

			# Else if we have a greater than
			elif 'gt' in value:
				sRet = '> ' + cls.escape(struct, field, value['gt'])

			# Else if we have a less than equal
			elif 'lte' in value:
				sRet = '<= ' + cls.escape(struct, field, value['lteq'])

			# Else if we have a greater than equal
			elif 'gte' in value:
				sRet = '>= ' + cls.escape(struct, field, value['gteq'])

			# Else if we have a not equal
			elif 'neq' in value:

				# If the value is a list
				if isinstance(value['neq'], (list,tuple)):

					# Build the list of values
					lValues = []
					for i in value['neq']:
						# If it's None
						if i is None: lValues.append('NULL')
						else: lValues.append(cls.escape(struct, field, i))
					sRet = 'NOT IN (%s)' % ','.join(lValues)

				# Else, it must be a single value
				else:
					sRet = '!= ' + cls.escape(struct, field, value['neq'])

		# Else, it must be a single value
		else:

			# If it's None
			if value is None: sRet = '= NULL'
			else: sRet = '= ' + cls.escape(struct, field, value)

		# Return the processed value
		return sRet
