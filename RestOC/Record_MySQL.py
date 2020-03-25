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
import json
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
			print('Unknown exception in Record_MySQL.Commands.__clear')
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
			print('Unknown Error in Record_MySQL.Commands.escape')
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
				print('Unknown Error in Record_MySQL.Commands.execute')
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
				print('Unknown Error in Record_MySQL.Commands.insert')
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
				print('Unknown Error in Record_MySQL.Commands.select')
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

	def create(self, conflict='error', changes=None):
		"""Create

		Adds the record to the DB and returns the primary key

		Arguments:
			conflict {str} -- Must be one of 'error', 'ignore', 'replace'
			changes {dict} -- Data needed to store a change record, is
				dependant on the 'changes' config value

		Returns:
			mixed|None
		"""

		# Make sure conflict arg is valid
		if conflict not in ('error', 'ignore', 'replace'):
			raise ValueError('conflict', conflict)

		# If the record requires revisions, make the first one
		if self._dStruct['revisions']:
			self._revision(True)

		# Create the string of all fields and values but the primary if it's
		#	auto incremented
		lTemp = [[], []]
		for f in self._dStruct['tree'].keys():
			if (f != self._dStruct['primary'] or not self._dStruct['auto_primary']) and f in self._dRecord:
				lTemp[0].append('`%s`' % f)
				if self._dRecord[f] != None:
					lTemp[1].append(self.processValue(self._dStruct, f, self._dRecord[f]))
				else:
					lTemp[1].append('NULL')

		# If we have replace for conflicts
		if conflict == 'replace':
			sUpdate = 'ON DUPLICATE KEY UPDATE %s' % ''.join([
				"%s = VALUES(%s)\n" % (lTemp[0][i], lTemp[0][i])
				for i in range(len(lTemp[0]))
			])

		# Else, no update
		else:
			sUpdate = ''

		# Join the fields and values
		sFields	= ','.join(lTemp[0])
		sValues	= ','.join(lTemp[1])

		# Cleanup
		del lTemp

		# Generate the INSERT statement
		sSQL = 'INSERT %s INTO `%s`.`%s` (%s)\n' \
				' VALUES (%s)\n' \
				'%s' % (
					(conflict == 'ignore' and 'IGNORE' or ''),
					self._dInfo['db'],
					self._dInfo['tree']._name,
					sFields,
					sValues,
					sUpdate
				)

		# If the primary key does not auto increment don't worry about storing
		#	the new ID
		if self._dStruct['auto_primary']:
			self._dRecord[self._dStruct['primary']] = Commands.insert(self._dStruct['host'], sSQL)
			mRet = self._dRecord[self._dStruct['primary']]
		else:
			if not Commands.execute(self._dStruct['host'], sSQL):
				mRet = None
			else:
				mRet = True

		# Clear changed fields
		self._dChanged = {}

		# If changes are required
		if self._dStruct['changes']:

			# Create the changes record
			dChanges = {
				"old": None,
				"new": "inserted"
			}

			# If Changes requires fields
			if isinstance(self._dStruct['changes'], list):

				# If they weren't passed
				if not isinstance(changes, dict):
					raise ValueError('changes')

				# Else, add the extra fields
				for k in self._dStruct['changes']:
					dChanges[k] = changes[k]

			# Generate the INSERT statement
			sSQL = 'INSERT `%s`, `created`, `items` INTO `%s`.`%s_changes`' \
					'VALUES(%s, CURRENT_TIMESTAMP, \'%s\')' % (
						self._dStruct['primary'],
						self._dStruct['db'],
						self._dStruct['table'],
						self.processValue(self._dStruct, self._dStruct['primary'], self._dRecord[self._dStruct['primary']]),
						json.dumps(dChanges)
					)

			# Create the changes record
			Commands.execute(self._dStruct['host'], sSQL)

		# Return
		return mRet

	def delete(self, changes=None):
		"""Delete

		Deletes the record represented by the instance

		Arguments:
			changes {dict} -- Data needed to store a change record, is
				dependant on the 'changes' config value

		Returns:
			bool
		"""

		# If the record lacks a primary key (never been created/inserted)
		if self._dStruct['primary'] not in self._dRecord:
			raise KeyError(self._dStruct['primary'])

		# Generate the DELETE statement
		sSQL = 'DELETE FROM `%s`.`%s` WHERE `%s` = %s' % (
			self._dStruct['db'],
			self._dStruct['table'],
			self._dStruct['primary'],
			self.processValue(
				self._dStruct,
				self._dStruct['primary'],
				self._dRecord[self._dStruct['primary']]
			)
		)

		# Delete the record
		iRet = Commands.execute(self._dStruct['host'], sSQL)

		# If no record was deleted
		if iRet != 1:
			return False

		# If changes are required
		if self._dStruct['changes']:

			# Create the changes record
			dChanges = {
				"old": self._dRecord,
				"new": None
			}

			# If Changes requires fields
			if isinstance(self._dStruct['changes'], list):

				# If they weren't passed
				if not isinstance(changes, dict):
					raise ValueError('changes')

				# Else, add the extra fields
				for k in self._dStruct['changes']:
					dChanges[k] = changes[k]

			# Generate the INSERT statement
			sSQL = 'INSERT `%s`, `created`, `items` INTO `%s`.`%s_changes`' \
					'VALUES(%s, CURRENT_TIMESTAMP, \'%s\')' % (
						self._dStruct['primary'],
						self._dStruct['db'],
						self._dStruct['table'],
						self.processValue(self._dStruct, self._dStruct['primary'], self._dRecord[self._dStruct['primary']]),
						json.dumps(dChanges)
					)

			# Insert the changes
			Commands.execute(self._dStruct['host'], sSQL)

		# Remove the primary key value so we can't delete again or save
		del self._dRecord[self._dStruct['primary']]

		# Return OK
		return True

	@classmethod
	def deleteGet(cls, _id=None, index=None, custom={}):
		"""Delete Get

		Deletes one or many records by primary key or index and returns how many
		were found/deleted

		Arguments:
			_id {mixed|mixed[]} -- The primary key(s) to delete or None for all
				records
			index {str} -- Used as the index instead of the primary key
			custom {dict} -- Custom Host and DB info
				'host' the name of the host to get/set data on
				'append' optional postfix for dynamic DBs

		Return:
			int
		"""

		# Fetch the record structure
		dStruct = cls.struct(custom)

		# If changes are required
		if dStruct['changes']:
			raise RuntimeError('Tables with \'changes\' flag can\'t be deleted using deleteGet')

		# If there's no index and at least one ID passed
		if not index and _id:
			if not dStruct['primary']:
				raise DocumentException('Can not delete by primary key if none exists')
			index = dStruct['primary']

		# Build the statement
		sSQL = 'DELETE FROM `%s`.`%s`' % (dStruct['db'], dStruct['table'])

		# If we have IDs
		if _id is not None:

			# If there's only one
			if not isinstance(_id, (tuple,list)):
				sSQL += ' WHERE `%s` = %s' % (index, cls.processValue(dStruct, index, id_))

			else:
				sSQL += ' WHERE `%s` IN (%s)' % (index, ','.join([
					cls.processValue(dStruct, index, s)
					for s in _id
				]))

		# Delete the records
		return Commands.execute(dStruct['host'], sSQL)

	# escape method
	@classmethod
	def escape(cls, host, type_, value):
		"""Escape

		Takes a value and turns it into an acceptable string for SQL

		Args:
			host {str} -- The name of the host if we need to call the server
			type_ {str} -- The type of data to escape
			value {mixed} -- The value to escape

		Returns:
			str
		"""

		# If we're escaping a bool
		if type_ == 'bool':

			# If it's already a bool or a valid int representation
			if isinstance(value, bool) or (isinstance(value, (int,long)) and value in [0,1,1L]):
				return (value and '1' or '0')

			# Else if it's a string
			elif isinstance(value, basestring):

				# If it's t, T, 1, f, F, or 0
				return (value in ('true', 'True', 'TRUE', 't', 'T', '1') and '1' or '0')

		# Else if it's a date, md5, or UUID, return as is
		elif type_ in ('base64', 'date', 'datetime', 'md5', 'time', 'uuid'):
			return "'%s'" % value

		# Else if the value is a decimal value
		elif type_ in ('decimal', 'float', 'price'):
			return str(float(value))

		# Else if the value is an integer value
		elif type_ in ('int', 'uint'):
			return str(int(value))

		# Else if it's a timestamp
		elif type_ == 'timestamp' and (isinstance(value, int) or re.match('^\d+$', value)):
			return 'FROM_UNIXTIME(%s)' % str(value)

		# Else it's a standard escape
		else:
			return "'%s'" % Commands.escape(host, value)

	@classmethod
	def exists(cls, _id, index=None, custom={}):
		"""Exists

		Returns true if the specified primary key or unique index value exists

		Arguments:
			_id {mixed} -- The primary key to check
			index {str} -- Used as the index instead of the primary key
			custom {dict} -- Custom Host and DB info
				'host' the name of the host to get/set data on
				'append' optional postfix for dynamic DBs

		Returns:
			bool
		"""

		# Fetch the record structure
		dStruct = cls.struct(custom)

		# Use the get method to avoid duplicate code and check if anything was
		#	returned
		if not cls.get(_id, raw=[dStruct['primary']], custom=custom):
			return False

		# If anything was returned, the key exists
		return True

	# filter static method
	@classmethod
	def filter(cls, fields, raw=None, orderby=None, custom={}):
		"""Filter

		Finds records based on the specific fields and values passed

		Arguments:
			fields {dict} -- A dictionary of field names to the values they
				should match
			raw (bool|list} -- Return raw data (dict) for all or a set list of
				fields
			orderby {str|str[]} -- A field or fields to order the results by
			custom {dict} -- Custom Host and DB info
				'host' the name of the host to get/set data on
				'append' optional postfix for dynamic DBs

		Returns:
			Record[]|dict[]
		"""

		# Fetch the record structure
		dStruct = cls.struct(custom)

		# Generate the SELECT fields
		if not isinstance(raw, bool):
			sFields = '`' + '`,`'.join(raw) + '`'
		else:
			sFields = '`' + '`,`'.join(dStruct['tree'].keys()) + '`'

		# Go through each value
		lWhere = [];
		for n,v in obj.items():

			# Generate theSQL and append it to the list
			lWhere.append(
				'`%s` %s' % (n, cls.processValue(dStruct, n, v))
			)

		# If the order isn't set
		if orderby == None:
			sOrderBy = ''

		# Else, generate it
		else:

			# If the field is a list of fields
			if isinstance(orderby, (list, tuple)):

				# Go through each field
				lOrderBy = []
				for i in orderby:
					if instanceof(i, (list,tuple)):
						lOrderBy.append('`%s` %s' % (i[0], i[1]))
					else:
						lOrderBy.append('`%s`' % i)
				sOrderBy = 'ORDER BY %s' % ','.join(lOrderBy)

			# Else there's only one field
			else:
				sOrderBy = 'ORDER BY `%s`' % orderby

		# Build the statement
		sSQL = 'SELECT %s FROM `%s`.`%s` ' \
				'WHERE %s ' \
				'%s' % (
					sFields,
					dStruct['db'],
					dStruct['table'],
					' AND '.join(lWhere),
					sOrderBy
				)

		# Get all the records
		lRecords = Commands.select(dStruct['host'], sSQL, ESelect.ALL)

		# If Raw requested, return as is
		if raw:
			return lRecords

		# Else create instances for each
		else:
			return [cls(d, custom) for d in lRecords]











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

		# Get the field type
		sType = struct['tree'][field].type()

		# If the value is a list
		if isinstance(value, (list,tuple)):

			# Build the list of values
			lValues = []
			for i in value:
				# If it's None
				if i is None: lValues.append('NULL')
				else: lValues.append(cls.escape(struct['host'], sType, i))
			sRet = 'IN (%s)' % ','.join(lValues)

		# Else if the value is a dictionary
		elif isinstance(value, dict):

			# If it has a start and end
			if 'between' in value:
				sRet = 'BETWEEN %s AND %s' % (
							cls.escape(struct['host'], sType, value['between'][0]),
							cls.escape(struct['host'], sType, value['between'][1])
						)

			# Else if we have a less than
			elif 'lt' in value:
				sRet = '< ' + cls.escape(struct['host'], sType, value['lt'])

			# Else if we have a greater than
			elif 'gt' in value:
				sRet = '> ' + cls.escape(struct['host'], sType, value['gt'])

			# Else if we have a less than equal
			elif 'lte' in value:
				sRet = '<= ' + cls.escape(struct['host'], sType, value['lteq'])

			# Else if we have a greater than equal
			elif 'gte' in value:
				sRet = '>= ' + cls.escape(struct['host'], sType, value['gteq'])

			# Else if we have a not equal
			elif 'neq' in value:

				# If the value is a list
				if isinstance(value['neq'], (list,tuple)):

					# Build the list of values
					lValues = []
					for i in value['neq']:
						# If it's None
						if i is None: lValues.append('NULL')
						else: lValues.append(cls.escape(struct['host'], sType, i))
					sRet = 'NOT IN (%s)' % ','.join(lValues)

				# Else, it must be a single value
				else:
					sRet = '!= ' + cls.escape(struct['host'], sType, value['neq'])

		# Else, it must be a single value
		else:

			# If it's None
			if value is None: sRet = '= NULL'
			else: sRet = '= ' + cls.escape(struct['host'], sType, value)

		# Return the processed value
		return sRet
