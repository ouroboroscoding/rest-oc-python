# coding=utf8
""" Services Module

Tools to create and communicate with Micro Services
"""

__author__ = "Chris Nasr"
__copyright__ = "Ouroboros Coding Inc."
__version__ = "1.0.0"
__email__ = "chris@ouroboroscoding.com"
__created__ = "2018-11-11"

# Ouroboros imports
import jsonb
import undefined

# Python imports
from datetime import datetime
from hashlib import sha1
import re
from time import sleep, time

# Pip imports
import requests

# Module imports
from . import Errors

__mbVerbose = False
"""Verbose Flag"""

__mdRegistered = {}
"""Registered Services"""

__miInternal = 0
"""Internal Key timeout in seconds"""

__msSalt = None
"""Internal Key Salt"""

__funcToRequest = {
	'create': [requests.post, 'POST'],
	'delete': [requests.delete, 'DELETE'],
	'read': [requests.get, 'GET'],
	'update': [requests.put, 'PUT']
}
"""Map functions to REST types"""

def request(service, action, path, req = {}):
	"""Request

	Method to convert REST requests into HTTP requests

	Arguments:
		service (str): The service we are requesting data from
		action (str): The action to take on the service
		path (str): The path of the request
		req (dict): The request details: 'data', 'session', and 'enviroment'

	Raises:
		ServiceException

	Return:
		Response
	"""

	# If we have a registered service
	if service in __mdRegistered:

		# If the service is locally loaded
		if 'instance' in __mdRegistered[service]:

			# If verbose requested
			if __mbVerbose:
				print('%s: Calling %s.%s("%s", %s)' % (
					str(datetime.now()),
					service,
					action,
					path,
					'data' in req and jsonb.encode(req['data'], 2) or 'NONE')
				)

			# Directly call the action
			oResponse = getattr(__mdRegistered[service]['instance'], action)(
				path, req
			)

			# If verbose requested
			if __mbVerbose:
				print('%s: Returning %s\n' % (
					str(datetime.now()),
					jsonb.encode(oResponse.to_dict(), 2))
				)

		# Else if the service is running elsewhere
		else:

			try: __funcToRequest[action]
			except KeyError: Error(Errors.SERVICE_ACTION, action)

			# Generate the URL to reach the service
			sURL = __mdRegistered[service]['url'] + path

			# Init the data and headers
			sData = ''
			dHeaders = {
				'Content-Length': '0',
				'Content-Type': 'application/json; charset=utf-8'
			}

			# If the data was passed
			if 'data' in req and req['data']:

				# Convert the data to JSON and store the length
				sData = jsonb.encode(req['data'])
				dHeaders['Content-Length'] = str(len(sData))

			# If we have a session, add the ID to the headers
			if 'session' in req and req['session']:
				dHeaders['Authorization'] = req['session'].id()

			# Try to make the request and store the response
			iAttempts = 0
			while True:
				iAttempts += 1
				try:
					oRes = __funcToRequest[action][0](sURL, data=sData, headers=dHeaders)

					# If the request wasn't successful
					if oRes.status_code != 200:

						# If we got a 401
						if oRes.status_code == 401:
							return Response.from_json(oRes.content)
						else:
							return Error(Errors.SERVICE_STATUS, '%d: %s' % (oRes.status_code, oRes.content))

					# If we got the wrong content type
					if oRes.headers['Content-Type'].lower() != 'application/json; charset=utf-8':
						return Error(Errors.SERVICE_CONTENT_TYPE, '%s' % oRes.headers['content-type'])

					# Success, break out of the loop
					break

				# If we couldn't connect to the service
				except requests.ConnectionError as e:

					# If we haven't exhausted attempts
					if iAttempts < 3:

						# Wait for a second
						sleep(1)

						# Loop back around
						continue

					# We've tried enough, return an error
					return Error(Errors.SERVICE_UNREACHABLE, str(e))

			# Else turn the content into an Response and return it
			oResponse = Response.from_json(oRes.text)

		# Return the Response of the request
		return oResponse

	# Service not registered
	else:
		raise ResponseException(error=(Errors.SERVICE_NOT_REGISTERED, service))

def create(service, path, req = {}):
	"""Create

	Make a POST request

	Arguments:
		service (str): The service to call
		path (str): The path on the service
		req (dict): The request details, which can include 'data',
					'environment', and 'session'

	Returns:
		Response
	"""
	return request(service, 'create', path, req)

def delete(service, path, req = {}):
	"""Delete

	Make a DELETE request

	Arguments:
		service (str): The service to call
		path (str): The path on the service
		req (dict): The request details, which can include 'data',
					'environment', and 'session'

	Returns:
		Response
	"""
	return request(service, 'delete', path, req)

def internal_key(key = None):
	"""Internal Key

	Generates or validates an internal key so services can communicate with
	each other

	Arguments:
		key (str): Passed to validate a key

	Returns:
		bool
	"""

	# Pull in salt
	global __msSalt

	# Generate a timestamp
	iTime = int(time())

	# If no key was passed
	if key is None:

		# Generate a timestamp and store it as a string
		sTime = str(iTime)

		# Generate a sha1 from the salt and parts of the time
		sSHA1 = sha1(sTime[5:].encode('utf-8') + __msSalt.encode('utf-8') + sTime[:5].encode('utf-8')).hexdigest()

		# Generate a key using the sha1 and the time
		return sSHA1 + ':' + sTime

	# If the key was passed
	else:
		try:
			# Split the key into sha1 and timestamp
			sSHA1_, sTime = key.split(':')

			# If the time is not close enough
			if iTime - int(sTime) > __miInternal:
				return False

			# Generate a sha1 from the salt and parts of the time
			sSHA1 = sha1(sTime[5:].encode('utf-8') + __msSalt.encode('utf-8') + sTime[:5].encode('utf-8')).hexdigest()

			# If the sha1s match return true
			return sSHA1 == sSHA1_

		# If something went wrong, return false
		except Exception:
			return False

def read(service, path, req = {}):
	"""Read

	Make a GET request

	Arguments:
		service (str): The service to call
		path (str): The path on the service
		req (dict): The request details, which can include 'data',
					'environment', and 'session'

	Returns:
		Response
	"""
	return request(service, 'read', path, req)

def register(services, restconf, salt, internal=5):
	"""Register

	Takes a dictionary of services to their instances, or None for remote
	services which will be found via the config

	Arguments:
		services (dict): Services being registered
		restconf (dict): Configuration variables for remote services
		salt (str): The salt used for internal key generation
		internal (uint): The time in seconds an internal key is valid

	Raises:
		ValueError

	Returns:
		None
	"""

	# Pull in the global salt and internal variables and set them
	global __msSalt, __miInternal
	__msSalt = salt
	__miInternal = internal

	# If we didn't get a dictionary
	if not isinstance(services, dict):
		raise ValueError('services')

	# Loop through the list of services to register
	for k,v in services.items():

		# If verbose requested
		if __mbVerbose: print('Registering service "%s": ' % str(k), end='')

		# If we received a local instance
		if isinstance(v, Service):

			# Store it
			__mdRegistered[k] = {"instance":v}

			# If verbose requested
			if __mbVerbose:	print('instance')

			# Call the services initialise method
			v.initialise()

		# Else the service is remote
		elif v is None:

			# Make sure we have the service
			if k not in restconf:
				raise ValueError('services.%s' % k)

			# Store it
			__mdRegistered[k] = {"url":restconf[k]['url']}

			# If verbose mode is on
			if __mbVerbose:	print('%s' % __mdRegistered[k]['url'])

		# Else, the value is invalid
		else:
			raise ValueError('services.%s' % str(k))

def update(service, path, req = {}):
	"""Update

	Make a PUT request

	Arguments:
		service (str): The service to call
		path (str): The path on the service
		req (dict): The request details, which can include 'data',
					'environment', and 'session'

	Returns:
		Response
	"""
	return request(service, 'update', path, req)

def verbose(flag=True):
	"""Verbose

	Puts Services in verbose mode for easy tracking of requests

	Arguments:
		flag (bool): defaults to True

	Returns:
		None
	"""

	global __mbVerbose

	if __mbVerbose and not flag:
		print('Service verbose mode will be turned off')

	__mbVerbose = flag

	if __mbVerbose:
		print('Service verbose mode has been turned on')

class Response(object):
	"""Response

	Represents a standard result from any/all requests
	"""

	def __init__(self, data = undefined, error = undefined, warning = undefined):
		"""Constructor

		Initialises a new Response instance

		Arguments:
			data (mixed): If a request returns data this should be set
			error (mixed): If a request has an error, this can be filled with
				a code and message string
			warning (mixed): If a request returns a warning this should be set

		Raises:
			ValueError

		Returns:
			Response
		"""

		# If there's data, store it as is
		if data is not undefined:
			self.data = data

		# If there's an error, figure out what type
		if error is not undefined:

			# If we got an int, it's a code with no message string
			if isinstance(error, int):
				self.error = {'code': error, 'msg': ''}

			# If we got a string, it's a message with no code
			elif isinstance(error, str):
				self.error = {'code': 0, 'msg': error}

			# If it's a tuple, 0 is a code, 1 is a message
			elif isinstance(error, tuple):
				self.error = {'code': error[0], 'msg': error[1]}

			# If we got a dictionary, assume it's already right
			elif isinstance(error, dict):
				self.error = error

			# If we got an exception
			elif isinstance(error, Exception):

				# If we got another Response in the Exception, store the error
				#	from it
				if isinstance(error.args[0], Response):
					self.error = error.args[0].error

				# Else, try to pull out the code and message
				else:
					self.error = {'code': error.args[0], 'msg': ''}
					if len(error.args) > 1: self.error['msg'] = error.args[1]

			# Else, we got something invalid
			else:
				raise ValueError('error')

		# If there's a warning, store it as is
		if not warning is undefined:
			self.warning = warning

	def __str__(self):
		"""str

		Python magic method to return a string from the instance

		Returns:
			str
		"""

		# Create a temp dict
		dRet = {}

		# If there's data
		try: dRet['data'] = self.data
		except AttributeError: pass

		# If there's an error
		try: dRet['error'] = self.error
		except AttributeError: pass

		# If there's a warning
		try: dRet['warning'] = self.warning
		except AttributeError: pass

		# Convert the dict to JSON and return it
		return jsonb.encode(dRet)

	def data_exists(self):
		"""Data Exists

		Returns True if there is data in the Response

		Returns:
			bool
		"""
		try: return self.data != None
		except AttributeError: return False

	def error_exists(self):
		"""Error Exists

		Returns True if there is an error in the Response

		Returns:
			bool
		"""
		try: return self.error != None
		except AttributeError: return False

	@classmethod
	def from_dict(cls, val):
		"""From Dict

		Converts a dict back into an Response

		Arguments:
			val (dict): A valid dict

		Returns:
			Response
		"""

		# Create a new instance
		o = cls()

		# If there's data
		try: o.data = val['data']
		except KeyError: pass

		# If there's an error
		try: o.error = val['error']
		except KeyError: pass

		# If there's a warning
		try: o.warning = val['warning']
		except KeyError: pass

		# Return the instance
		return o

	@classmethod
	def from_json(cls, val):
		"""From JSON

		Tries to convert a string made from str() back into an Response

		Arguments:
			val (str): A valid JSON string

		Returns:
			Response
		"""

		# Try to convert the string to a dict
		try: d = jsonb.decode(val)
		except ValueError as e: raise ValueError('val', str(e))
		except TypeError as e: raise ValueError('val', str(e))

		# Return the fromDict result
		return cls.from_dict(d)

	def to_dict(self):
		"""To Dict

		Converts the Response into a dict

		Returns:
			dict
		"""

		# Init the return
		dRet = {}

		# Look for a data attribute
		try: dRet['data'] = self.data
		except AttributeError: pass

		# Look for an error attribute
		try: dRet['error'] = self.error
		except AttributeError: pass

		# Look for a warning attribute
		try: dRet['warning'] = self.warning
		except AttributeError: pass

		# Return the dict
		return dRet

	def warning_exists(self):
		"""Warning Exists

		Returns True if there is a warning in the Response

	Returns:
			bool
		"""
		try: return self.warning != None
		except AttributeError: return False

class Error(Response):
	"""Error

	Shorthand form of Response(error=)
	"""

	def __init__(self, code, msg = None):
		"""Constructor

		Initialises a new Response instance

		Arguments:
			code (uint): The error code
			msg (mixed): Optional message for more info on the error

		Returns:
			Error
		"""

		# Set the error code
		self.error = {
			'code': code,
			'msg': msg
		}

class ResponseException(Exception):
	"""Response Exception

	Stupid python won't let you raise anything that doesn't extend BaseException
	"""

	def __init__(self, data = undefined, error = undefined, warning = undefined):
		"""Constructor

		Dumb dumb python

		Arguments:
			data (mixed): If a request returns data this should be set
			error (mixed): If a request has an error, this can be filled with
				a code and message string
			warning (mixed): If a request returns a warning this should be set

		Returns:
			ResponseException
		"""

		# If we got a Response object
		if isinstance(data, Response):
			super().__init__(data)

		# Else, construct the Response and pass it to the parent
		else:
			super().__init__(Response(data, error, warning))

class Service(object):
	"""Service

	The object to build all Services from
	"""

	__pathToDef = {}
	"""Map of paths to function name"""

	__keyToError = {
		'data': Errors.SERVICE_NO_DATA,
		'session': Errors.SERVICE_NO_SESSION
	}
	"""Maps missing data keys to error codes"""

	__nounRegex = re.compile(r'([a-z]+(?:_[a-z]+)*)_(create|delete|read|update)')
	"""Regular Expression to match to valid service noun method"""

	def create(self, path, req):
		"""Create

		Create a new object

		Arguments:
			path (str): The path passed to the request
			req (dict): The request details, which can include 'data',
						'environment', and 'session'

		Return:
			Response
		"""

		# Generate the method name from the URI
		sMethod = self.path_to_method(path, '_create')

		# Try to find the method
		try:
			f = getattr(self, sMethod)

		# Method doesn't exist, URI is invalid
		except AttributeError as e:

			# If the method wasn't found
			if "'%s'" % sMethod in e.args[0]:
				return Response(error=(Errors.SERVICE_NO_SUCH_NOUN, 'POST %s' % path))
			else:
				raise

		# Try to call the method
		try:
			return f(req)

		# If we got a KeyError
		except KeyError as e:
			if e.args[0] in self.__keyToError:
				return Error(self.__keyToError[e.args[0]])
			raise e

		# Response thrown
		except ResponseException as e:
			return e.args[0]

	def delete(self, path, req):
		"""Delete

		Delete an existing object

		Arguments:
			path (str): The path passed to the request
			req (dict): The request details, which can include 'data',
						'environment', and 'session'

		Return:
			Response
		"""

		# Generate the method name from the URI
		sMethod = self.path_to_method(path, '_delete')

		# Try to find the method
		try:
			f = getattr(self, sMethod)

		# Method doesn't exist, URI is invalid
		except AttributeError as e:

			# If the method wasn't found
			if "'%s'" % sMethod in e.args[0]:
				return Response(error=(Errors.SERVICE_NO_SUCH_NOUN, 'DELETE %s' % path))
			else:
				raise

		# Try to call the method
		try:
			return f(req)

		# If we got a KeyError
		except KeyError as e:
			if e.args[0] in self.__keyToError:
				return Error(self.__keyToError[e.args[0]])
			raise e

		# Response thrown
		except ResponseException as e:
			return e.args[0]

	def initialise(self):
		"""Initialise

		Initialises the instance and returns itself for chaining

		Returns:
			Service
		"""

		# Return self for chaining
		return self

	def generateURLS(self, prefix = None):
		"""Generate URLS

		Finds all the appropriate functions on the Child service and returns
		a dictionary of URLs to bound functions

		Arguments:
			prefix (str): The optional prefix for urls

		Returns:
			{ str : bound method }
		"""

		# Fetch all available methods
		lURLs = []

		# Go through each available method
		for s in dir(self):

			# Try matching the proper format
			oMatch = self.__nounRegex.match(s)

			# If we have a match
			if oMatch:
				lURL = prefix and [prefix] or []
				lURL.extend(oMatch.group(1).split('_'))
				lURLs.append(['/'.join(lURL), oMatch.group(2)])

		# Returns the generated urls
		return lURLs

	@classmethod
	def install(cls):
		"""Install

		Installs any necessary DBs, configs, etc, needed by the Service when it
		is first installed

		Raises:
			NotImplementedError

		Returns:
			bool
		"""
		raise NotImplementedError('Must implement the "install" method')

	def read(self, path, req):
		"""Read

		Read an existing object

		Arguments:
			path (str): The path passed to the request
			req (dict): The request details, which can include 'data',
						'environment', and 'session'

		Return:
			Response
		"""

		# Generate the method name from the URI
		sMethod = self.path_to_method(path, '_read')

		# Try to find the method
		try:
			f = getattr(self, sMethod)

		# Method doesn't exist, URI is invalid
		except AttributeError as e:

			# If the method wasn't found
			if "'%s'" % sMethod in e.args[0]:
				return Response(error=(Errors.SERVICE_NO_SUCH_NOUN, 'GET %s' % path))
			else:
				raise

		# Try to call the method
		try:
			return f(req)

		# If we got a KeyError
		except KeyError as e:
			if e.args[0] in self.__keyToError:
				return Error(self.__keyToError[e.args[0]])
			raise e

		# Response thrown
		except ResponseException as e:
			return e.args[0]

	def update(self, path, req):
		"""Update

		Update an existing object

		Arguments:
			path (str): The path passed to the request
			req (dict): The request details, which can include 'data',
						'environment', and 'session'

		Return:
			Response
		"""

		# Generate the method name from the URI
		sMethod = self.path_to_method(path, '_update')

		# Try to find the method
		try:
			f = getattr(self, sMethod)

		# Method doesn't exist, URI is invalid
		except AttributeError as e:

			# If the method wasn't found
			if "'%s'" % sMethod in e.args[0]:
				return Response(error=(Errors.SERVICE_NO_SUCH_NOUN, 'PUT %s' % path))
			else:
				raise

		# Try to call the method
		try:
			return f(req)

		# If we got a KeyError
		except KeyError as e:
			if e.args[0] in self.__keyToError:
				return Error(self.__keyToError[e.args[0]])
			raise e

		# Response thrown
		except ResponseException as e:
			return e.args[0]

	@classmethod
	def path_to_method(cls, path, append=''):
		"""Path to Method

		Takes a path and converts it to the standard naming for Service methods

		Arguments:
			path (str): The path to parse
			append (str): If set, appended to method name

		Returns:
			str
		"""

		# If we already have the function name associated
		try:
			sRet = cls.__pathToDef[path]

		# Else, if we haven't generated it yet
		except KeyError:

			# Generate the name from the path
			sRet = ''
			iLen = len(path)
			i = 0
			while i < iLen:
				sRet += (path[i] in ['/', '-']) and '_' or path[i]
				i += 1

			# Store it for next time
			cls.__pathToDef[path] = sRet

		# Return the generated function name plus the append
		return sRet + append
