# coding=utf8
""" Email Module

Methods for sending and receiving email
"""

__author__ = "Chris Nasr"
__copyright__ = "Ouroboros Coding Inc."
__version__ = "1.0.0"
__email__ = "chris@ouroboroscoding.com"
__created__ = "2022-08-25"

# Python imports
import platform
import re
import sys

# Module imports
from . import SMTP

last_error = ''
"""The last error generated"""

__regex = re.compile(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$')
"""E-mail address regular expression"""

__mdConf = None
"""Email conf"""

def init(conf):
	"""Init

	Initialises the module

	Returns:
		None
	"""

	# Import the module variable
	global __mdConf

	# Load email conf
	__mdConf = conf

	# If we have no hostname
	if 'hostname' not in __mdConf:
		__mdConf['hosntname'] = platform.node()

	# Init the SMTP module
	SMTP.init(**__mdConf['smtp'])

def error(message):
	"""Email Error

	Send out an email with an error message

	Arguments:
		error (str): The error to email

	Returns:
		bool
	"""

	# If we have no error_to
	if 'error_to' not in __mdConf:
		raise KeyError('missing "error_to" in EMail config. Make sure to pass it in init() config.')

	# Send the email
	bRes = send({
		'to': __mdConf['error_to'],
		'subject': '%s Error' % __mdConf['hostname'],
		'text': message
	})
	if not bRes:
		print('Failed to send email: %s' % EMail.last_error, file=sys.stderr)
		return False

	# Return OK
	return True

def send(conf):
	"""Send

	Sends an email over SMTP

	Arguments:
		conf (dict): Data used to generate the email config

	Returns:
		bool
	"""

	global last_error

	# If we haven't been initialised
	if not __mdConf:
		raise RuntimeError('Module not initialised. Call .init(conf) before using')

	# Check that we have at least one type of body
	if 'html' not in conf and 'text' not in conf:
		raise ValueError('must pass one of "text" or "html"')

	# If the from is not set
	if 'from' not in conf:
		conf['from'] = 'from' in __mdConf and __mdConf['from'] or ('webmaster@%s' % __mdConf['hostname'])

	# If there's an attachment
	if 'attachments' in conf:

		# Make sure it's a list
		if not isinstance(conf['attachments'], (list,tuple)):
			conf['attachments'] = [conf['attachments']]

		# Loop through the attachments
		for i in range(len(conf['attachments'])):

			# If we didn't get a dictionary
			if not isinstance(conf['attachments'][i], dict):
				raise ValueError('attachments.%d must be a dict' % i)

			# If the fields are missing
			try:
				DictHelper.eval(conf['attachments'][i], ['body', 'filename'])
			except ValueError as e:
				raise ValueError([('attachments.%d.%s' % (i, s), 'invalid') for s in e.args])

			# Try to decode the base64
			conf['attachments'][i]['body'] = b64decode(conf['attachments'][i]['body'])

	# Send the e-mail
	iRes = SMTP.Send(
		__mdConf['override'] or conf['to'],
		conf['subject'],
		conf
	)

	# If there was an error
	if iRes != SMTP.OK:
		last_error = '%i %s' % (iRes, SMTP.last_error())
		return False

	# Clear the error
	last_error = ''

	# Return OK
	return True

def valid(address):
	"""Valid

	Returns true if the email address is valid

	Arguments:
		address (str): The e-mail address to verify

	Returns
		bool
	"""

	# If we get a match
	if __regex.match(address):
		return True

	# No match
	return False
