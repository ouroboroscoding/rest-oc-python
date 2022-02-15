# coding=utf8
""" Date & Time Helper Module

Several useful helper methods for use with dates and times
"""

# Compatibility
from past.builtins import basestring

__author__ = "Chris Nasr"
__copyright__ = "OuroborosCoding"
__version__ = "1.0.0"
__email__ = "chris@ouroboroscoding.com"
__created__ = "2021-05-01"

# Python imports
from math import floor

# Pip imports
import arrow

def _toArrow(val):
	"""To Arrow

	Converts a value to an Arrow instance for easier use

	Arguments:
		val (mixed): An value that can hopefully be turned into some sort of
						date/time

	Returns:
		arrow.arrow.Arrow
	"""

	# If we got a timestamp
	if isinstance(val, int):
		return arrow.get(val);

	# If we got a string
	if isinstance(val, basestring):

		# If it's only ten characters, add the time and timezone
		if len(val) == 10:
			return arrow.get('%sT00:00:00+00:00' % val)

		# If it's 19 characters, replace any space with T and add the timezone
		if len(val) == 19:
			return arrow.get('%+00:00' % val.replace(' ', 'T'))

		# If it's 25 characters, assume it's good
		if len(val) == 25:
			return arrow.get(val);

		# Raise an exception
		raise ValueError('Invalid date string', val)

	# If it's already an arrow instance
	if isinstance(val, arrow.arrow.Arrow):
		return val

	# Raise an exception
	raise ValueError('Invalid date/time', val)

def age(dob):
	"""Age

	Returns the current age of someone based on today's date and their DOB.
	This method is not %100 accurate, but it's good enough for 99% of cases

	Arguments:
		dob (Arrow|uint|str): The date of birth of the person

	Returns:
		uint
	"""

	# Make sure we have an arrow instance
	oDOB = _toArrow(dob)

	# Get the delta from today
	oDelta = arrow.get() - oDOB

	# Return the age
	return floor(oDelta.days / 365.25)

def date(d):
	"""Date

	Returns a string in YYYY-MM-DD format from a timestamp, date string, or
	arrow instance

	Arguments:
		d (Arrow|uint|str): The date to format

	Returns:
		str
	"""

	# Make sure we have an arrow instance
	oD = _toArrow(d)

	# Return the date string
	return oD.format('YYYY-MM-DD')

def dateInc(days=1, from_=None):
	"""Date Increment

	Returns a date incremented by the given days. Use negative to decrement.

	Arguments:
		days (int): The number of days to increment (or decrement) by
		from_ (mixed): Optional, the date to increment from, else today

	Returns:
		arrow.arrow.Arrow
	"""

	# If we got a from
	oDate = from_ and _toArrow(from_) or arrow.get();

	# Increment the date and return it
	return oDate.shift(days=days)

def datetime(d):
	"""Date Time

	Returns a string in YYYY-MM-DD HH:mm:ss format from a timestamp, date
	string, or arrow instance

	Arguments:
		d (Arrow|uint|str): The date to format

	Returns:
		str
	"""

	# Make sure we have an arrow instance
	oD = _toArrow(d)

	# Return the date/time string
	return oD.format('YYYY-MM-DD HH:mm:ss')

def timeElapsed(seconds, opts=None):
	"""Time Elapsed

	Returns seconds in a human readable format with several options to show/hide
	hours/minutes/seconds

	Arguments:
		seconds (uint): The seconds to convert to ((HH:)mm:)ss
		opts (dict): Optional flags:
						show_minutes: default True
						show_seconds: default True
						show_zero_hours: default False
						show_zero_minutes: default False

	Returns:
		str
	"""

	# Get the hours and remaining seconds
	h, r = divmod(seconds, 3600)

	# Get the minutes and seconds
	m, s = divmod(r, 60)

	# Generate the flags
	bShowMinutes = not opts or 'show_minutes' not in opts or opts['show_minutes']
	bShowSeconds = not opts or 'show_seconds' not in opts or opts['show_seconds']
	bShowZeroHours = opts and 'show_zero_hours' in opts and opts['show_zero_hours']
	bShowZeroMinutes = opts and 'show_zero_minutes' in opts and opts['show_zero_minutes']

	# Init the list we'll turn into time
	lTime = None

	# If we have hours
	if h:

		# Start by adding hours
		lTime = [str(h)]

		# If we want to show minutes
		if bShowMinutes:
			lTime.append(m < 10 and ('0%d' % m) or str(m))

			# If we want to show seconds (can't show seconds if no minutes)
			if bShowSeconds:
				lTime.append(s < 10 and ('0%d' % s) or str(s))

	# Else, if we have minutes
	elif m:

		# Init the time
		lTime = []

		# If we want to show zero hours
		if bShowZeroHours:
			lTime.append('0')

		# If we want to show minutes
		if bShowMinutes:
			lTime.append((bShowZeroHours and m < 10) and ('0%d' % m) or str(m))

			# If we want to show seconds (can't show seconds if no minutes)
			if bShowSeconds:
				lTime.append(s < 10 and ('0%d' % s) or str(s))

	# Else, we only have seconds
	else:

		# Init the time
		lTime = []

		# If we want to show zero hours
		if bShowZeroHours:
			lTime.extend(['0', '00'])

		# Else, if we want to show zero minutes
		elif bShowZeroMinutes:
			lTime.append('0')

		# If we want to show seconds
		if bShowMinutes and bShowSeconds:
			lTime.append(
				((bShowZeroMinutes or bShowZeroHours) and s < 10) and \
				('0%d' % s) or \
				str(s)
			)

	# Put them all together and return
	return ':'.join(lTime)
