from setuptools import setup

with open('README.md', 'r') as oF:
	long_description=oF.read()

setup(
	name='Rest-OC',
	version='0.10.5',
	description='RestOC is a library of python 3 modules for rapidly setting up REST microservices.',
	long_description=long_description,
	long_description_content_type='text/markdown',
	url='https://ouroboroscoding.com/rest-oc/',
	project_urls={
		'Documentation': 'https://ouroboroscoding.com/rest-oc/',
		'Source': 'https://github.com/ouroboroscoding/rest-oc-python',
		'Tracker': 'https://github.com/ouroboroscoding/rest-oc-python/issues'
	},
	keywords=['rest','microservices'],
	author='Chris Nasr - Ouroboros Coding Inc.',
	author_email='chris@ouroboroscoding.com',
	license='Apache-2.0',
	packages=['RestOC'],
	install_requires=[
		'arrow==1.2.2',
		'bottle==0.12.21',
		'format-oc==1.5.18',
		'gunicorn==20.1.0',
		'hiredis==1.1.0',
		'Jinja2==2.11.3',
		'pdfkit==0.6.1',
		'piexif==1.1.3',
		'Pillow==8.4.0',
		'PyMySQL==0.10.1',
		'redis==3.5.3',
		'requests==2.28.0',
		'rethinkdb==2.4.7'
	],
	zip_safe=True
)
