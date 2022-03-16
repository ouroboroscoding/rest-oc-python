from setuptools import setup

with open('README.md', 'r') as oF:
	long_description=oF.read()

setup(
	name='Rest-OC',
	version='0.9.21',
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
	author='Chris Nasr - Ouroboros Coding',
	author_email='chris@ouroboroscoding.com',
	license='Apache-2.0',
	packages=['RestOC'],
	install_requires=[
		'arrow==1.1.0',
		'bottle==0.12.19',
		'format-oc==1.5.15',
		'gunicorn==20.0.4',
		'hiredis==1.1.0',
		'Jinja2==2.11.3',
		'pdfkit==0.6.1',
		'Pillow==8.4.0',
		'PyMySQL==0.10.1',
		'redis==3.5.3',
		'requests==2.25.1',
		'rethinkdb==2.4.7'
	],
	zip_safe=True
)
