from setuptools import setup

with open('README.md', 'r') as oF:
	long_description=oF.read()

setup(
	name='Rest-OC',
	version='1.0.1',
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
	python_requires='>=3.10',
	install_requires=[
		'arrow==1.2.2',
		'bottle==0.12.23',
		'format-oc==1.5.18',
		'gunicorn==20.1.0',
		'hiredis==2.0.0',
		'Jinja2==3.1.2',
		'markupsafe==2.0.1',
		'pdfkit==1.0.0',
		'piexif==1.1.3',
		'Pillow==9.2.0',
		'PyMySQL==1.0.2',
		'redis==4.3.4',
		'requests==2.28.1',
		'rethinkdb==2.4.9'
	],
	zip_safe=True
)
