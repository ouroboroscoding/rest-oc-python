from setuptools import setup

with open('README.md', 'r') as oF:
	long_description=oF.read()

setup(
	name='Rest-OC',
	version='1.2.0',
	description='Rest-OC is a library of python 3 modules for rapidly setting up REST microservices.',
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
	license='MIT',
	packages=['RestOC'],
	python_requires='>=3.10',
	install_requires=[
		'arrow==1.2.3',
		'bottle==0.12.25',
		'format-oc==1.6.1',
		'gunicorn==20.1.0',
		'Jinja2==3.1.2',
		'markupsafe==2.0.1',
		'pdfkit==1.0.0',
		'piexif==1.1.3',
		'Pillow==9.3.0',
		'PyMySQL==1.0.2',
		'redis==4.5.1',
		'requests==2.28.2',
		'rethinkdb==2.4.9'
	],
	zip_safe=True
)