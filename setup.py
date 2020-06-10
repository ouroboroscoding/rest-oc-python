from setuptools import setup

setup(
	name='rest-oc',
	version='0.4.1',
	url='https://github.com/ouroboroscoding/rest-oc-python',
	description='RestOC is a library of python 3 modules for rapidly setting up REST microservices.',
	keywords=['rest','microservices'],
	author='Chris Nasr - OuroborosCoding',
	author_email='ouroboroscode@gmail.com',
	license='Apache-2.0',
	packages=['RestOC'],
	install_requires=[
		'bottle==0.12.13',
		'format-oc==1.5.11',
		'gunicorn==19.9.0',
		'hiredis==0.2.0',
		'Jinja2==2.10.1',
		'pdfkit==0.6.1',
		'Pillow==6.2.0',
		'PyMySQL==0.9.3',
		'redis==2.10.6',
		'requests==2.20.1',
		'rethinkdb==2.4.1'
	],
	zip_safe=True
)
