from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='zmq_transport_stream',
    version='0.3.7',
    description='(de-)/mux ØMQ sockets over a kinesis trasport stream',
    url='https://collinsaerospace.com',
    author='Dan Hahne',
    author_email='contact@danhahne.com',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: System Tools',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
    ],
    install_requires=['boto3', 'zmq'],
    packages=['zmq_transport'],
)
