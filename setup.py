from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='zmq_transport_stream',
    version='0.1.15',
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
    install_requires=[
        'boto3',
        'zmq',
        'argcomplete',
        'pyyaml',
        'python-dotenv',
        'kinesis-python',
    ],
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    entry_points={
        'console_scripts': [
            'transport-stream-dev-publisher=dev_utils:dev_publisher',
            'zmq-scope=dev_utils:dev_scope',
            'zmq-transport-stream=transport_stream:run_service',
        ],
    },
)
