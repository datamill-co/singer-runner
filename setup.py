#!/usr/bin/env python

from os import path
from distutils.core import setup
from setuptools import find_packages

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='singer-runner',
    url='https://github.com/datamill-co/singer-runner',
    description='Runs Singer.io taps and targets, adding additional tooling.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    author='datamill',
    version='0.0.2',
    packages=find_packages(),
    install_requires=[
        'pyyaml>=4.2b1',
        'smart-open==1.8.0',
        'click==7.0'
    ],
    tests_require=[
        'pytest==4.5.0'
    ],
    extras_require={
        'gcs': [
            'google-cloud-storage==1.15.1'
        ]
    },
    entry_points={
        'console_scripts': [
            'singer-runner=singer_runner:main'
        ]
    }
)
