#!/usr/bin/env python

from distutils.core import setup
from setuptools import find_packages

setup(
    name='singer-runner',
    version='0.0.1',
    packages=find_packages(),
    install_requires=[
        'pyyaml>=4.2b1',
        'smart-open==1.7.1', 'click'
    ],
    tests_require=[
    ],
    entry_points={
        'console_scripts': [
            'singer-runner=singer_runner:main'
        ]
    }
)
