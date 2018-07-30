#!/usr/bin/env python
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
import sys

if sys.version_info >= (3, 4):
    install_requires = []
else:
    install_requires = ['asyncio']

setup(
        name='asyncio_redis',
        author='Jonathan Slenders',
        version='0.15.1',
        license='LICENSE.txt',
        url='https://github.com/jonathanslenders/asyncio-redis',

        description='PEP 3156 implementation of the redis protocol.',
        long_description=open("README.rst").read(),
        packages=['asyncio_redis'],
        install_requires=install_requires,
        extras_require={
            'hiredis': ['hiredis'],
        },
)
