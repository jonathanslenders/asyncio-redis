#!/usr/bin/env python
from setuptools import setup

setup(
    name="asyncio_redis",
    author="Jonathan Slenders",
    version="0.16.0",
    license="LICENSE.txt",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Framework :: AsyncIO",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    url="https://github.com/jonathanslenders/asyncio-redis",
    description="PEP 3156 implementation of the redis protocol.",
    long_description=open("README.rst").read(),
    packages=["asyncio_redis"],
    python_requires=">=3.6",
    extras_require={"hiredis": ["hiredis"]},
)
