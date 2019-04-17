#import os
#import sys
#from distutils.sysconfig import get_python_lib

from setuptools import find_packages, setup

# Lookup the version from the code
VERSION = __import__('dbcluster').__VERSION__

EXCLUDE_FROM_PACKAGES = []

install_requires = [line.strip() for line in open("requirements.txt")]

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='dbcluster',
    version=VERSION,
    url='http://github.com/krishardy/dbcluster',
    download_url='https://github.com/krishardy/dbcluster/archive/{}.tar.gz'.format(VERSION),
    author='Kris Hardy',
    author_email='kris@abqsoft.com',
    description=('Database Cluster Session Manager'),
    license='Apache 2.0',
    packages=find_packages(exclude=EXCLUDE_FROM_PACKAGES),
    include_package_data=True,
    entry_points={
    },
    extras_require={
    },
    install_requires=install_requires,
    data_files=[
        "README.md",
        "requirements.txt",
        "dev-requirements.txt"
        ],
    zip_safe=False,
    use2to3=False,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries',
    ],
    long_description_content_type="text/markdown",
    long_description=long_description
)


