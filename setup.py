# -*- coding: utf-8 -*-
import os
import re
from setuptools import setup, find_namespace_packages


# Get README and remove badges.
README = open('README.rst').read()
README = re.sub('----.*marker', '----', README, flags=re.DOTALL)

DESCRIPTION = ('A Python version of the VDMS - web-service which allows '
               'principal users to request IMS data and IDC products.')

NAME = 'pyvdms'

setup(
    name=NAME,
    python_requires='>=3.6.0',
    description=DESCRIPTION,
    long_description=README,
    author='Pieter Smets',
    author_email='mail@pietersmets.be',
    url='https://github.com/psmsmets/pyvdms',
    download_url='https://github.com/psmsmets/pyvdms',
    license='GNU General Public License v3 (GPLv3)',
    packages=find_namespace_packages(include=['pyvdms.*']),
    keywords=[
        'CTBTO', 'VDMS', 'IMS', 'IDC', 'nms_client', 'obspy' 
    ],
    entry_points={
        'console_scripts': [
           f'{NAME}-scheduler=pyvdms.scheduler.scheduler:main'
        ],
    },
    scripts=[],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: ' +
            'GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    install_requires=[
        'numpy>=1.16',
        'obspy>=1.2.0',
        'pandas>=0.25.0',
        'humanfriendly>=4.18',
        'tabulate>=0.8.5',
        'python-crontab>=2.3.9',
        'dateparser>=0.7.2',
        'python-slugify>=4.0.0'
    ],
    use_scm_version={
        'root': '.',
        'relative_to': __file__,
        'write_to': os.path.join(NAME, 'version.py'),
    },
    setup_requires=['setuptools_scm'],
)
