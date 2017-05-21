# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

setup(
    name='groupCommunication',
    version='0.1',
    url='https://github.com/carlos96rg/groupCommunication',
    license='MIT License',
    author='Carlos Rincon, Daniel Zarco',
    install_requires=['gevent', 'pyactor'],
    test_suite='test',
)
