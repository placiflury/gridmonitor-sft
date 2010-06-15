#!/usr/bin/env python

from distutils.core import setup
#from setuptools  import setup


setup(
    name = "smscg_sft",
    version = "0.3.0",
    description = "Site Functional Tests (SFT) module for the SMSCG gridmonitor.",
    long_description = """
	This module provides a daemon that runs Site Functional Tests. The SFTs are
    stored in a database (both the test specification and the test output). 
    The SMSCG gridmonitor is accessing the database for displaying the SFTs.
    Via the SMSCG gridmonitor, the password of SFT users can be changed.
    (Currently the gridmonitor does not support further SFT editing.)
    """,
    platforms = "Linux",
    license = "BSD. Copyright (c) 2008, SMSCG - Swiss Multi Science Computing Grid. All rights reserved." ,
    author = "Placi Flury",
    author_email = "grid@switch.ch",
    url = "http://www.smscg.ch",
    download_url = "http://repo.smscg.ch",
    packages = ['sft','sft/db', 'sft/utils'],
    scripts = ['sft_daemon.py'],
    data_files=[('.',['config/config.ini','config/logging.conf',
                'test/job1.xrsl'])]
)

