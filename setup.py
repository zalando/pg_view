#!/usr/bin/env python
# -*- coding: utf-8 -*-
import glob
import inspect
import os
import sys

import setuptools
from setuptools import setup
from setuptools.command.test import test as TestCommand

__location__ = os.path.join(os.getcwd(), os.path.dirname(inspect.getfile(inspect.currentframe())))


def read_module(path):
    data = {}
    with open(path, 'r') as fd:
        exec(fd.read(), data)
    return data


meta = read_module(os.path.join('pg_view', 'meta.py'))
NAME = 'pg-view'
MAIN_MODULE = 'pg_view'
VERSION = meta['__version__']
DESCRIPTION = 'PostgreSQL Activity View Utility'
LICENSE = 'Apache License 2.0'
URL = 'https://github.com/zalando/pg_view'
author, email = meta['__author__'].rsplit(None, 1)
AUTHOR = author
EMAIL = email.strip('<>')
KEYWORDS = 'postgres postgresql pg database'

# Add here all kinds of additional classifiers as defined under
# https://pypi.python.org/pypi?%3Aaction=list_classifiers
CLASSIFIERS = [
    'Development Status :: 5 - Production/Stable',
    'Environment :: Console',
    'Intended Audience :: Developers',
    'Intended Audience :: System Administrators',
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: POSIX :: Linux',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: Implementation :: CPython',
    'Topic :: Database'
]

CONSOLE_SCRIPTS = ['pg_view = pg_view:main']


class PyTest(TestCommand):

    user_options = [('cov=', None, 'Run coverage'), ('cov-xml=', None, 'Generate junit xml report'), ('cov-html=',
                    None, 'Generate junit html report'), ('junitxml=', None, 'Generate xml of test results')]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.cov = None
        self.cov_xml = False
        self.cov_html = False
        self.junitxml = None

    def finalize_options(self):
        TestCommand.finalize_options(self)
        if self.cov is not None:
            self.cov = ['--cov', self.cov, '--cov-report', 'term-missing']
            if self.cov_xml:
                self.cov.extend(['--cov-report', 'xml'])
            if self.cov_html:
                self.cov.extend(['--cov-report', 'html'])

    def run_tests(self):
        try:
            import pytest
        except:
            raise RuntimeError('py.test is not installed, run: pip install pytest')
        params = {'args': self.test_args}
        if self.cov:
            params['args'] += self.cov
        params['args'] += ['--doctest-modules', MAIN_MODULE, '-s', '-vv']
        errno = pytest.main(**params)
        sys.exit(errno)


def get_install_requirements(path):
    content = open(os.path.join(__location__, path)).read()
    return [req for req in content.split('\\n') if req != '']


def read(fname):
    if sys.version_info[0] < 3:
        return open(os.path.join(__location__, fname)).read()
    else:
        return open(os.path.join(__location__, fname), encoding='utf-8').read()


def setup_package():
    # Assemble additional setup commands
    cmdclass = {}
    cmdclass['test'] = PyTest

    install_reqs = get_install_requirements('requirements.txt')

    command_options = {'test': {'cov': ('setup.py', MAIN_MODULE), 'cov_xml': ('setup.py', True)}}

    setup(
        name=NAME,
        version=VERSION,
        url=URL,
        description=DESCRIPTION,
        author=AUTHOR,
        author_email=EMAIL,
        license=LICENSE,
        keywords=KEYWORDS,
        long_description=read('README.rst'),
        classifiers=CLASSIFIERS,
        test_suite='tests',
        py_modules=[os.path.splitext(i)[0] for i in glob.glob(os.path.join(MAIN_MODULE, "*.py"))],
        packages=setuptools.find_packages(exclude=['tests']),
        install_requires=install_reqs,
        setup_requires=['flake8'],
        cmdclass=cmdclass,
        tests_require=['pytest-cov', 'pytest'],
        command_options=command_options,
        entry_points={'console_scripts': CONSOLE_SCRIPTS},
    )


if __name__ == '__main__':
    setup_package()
