#!/usr/bin/env python

import io
import os
import sys
from codecs import open
from shutil import rmtree

from setuptools import Command, setup

here = os.path.abspath(os.path.dirname(__file__))
with io.open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = '\n' + f.read()


class PublishCommand(Command):
    """Support setup.py publish."""

    description = 'Build and publish the package.'
    user_options = []

    @staticmethod
    def status(s):
        """Prints things in bold."""
        print('\033[1m{}\033[0m'.format(s))

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        try:
            self.status('Removing previous builds...')
            rmtree(os.path.join(here, 'dist'))
        except FileNotFoundError:
            pass

        self.status('Building Source and Wheel distribution...')
        os.system('{} setup.py sdist bdist_wheel'.format(sys.executable))

        self.status('Uploading the package to private PyPi via Twine...')
        os.system(
            'twine upload -r internal dist/* --config-file .pypirc --verbose --skip-existing')

        sys.exit()


requires = ['SQLAlchemy;python_version>="3.0"',
            'pandas']
version = '1.3.9'


def read(f):
    """Open a file"""
    return open(f, encoding='utf-8').read()


packages = [
    "dbrequests",
    "dbrequests.mysql",
]

tests = [p + ".tests" for p in packages]

setup(
    name='dbrequests',
    version=version,
    description='Python package for querying and connecting to databases.',
    long_description=read('README.md') + '\n\n' + read('HISTORY.md'),
    author='Matthaeus Deutsch',
    author_email='mdeutsch@outlook.com',
    long_description_content_type="text/markdown",
    url='https://github.com/INWTlab/dbrequests',
    packages=packages + tests,
    package_data={
        '': ['LICENSE'],
        'dbrequests': ['sql/*', 'tests/*'],
        'dbrequests.mysql': ['mysql/tests/*'],
    },
    install_requires=requires,
    extras_require={
        'pg': ['psycopg2'],
        'redshift': ['sqlalchemy-redshift', 'psycopg2'],
        'mysql': ['pymysql', 'datatable']
    },
    license='MIT',
    classifiers=(
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ),
    cmdclass={
        'publish': PublishCommand,
    }
)
