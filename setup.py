import sys
import setuptools
from setuptools.command.test import test as TestCommand

import google_storage


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        retcode = pytest.main(self.test_args)

        sys.exit(retcode)


setuptools.setup(
    name='google_storage',
    version=google_storage.__version__,
    tests_require=[
        'six>=1.9.0',
        'pytest==2.7.2',
        'coverage==3.7.1'
    ],
    install_requires=[
        'setuptools',
        'httplib2==0.9.1',
        'google-api-python-client==1.4.1',
        'pycrypto'
    ],
    cmdclass={
        'test': PyTest
    },
    author_email='development@pathintel.com',
    description='Wrapper for python interfaces to GCS',
    include_package_data=True,
    packages=setuptools.find_packages()
)
