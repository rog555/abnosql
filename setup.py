#!/usr/bin/env python
import os
import re
from setuptools import find_packages  # type: ignore
from setuptools import setup


PACKAGE_DIR = os.path.abspath(os.path.dirname(__file__))


def read(*args):
    """Reads complete file contents."""
    return open(os.path.join(PACKAGE_DIR, *args)).read()


def get_version():
    """Reads the version from this module."""
    init = read('abnosql', '__init__.py')
    return re.compile(
        r"""__version__ = ['"]([0-9.]+)['"]"""
    ).search(init).group(1)


def get_requirements():
    """Reads the requirements file."""
    requirements = read("requirements.txt")
    return list(requirements.strip().splitlines())


__version__ = get_version()


base_deps = [
    'click',
    'pluggy',
    'sqlglot',
    'tabulate'
]
dynamodb_deps = [
    'boto3',
    'dynamodb_json'
]
cosmos_deps = [
    'azure-cosmos'
]
all_deps = base_deps + dynamodb_deps + cosmos_deps
tests_require = all_deps + [
    'coverage',
    'moto',
    'responses',
    'pytest',
    'pytest-cov',
]
dev_require = tests_require + [
    'pre-commit'
]

setup(
    name='abnosql',
    version=__version__,
    description='NoSQL Abstraction Library',
    long_description=read('README.md'),
    long_description_content_type="text/markdown",
    author='Roger Foskett',
    author_email='r_foskett@hotmail.com',
    maintainer='Roger Foskett',
    maintainer_email='r_foskett@hotmail.com',

    url='https://github.com/rog555/abnosql',
    download_url='http://pypi.python.org/pypi/abnosql',
    keywords='nosql, azure cosmos, aws dynamodb',

    license='MIT',
    platforms='any',
    packages=find_packages(exclude=['tests']),

    tests_require=tests_require,
    extras_require={
        'dev': dev_require,
        'test': tests_require,
        'dynamodb': dynamodb_deps,
        'cosmos': cosmos_deps
    },
    python_requires='>=3.8,<4.0',
    test_suite='tests',

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: System :: Distributed Computing',
        'Topic :: Database :: Front-Ends',
        'Typing :: Typed'
    ],
    entry_points={
        'console_scripts': [
            'abnosql = abnosql:cli',
        ]
    }
)
