#!/usr/bin/env python
import os
from setuptools import find_packages  # type: ignore
from setuptools import setup


PACKAGE_DIR = os.path.abspath(os.path.dirname(__file__))


def read(*args):
    return open(os.path.join(PACKAGE_DIR, *args)).read()


def get_version():
    line = read('abnosql', 'version.py').splitlines()[0].strip()
    parts = line.split(' ')
    assert len(parts) == 3
    assert parts[0] == '__version__'
    assert parts[1] == '='
    return parts[2].strip('\'"')


__version__ = get_version()


base_deps = [
    'click',
    'pluggy',
    'sqlglot',
    'tabulate'
]
aws_crypto_deps = [
    'boto3',
    'aws-encryption-sdk',
]
aws_dynamodb_deps = [
    'boto3',
    'dynamodb_json'
]
azure_cosmos_deps = [
    'azure-cosmos'
]
azure_crypto_deps = [
    'azure-identity',
    'azure-keyvault-keys',
    'cryptography'
]
all_deps = (
    base_deps
    + aws_dynamodb_deps
    + aws_crypto_deps
    + azure_cosmos_deps
    + azure_crypto_deps
)
tests_require = all_deps + [
    'coverage',
    'moto[dynamodb]',
    'mypy',
    'pytest',
    'pytest-cov',
    'responses'
]
dev_require = tests_require + [
    'pdoc',
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
        'dynamodb': aws_dynamodb_deps,
        'cosmos': azure_cosmos_deps,
        'aws-crypto': aws_crypto_deps,
        'azure-crypto': azure_crypto_deps
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
