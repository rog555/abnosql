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
    'jsonschema',
    'pluggy',
    'pyyaml'
]
cli_deps = [
    'click',
    'tabulate'
]
aws_kms_deps = [
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
azure_kms_deps = [
    'azure-identity',
    'azure-keyvault-keys',
    'cryptography'  # already used by azure-identity
]
all_deps = (
    base_deps
    + cli_deps
    + aws_dynamodb_deps
    + aws_kms_deps
    + azure_cosmos_deps
    + azure_kms_deps
)
test_deps = all_deps + [
    'coverage',
    'moto[dynamodb]',
    'moto[kms]',
    'mypy',
    'pytest',
    'pytest-cov',
    'responses',
    'sqlglot'
]
dev_deps = test_deps + [
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

    tests_require=test_deps,
    extras_require={
        'dev': dev_deps,
        'test': test_deps,
        'cli': cli_deps,
        'aws': aws_dynamodb_deps + aws_kms_deps,
        'azure': azure_cosmos_deps + azure_kms_deps,
        'dynamodb': aws_dynamodb_deps,
        'cosmos': azure_cosmos_deps,
        'aws-kms': aws_kms_deps,
        'azure-kms': azure_kms_deps
    },
    python_requires='>=3.9,<4.0',
    test_suite='tests',

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: System :: Distributed Computing',
        'Topic :: Database :: Front-Ends',
        'Typing :: Typed'
    ],
    entry_points={
        'console_scripts': [
            'abnosql = abnosql:cli.cli',
        ]
    }
)
