# NoSQL Abstraction Library

Basic CRUD and query support for NoSQL databases

- AWS DynamoDB
- Azure Cosmos NoSQL

This library is not intended to create databases/tables, use Terraform/ARM/CloudFormation etc for that

Why not just 'nosql' or 'pynosql'? because they already exist on pypi :-)

- [NoSQL Abstraction Library](#nosql-abstraction-library)
  - [Installation](#installation)
  - [Example Usage](#example-usage)
- [Configuration](#configuration)
  - [AWS DynamoDB](#aws-dynamodb)
  - [Azure Cosmos NoSQL](#azure-cosmos-nosql)
- [Plugins and Hooks](#plugins-and-hooks)
- [Testing](#testing)
  - [AWS DynamoDB](#aws-dynamodb-1)
  - [Azure Cosmos NoSQL](#azure-cosmos-nosql-1)


## Installation

```
pip install abnosql[dynamodb]
pip install abnosql[cosmos]
```

By default, abnosql does not include database depedendencies.  This is to facilitate packaging
abnosql into AWS Lambda or Azure Functions (for example), without over-bloating the packages

## Example Usage

```
from abnosql import table
import os

os.environ['ABNOSQL_DB'] = 'dynamodb'

item = {
    'hk': '1',
    'rk': 'a',
    'num': 5,
    'obj': {
        'foo': 'bar',
        'num': 5,
        'list': [1, 2, 3],
    },
    'list': [1, 2, 3],
    'str': 'str'
}

tb = table('mytable')

tb.put_item(item)
tb.put_items([item])

# note partition/hash key should be first kwarg
assert tb.get_item(hk='1', rk='a') == item

assert tb.query({'hk': '1'}) == [item]

# be careful not to use cloud specific statements!
assert tb.query_sql(
    'SELECT * FROM mytable WHERE hk = @hk',
    {'@hk': '1'}
) == [item]

tb.delete_item({'hk': '1', 'rk': 'a'})
```

# Configuration

## AWS DynamoDB

Set the following environment variable and use the usual AWS environment variables that boto3 uses

- `ABNOSQL_DB` = "dynamodb"

Or set the boto3 session in the config

```
from abnosql import table
import boto3

tb = table(
    'mytable',
    config={'session': boto3.Session()},
    database='dynamodb'
)
```

## Azure Cosmos NoSQL

Set the following environment variables

- `ABNOSQL_DB` = "cosmos"
- `ABNOSQL_COSMOS_ACCOUNT` = your database account
- `ABNOSQL_COSMOS_ENDPOINT` = drived from `ABNOSQL_COSMOS_ACCOUNT` if not set
- `ABNOSQL_COSMOS_CREDENTIAL` = your cosmos credential
- `ABNOSQL_COSMOS_DATABASE` = cosmos database

Or define in config

```
from abnosq import table

tb = table(
    'mytable',
    config={'account': 'foo', 'credential': 'someb64key', 'database': 'bar'},
    database='cosmos'
)
```

# Plugins and Hooks

abnosql uses pluggy and registers in the `abnosq.table` namespace

The following hooks are available

- `set_config` - set config
- `get_item_post` - called after `get_item()`, can return modified data
- `put_item_post`
- `put_items_post`
- `delete_item_post`

See the [hookimpl](./abnosql/table.py) and example `test_hooks()` in the [tests](./tests/common.py)

# Testing

## AWS DynamoDB

Use `moto` package and `abnosql.mocks.mock_dynamodbx` 

Example:

```
from abnosql.mocks import mock_dynamodbx 
from moto import mock_dynamodb2

@mock_dynamodb2
@mock_dynamodbx
def test_something():
    ...
```

More examples in [tests/test_dynamodb.py](./tests/test_dynamodb.py)

## Azure Cosmos NoSQL

Use `requests` package and `abnosql.mocks.mock_cosmos` 

Example:

```
from abnosql.mocks import mock_cosmos
import requests

@mock_cosmos
@responses.activate
def test_something():
    ...
```

More examples in [tests/test_cosmos.py](./tests/test_cosmos.py)