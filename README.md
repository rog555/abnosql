# NoSQL Abstraction Library

Basic CRUD and query support for NoSQL databases, allowing for portable cloud native applications

- AWS DynamoDB <img height="15" width="15" src="https://unpkg.com/simple-icons@v9/icons/amazondynamodb.svg" />
- Azure Cosmos NoSQL <img height="15" width="15" src="https://unpkg.com/simple-icons@v9/icons/microsoftazure.svg" />

This library is not intended to create databases/tables, use Terraform/ARM/CloudFormation etc for that

Why not just use the name 'nosql' or 'pynosql'? because they already exist on pypi :-)

[![tests](https://github.com/rog555/abnosql/actions/workflows/python-package.yml/badge.svg)](https://github.com/rog555/abnosql/actions/workflows/python-package.yml)[![codecov](https://codecov.io/gh/rog555/abnosql/branch/main/graph/badge.svg?token=9gTkGPgASh)](https://codecov.io/gh/rog555/abnosql)

- [NoSQL Abstraction Library](#nosql-abstraction-library)
  - [Installation](#installation)
- [Usage](#usage)
  - [API Docs](#api-docs)
  - [Querying](#querying)
  - [Indexes](#indexes)
  - [Partition Keys](#partition-keys)
- [Configuration](#configuration)
  - [AWS DynamoDB](#aws-dynamodb)
  - [Azure Cosmos NoSQL](#azure-cosmos-nosql)
- [Plugins and Hooks](#plugins-and-hooks)
- [Testing](#testing)
  - [AWS DynamoDB](#aws-dynamodb-1)
  - [Azure Cosmos NoSQL](#azure-cosmos-nosql-1)
- [CLI](#cli)
- [Future Enhancements / Ideas](#future-enhancements--ideas)


## Installation

```
pip install abnosql[dynamodb]
pip install abnosql[cosmos]
```

By default, abnosql does not include database dependencies.  This is to facilitate packaging
abnosql into AWS Lambda or Azure Functions (for example), without over-bloating the packages

# Usage

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

assert tb.query({'hk': '1'})['items'] == [item]

# be careful not to use cloud specific statements!
assert tb.query_sql(
    'SELECT * FROM mytable WHERE hk = @hk AND num > @num',
    {'@hk': '1', '@num': 5}
)['items'] == [item]

tb.delete_item({'hk': '1', 'rk': 'a'})
```

## API Docs

See [API Docs](https://rog555.github.io/abnosql/abnosql/table.html)

## Querying

`query()` performs DynamoDB [Query](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html) using KeyConditionExpression and exact match on FilterExpression if filters are supplied.  For Cosmos, SQL is generated.  This is the safest/most cloud agnostic way to query and probably OK for most use cases.

`query_sql()` performs Dynamodb [ExecuteStatement](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ExecuteStatement.html) passing in the supplied [PartiQL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.html) statement.  Cosmos uses the NoSQL [SELECT](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/select) syntax.

During mocked tests, [SQLGlot](https://sqlglot.com/) is used to [execute](https://sqlglot.com/sqlglot.html#sql-execution) the statement, so results may differ...

Care should be taken with `query_sql()` to not to use SQL features that are specific to any specific provider (breaking the abstraction capability of using abnosql in the first place)

## Indexes

Beyond partition and range keys defined on the table, indexes are not currently supported - and these will likey differ between providers anyway (eg DynamoDB supports [Secondary Indexes](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html), whereas [Cosmos](https://learn.microsoft.com/en-us/azure/cosmos-db/index-overview) has Range, Spatial and Composite.


## Partition Keys

A few methods such as `get_item()`, `delete_item()` and `query()` need to know partition/hash keys as defined on the table.  To avoid having to configure this or lookup from the provider, the convention used is that the first kwarg or dictionary item is the partition key, and if supplied the 2nd is the range/sort key.


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

abnosql uses pluggy and registers in the `abnosql.table` namespace

The following hooks are available

- `set_config` - set config
- `get_item_post` - called after `get_item()`, can return modified data
- `put_item_post`
- `put_items_post`
- `delete_item_post`

See the [TableSpecs](https://github.com/rog555/abnosql/blob/main/abnosql/table.py#L16) and example [test_hooks()](https://github.com/rog555/abnosql/blob/main/tests/common.py#L70)

# Testing

## AWS DynamoDB

Use `moto` package and `abnosql.mocks.mock_dynamodbx` 

mock_dynamodbx is used for query_sql and only needed if/until moto provides better partiql support

Example:

```
from abnosql.mocks import mock_dynamodbx 
from moto import mock_dynamodb

@mock_dynamodb
@mock_dynamodbx  # needed for query_sql only
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

# CLI

Small abnosql CLI installed with few of the commands above

```
Usage: abnosql [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  delete-item
  get-item
  put-item
  put-items
  query
  query-sql
```

Example querying table in Azure Cosmos, with cosmos.json config file containing endpoint, credential and database

```
$ abnosql query-sql mytable 'SELECT * FROM mytable' -d cosmos -c cosmos.json
partkey      id      num  obj                                          list       str
-----------  ----  -----  -------------------------------------------  ---------  -----
p1           p1.1      5  {'foo': 'bar', 'num': 5, 'list': [1, 2, 3]}  [1, 2, 3]  str
p2           p2.1      5  {'foo': 'bar', 'num': 5, 'list': [1, 2, 3]}  [1, 2, 3]  str
p2           p2.2      5  {'foo': 'bar', 'num': 5, 'list': [1, 2, 3]}  [1, 2, 3]  str
```

# Future Enhancements / Ideas

- [ ] test pagination & exception handling
- [ ] [Google Firestore](https://cloud.google.com/python/docs/reference/firestore/latest) support, ideally in the core library (though could be added outside via use of the plugin system).  Would need something like [FireSQL](https://firebaseopensource.com/projects/jsayol/firesql/) implemented for oython, maybe via sqlglot
- [ ] Simple caching (maybe) using globals (used for AWS Lambda / Azure Functions)
- [ ] PostgresSQL support using JSONB column (see [here](https://medium.com/geekculture/json-and-postgresql-using-json-to-mimic-nosqls-storage-benefits-1564c69f61fc) for example).  Would be nice to avoid an ORM and having to define a model for each table...
- [ ] blob storage backend? could use something similar to [NoDB](https://github.com/Miserlou/NoDB) but maybe combined with [smart_open](https://github.com/RaRe-Technologies/smart_open) and DuckDB's [Hive Partitioning](https://duckdb.org/docs/data/partitioning/hive_partitioning.html)
- [ ] Redis..
- [ ] Hook implementations to write to ElasticSearch / OpenSearch for better searching.  Useful when not able to use [AWS Stream Processors](https://aws.amazon.com/blogs/compute/indexing-amazon-dynamodb-content-with-amazon-elasticsearch-service-using-aws-lambda/) [Azure Change Feed](https://learn.microsoft.com/en-us/azure/cosmos-db/change-feed), or [Elasticstore](https://github.com/acupofjose/elasticstore). Why? because not all databases support stream processing, and if they do you don't want the hastle of using [CDC](https://berbagimadani.medium.com/sync-postgresql-to-elasticsearch-and-cdc-change-data-capture-b847e8bcf568)
- [ ] database credential lookup using cloud native secret/vault services