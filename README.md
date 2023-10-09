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
  - [Updates](#updates)
  - [Existence Checking](#existence-checking)
  - [Schema Validation](#schema-validation)
  - [Partition Keys](#partition-keys)
  - [Pagination](#pagination)
  - [Audit](#audit)
  - [Change Feed / Stream Support](#change-feed--stream-support)
  - [Client Side Encryption](#client-side-encryption)
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
pip install 'abnosql[dynamodb]'
pip install 'abnosql[cosmos]'
```

For optional [client side](#client-side-encryption) field level envelope encryption

```
pip install 'abnosql[aws-kms]'
pip install 'abnosql[azure-kms]'
```

By default, abnosql does not include database dependencies.  This is to facilitate packaging
abnosql into AWS Lambda or Azure Functions (for example), without over-bloating the packages

# Usage

```
from abnosql import table
import os

os.environ['ABNOSQL_DB'] = 'dynamodb'
os.environ['ABNOSQL_KEY_ATTRS'] = 'hk,rk'

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

# create/replace
tb.put_item(item)

# update - using ABNOSQL_KEY_ATTRS
updated_item = tb.put_item(
    {'hk': '1', 'rk': 'a', 'str': 'STR'},
    update=True
)
assert updated_item['str'] == 'STR'

# bulk
tb.put_items([item])

# note partition/hash key should be first kwarg
assert tb.get_item(hk='1', rk='a') == item

assert tb.query({'hk': '1'})['items'] == [item]

# scan
assert tb.query()['items'] == [item]

# be careful not to use cloud specific statements!
assert tb.query_sql(
    'SELECT * FROM mytable WHERE mytable.hk = @hk AND mytable.num > @num',
    {'@hk': '1', '@num': 4}
)['items'] == [item]

tb.delete_item({'hk': '1', 'rk': 'a'})
```

## API Docs

See [API Docs](https://rog555.github.io/abnosql/abnosql/table.html)

## Querying

`query()` performs DynamoDB [Query](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html) using KeyConditionExpression (if `key` supplied) and exact match on FilterExpression if filters are supplied.  For Cosmos, SQL is generated.  This is the safest/most cloud agnostic way to query and probably OK for most use cases.

`query_sql()` performs Dynamodb [ExecuteStatement](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ExecuteStatement.html) passing in the supplied [PartiQL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.html) statement.  Cosmos uses the NoSQL [SELECT](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/select) syntax.

During mocked tests, [SQLGlot](https://sqlglot.com/) is used to [execute](https://sqlglot.com/sqlglot.html#sql-execution) the statement, so results may differ...

Care should be taken with `query_sql()` to not to use SQL features that are specific to any specific provider (breaking the abstraction capability of using abnosql in the first place)

## Indexes

Beyond partition and range keys defined on the table, indexes currently have limited support within abnosql

 - The DynamoDB implemention of `query()` allows a [secondary index](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html) to be specified via optional `index` kwarg
 - [Cosmos](https://learn.microsoft.com/en-us/azure/cosmos-db/index-overview) has Range, Spatial and Composite indexes, however the abnosql library does not do anything yet with `index` kwarg in `query()` implementation.

## Updates

`put_item()` and `put_items()` support `update` boolean attribute, which if supplied will do an `update_item()` on DynamoDB, and a `patch_item()` on Cosmos.  For this to work however, you must specify the key attribute names, either via `ABNOSQL_KEY_ATTRS` env var as a comma separated list (eg perhaps multiple tables all share common partition/range key scheme), or as the `key_attrs` config item  when instantiating the table, eg:

```
tb = table('mytable', {'key_attrs': ['hk', 'rk']})
```

If you don't need to do any updates and only need to do create/replace, then these key attribute names do not need to be supplied

All items being updated must actually exist first, or else exception raised


## Existence Checking

If `check_exists` config attribute is `True`, then CRUD operations will raise exceptions as follows:

- `get_item()` raises `NotFoundException` if item doesnt exist
- `put_item()` raises `ExistsException` if item already exists
- `put_item(update=True)` raises `NotFoundException` if item doesnt exist to update
- `delete_item()` raises `NotFoundException` if item doesnt exist

This adds some delay overhead as abnosql must check if item exists

This can also be enabled by setting environment variable `ABNOSQL_CHECK_EXISTS=TRUE`

If for some reason you need to override this behaviour once enabled for `put_item()` create operation,
you can pass `abnosql_check_exists=False` into the item (this gets popped out so not persisten), which
will allow create operation to overwrite the existing item without throwing `ExistsException`

## Schema Validation

`config` can define jsonschema to validate upon create or update operations (via `put_item()`)

Combination of the following config attributes supported

- `schema` : jsonschema dict or yaml string, applied to both create and update
- `create_schema` : jsonschema dict/yaml only on create
- `update_schema` : jsonschema dict/yaml only on update
- `schema_errmsg` : override default error message on both create and update
- `create_schema_errmsg` : override default error message on create
- `update_schema_errmsg` : override default error message on update

You can get details of validation errors through `e.to_problem()` or `e.detail`

NOTE: `key_attrs` required when updating (see [Updates](#updates))

## Partition Keys

A few methods such as `get_item()`, `delete_item()` and `query()` need to know partition/hash keys as defined on the table.  To avoid having to configure this or lookup from the provider, the convention used is that the first kwarg or dictionary item is the partition key, and if supplied the 2nd is the range/sort key.

## Pagination

`query` and `query_sql` accept `limit` and `next` optional kwargs and return `next` in response. Use these to paginate.

This works for AWS DyanmoDB, however Azure Cosmos has a limitation with continuation token for cross partitions queries (see [Python SDK documentation](https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/cosmos/azure-cosmos)).  For Cosmos, abnosql appends OFFSET and LIMIT in the SQL statement if not already present, and returns `next`.  `limit` is defaulted to 100.  See the tests for examples

## Audit

`put_item()` and `put_items()` take an optional `audit_user` kwarg.  If supplied, absnosql will add the following to the item:

- `createdBy` - value of `audit_user`, added if does not exist in item supplied to put_item()
- `createdDate` - UTC ISO timestamp string, added if does not exist
- `modifiedBy` - value of `audit_user` always added
- `modifiedDate` - UTC ISO timestamp string, always added

You can also specify `audit_user` as config attribute to table.  If you prefer snake_case over CamelCase, you can set env var `ABNOSQL_CAMELCASE` = `FALSE`

NOTE: created* will only be added if `update` is not True in a `put_item()` operation

## Change Feed / Stream Support

**AWS DynamoDB** [Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html) allow Lambda functions to be triggered upon create, update and delete table operations.  The event sent to the lambda (see [aws docs](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.Lambda.Tutorial2.html)) contains `eventName` and `eventSourceARN`, where:

- `eventName` - name of event, eg `INSERT`, `MODIFY` or `REMOVE` (see [here](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_Record.html))
- `eventSourceARN` - ARN of the table name

This allows a single stream processor lambda to process events from multiple tables (eg for writing into ElasticSearch)

Like DynamoDB, **Azure CosmosDB** supports [change feeds](https://learn.microsoft.com/en-us/azure/cosmos-db/change-feed), however the event sent to the function (currently) omits the event source (table name) and only delete event names are available if a [preview change feed mode](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html) is enabled, which needs explicit enablement for.

Because both the eventName and eventSource are ideally needed (irrespective of preview mode or not), abnosql library automatically adds the `changeMetadata` to an item during create, update and delete, eg:

```
item = {
    "hk": "1",
    "rk": "a",
    "changeMetadata": {
        "eventName": "INSERT",
        "eventSource": "sometable"
    }
}
```

Because no REMOVE event is sent at all without preview change feed mode above - abnosql must first update the item, and then delete it.  This is also needed for the eventSource / table name to be captured in the event, so unfortunately until Cosmos supports both attributes, update is needed before a delete.  5 second synchronous sleep is added by default between update and delete to allow CosmosDB to send the update event (0 seconds results in no update event).  This can be controlled with `ABNOSQL_COSMOS_CHANGE_META_SLEEPSECS` env var (defaults to `5` seconds), and disabled by setting to `0`

This behaviour is enabled by default, however can be disabled by setting `ABNOSQL_COSMOS_CHANGE_META` env var to `FALSE` or `cosmos_change_meta=False` in table config.  `ABNOSQL_CAMELCASE` = `FALSE` env var can also be used to change attribute names used to snake_case if needed

To write an Azure Function / AWS Lambda that is able to process both DynamoDB and Cosmos events, look for `changeMetadata` first and if present use that otherwise look for `eventName` and `eventSourceARN` in the event payload assuming its DynamoDB

## Client Side Encryption

If configured in table config with `kms` attribute, abnosql will perform client side encryption using AWS KMS or Azure KeyVault

Each attribute value defined in the config is encrypted with a 256-bit AES-GCM data key generated for each attribute value:

- `aws` uses [AWS Encryption SDK for Python](https://docs.aws.amazon.com/encryption-sdk/latest/developer-guide/python.html)
- `azure` uses [python cryptography](https://cryptography.io/en/latest/hazmat/primitives/aead/#cryptography.hazmat.primitives.ciphers.aead.AESGCM.generate_key) to generate AES-GCM data key, encrypt the attribute value and then uses an RSA CMK in Azure Keyvault to wrap/unwrap (envelope encryption) the AES-GCM data key.  The module uses the [azure-keyvaults-keys](https://learn.microsoft.com/en-us/python/api/overview/azure/keyvault-keys-readme?view=azure-python) python SDK for wrap/unrap functionality of the generated data key (Azure doesnt support generate data key as AWS does)

Both providers use a [256-bit AES-GCM](https://docs.aws.amazon.com/encryption-sdk/latest/developer-guide/supported-algorithms.html) generated data key with AAD/encryption context (Azure provider uses a 96-nonce).  AES-GCM is an Authenticated symmetric encryption scheme used by both AWS and Azure (and [Hashicorp Vault](https://developer.hashicorp.com/vault/docs/secrets/transit#aes256-gcm96))

See also [AWS Encryption Best Practices](https://docs.aws.amazon.com/prescriptive-guidance/latest/encryption-best-practices/welcome.html)

Example config:

```
{
    'kms': {
        'key_ids': ['https://foo.vault.azure.net/keys/bar/45e36a1024a04062bd489db0d9004d09'],
        'key_attrs': ['hk', 'rk'],
        'attrs': ['obj', 'str']
    }
}
```

Where:
- `key_ids`: list of AWS KMS Key ARNs or Azure KeyVault identifier (URL to RSA CMK).  This is picked up via `ABNOSQL_KMS_KEYS` env var as a comma separated list (*NOTE: env var recommended to avoid provider specific code*)
- `key_attrs`: list of key attributes in the item from which the AAD/encryption context is set.  Taken from `ABNOSQL_KEY_ATTRS` env var or table `key_attrs` if defined there
- `attrs`: list of attributes keys to encrypt
- `key_bytes`: optional for azure, use your own AESGCM key if specified, otherwise generate one

If `kms` config attribute is present, abnosql will look for the `ABNOSQL_KMS` provider to load the appropriate provider KMS module (eg "aws" or "azure"), and if not present use default depending on the database (eg cosmos will use azure, dynamodb will use aws)

In example above, the key_attrs `['hk', 'rk']` are used to define the encryption context / AAD used, and attrs `['obj', 'str']` what attributes to encrypt/decrypt

With an item:

```
{
    'hk': '1',
    'rk': 'b',
    'obj': {'foo':'bar'},
    'str': 'foobar'
}
```

The encryption context / AAD is set to hk=1 and rk=b and obj and str values are encrypted

If you don't want to use any of these providers, then you can use `put_item_pre` and `get_item_post` hooks to perform your own client side encryption

See also [AWS Multi-region encryption keys](https://docs.aws.amazon.com/encryption-sdk/latest/developer-guide/configure.html#config-mrks) and set `ABNOSQL_KMS_KEYS` env var as comma list of ARNs

# Configuration

It is recommended to use environment variables where possible to avoid provider specific application code

if `ABNOSQL_DB` env var is not set, abnosql will attempt to apply defaults based on available environment variables:

- `AWS_DEFAULT_REGION` - sets database to `dynamodb` (see [aws docs](https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html))
- `FUNCTIONS_WORKER_RUNTIME` - sets database to `cosmos` (see [azure docs](https://learn.microsoft.com/en-us/azure/azure-functions/functions-app-settings#functions_worker_runtime))


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

Set the following environment variables:

- `ABNOSQL_DB` = "cosmos"
- `ABNOSQL_COSMOS_ACCOUNT` = your database account
- `ABNOSQL_COSMOS_ENDPOINT` = drived from `ABNOSQL_COSMOS_ACCOUNT` if not set
- `ABNOSQL_COSMOS_CREDENTIAL` = your cosmos credential, use [Azure Key Vault References](https://learn.microsoft.com/en-us/azure/app-service/app-service-key-vault-references?tabs=azure-cli) if using Azure Functions.  Don't set to use DefaultAzureCredential / managed identity.
- `ABNOSQL_COSMOS_DATABASE` = cosmos database

**OR** - use the connection string format:

- `ABNOSQL_DB` = "cosmos://account@credential:database" or "cosmos://account@:database" to use managed identity (credential could also be "DefaultAzureCredential")

Alternatively, define in config (though ideally you want to use env vars to avoid application / environment specific code).

```
from abnosql import table

tb = table(
    'mytable',
    config={'account': 'foo', 'database': 'bar'},
    database='cosmos'
)
```

# Plugins and Hooks

abnosql uses pluggy and registers in the `abnosql.table` namespace

The following hooks are available

- `set_config` - set config
- `get_item_post` - called after `get_item()`, can return modified data
- `put_item_pre`
- `put_item_post`
- `put_items_post`
- `delete_item_post`

See the [TableSpecs](https://github.com/rog555/abnosql/blob/main/abnosql/table.py#L16) and example [test_hooks()](https://github.com/rog555/abnosql/blob/main/tests/common.py#L70)

# Testing

## AWS DynamoDB

Use `moto` package and `abnosql.mocks.mock_dynamodbx` 

mock_dynamodbx is used for query_sql and only needed if/until moto provides full partiql support

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

To install dependencies

```
pip install 'abnosql[cli]'
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

- [x] client side encryption
- [x] test pagination & exception handling
- [ ] [Google Firestore](https://cloud.google.com/python/docs/reference/firestore/latest) support, ideally in the core library (though could be added outside via use of the plugin system).  Would need something like [FireSQL](https://firebaseopensource.com/projects/jsayol/firesql/) implemented for oython, maybe via sqlglot
- [ ] [Google Vault](https://cloud.google.com/python/docs/reference/cloudkms/latest/) KMS support
- [ ] [Hashicorp Vault](https://github.com/hashicorp/vault-examples/blob/main/examples/_quick-start/python/example.py) KMS support
- [ ] Simple caching (maybe) using globals (used for AWS Lambda / Azure Functions)
- [ ] PostgresSQL support using JSONB column (see [here](https://medium.com/geekculture/json-and-postgresql-using-json-to-mimic-nosqls-storage-benefits-1564c69f61fc) for example).  Would be nice to avoid an ORM and having to define a model for each table...
- [ ] blob storage backend? could use something similar to [NoDB](https://github.com/Miserlou/NoDB) but maybe combined with [smart_open](https://github.com/RaRe-Technologies/smart_open) and DuckDB's [Hive Partitioning](https://duckdb.org/docs/data/partitioning/hive_partitioning.html)
- [ ] Redis..
- [ ] Hook implementations to write to ElasticSearch / OpenSearch for better searching.  Useful when not able to use [AWS Stream Processors](https://aws.amazon.com/blogs/compute/indexing-amazon-dynamodb-content-with-amazon-elasticsearch-service-using-aws-lambda/) [Azure Change Feed](https://learn.microsoft.com/en-us/azure/cosmos-db/change-feed), or [Elasticstore](https://github.com/acupofjose/elasticstore). Why? because not all databases support stream processing, and if they do you don't want the hastle of using [CDC](https://berbagimadani.medium.com/sync-postgresql-to-elasticsearch-and-cdc-change-data-capture-b847e8bcf568)
