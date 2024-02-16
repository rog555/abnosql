Changelog
=========

## [v0.0.21] - 2024-02-14
- use a singleton Azure Cosmos DB client for the lifetime of the application

## [v0.0.20] - 2023-10-09
- abnosql_check_exists=False on cosmos delete bug

## [v0.0.19] - 2023-10-09
- skip validation on cosmos delete

## [v0.0.18] - 2023-10-09
- audit_user as config option

## [v0.0.17] - 2023-10-05
- check_exists, schema validation and cosmos DefaultAzureCredential cred fix
  
## [v0.0.16] - 2023-09-29
- support cosmos managed identity via DefaultAzureCredential if credential not supplied

## [v0.0.15] - 2023-09-28
- fix for dynamodb query filter and deserialization

## [v0.0.14] - 2023-09-27
- memory.py fix to query camelCase via sqlglot

## [v0.0.13] - 2023-09-25
- memory.py table minor fix to support cosmos queries with hyphen in tablenames
  
## [v0.0.12] - 2023-09-22
- default sleep seconds to 5 between cosmos update/delete for change metadata

## [v0.0.11] - 2023-09-12
- fix change metadata REMOVE bug
  
## [v0.0.10] - 2023-09-08
- fix change metadata REMOVE bug

## [v0.0.9] - 2023-08-31

- cosmos change feed eventName to INSERT, MODIFY or REMOVE

## [v0.0.8] - 2023-08-24

- add cosmos change feed support via ABNOSQL_COSMOS_CHANGE_META env var

## [v0.0.7] - 2023-08-08

- delay requiring dependencies until loading, index support for query()

## [v0.0.6] - 2023-07-31

- cosmos support for table names containing hyphens

## [v0.0.5] - 2023-07-20

- patch/update support

## [v0.0.4] - 2023-07-20

- infer ABNOSQL_DB and audit camelCase

## [v0.0.3] - 2023-07-17

- client side encryption, user audit

## [v0.0.2] - 2023-07-07

- update readme + minor CLI fix

## [v0.0.1] - 2023-07-07

- initial version supporting AWS DynamoDB and Azure Cosmos NoSQL
