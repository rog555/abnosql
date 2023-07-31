#!/usr/bin/env python
# mypy: ignore-errors
import json
import os

import click
from tabulate import tabulate  # type: ignore

from abnosql import table as _table


def dump(obj):
    if isinstance(obj, list):
        print(tabulate(obj, headers='keys'))
    else:
        print(json.dumps(obj, indent=2))


def get_key(partition_key, id_key=None):
    key = {}
    if len(partition_key.split('=')) != 2:
        raise ValueError('--partition-key format is name=value')
    vals = partition_key.split('=')
    key = {
        vals[0]: vals[1]
    }
    if id_key is not None:
        if len(id_key.split('=')) != 2:
            raise ValueError('--id-key format is name=value')
        vals = id_key.split('=')
        key[vals[0]] = vals[1]
    return key


def parse_dict_arg(arg):
    d = {}
    if arg is None:
        return None
    for val in arg.split(','):
        parts = [_.strip() for _ in val.split('=') if _.strip() != '']
        if len(parts) == 2:
            d[parts[0]] = parts[1]
    return d


def get_obj(argname, _obj, file_only=False):
    obj = None
    if os.path.isfile(_obj):
        obj = json.loads(open(_obj, 'r').read())
    elif file_only is True:
        raise ValueError(f'{argname} must be json file')
    elif _obj.startswith('{'):
        obj = json.loads(_obj)
    elif '=' in _obj:
        obj = parse_dict_arg(_obj)
    return obj


def get_config(obj):
    config_file = os.environ.get('ABNOSQL_CONFIG')
    if obj is None:
        if config_file is not None and os.path.isfile(config_file):
            obj = config_file
        else:
            return None
    return get_obj('--config', obj, True)


@click.group()
def cli():
    pass


@click.command()
@click.argument('table')
@click.argument('partition-key')
@click.option('--id-key', '-k')
@click.option('--database', '-d')
@click.option('--config', '-c')
def get_item(table, partition_key, id_key, database, config):
    tb = _table(table, get_config(config), database=database)
    dump(tb.get_item(**get_key(partition_key, id_key)))


@click.command()
@click.argument('table')
@click.argument('item')
@click.option('--database', '-d')
@click.option('--config', '-c')
def put_item(table, item, database, config):
    tb = _table(table, get_config(config), database=database)
    tb.put_item(get_obj('item', item))
    print('created')


@click.command()
@click.argument('table')
@click.argument('items')
@click.option('--database', '-d')
@click.option('--config', '-c')
def put_items(table, item, database, config):
    tb = _table(table, get_config(config), database=database)
    tb.put_item(get_obj('--items', item, True))
    print('created')


@click.command()
@click.argument('table')
@click.argument('partition-key')
@click.option('--id-key', '-i')
@click.option('--database', '-d')
@click.option('--config', '-c')
def delete_item(table, partition_key, id_key, database, config):
    tb = _table(table, get_config(config), database=database)
    tb.delete_item(**get_key(partition_key, id_key))
    print('deleted')


@click.command()
@click.argument('table')
@click.option('--partition-key', '-p')
@click.option('--id-key', '-k')
@click.option('--filters', '-f')
@click.option('--database', '-d')
@click.option('--config', '-c')
def query(table, partition_key, id_key, filters, database, config):
    tb = _table(table, get_config(config), database=database)
    dump(tb.query(
        key=get_key(partition_key, id_key) if partition_key else None,
        filters=parse_dict_arg(filters)
    )['items'])


@click.command()
@click.argument('table')
@click.argument('statement')
@click.option('--parameters', '-p')
@click.option('--database', '-d')
@click.option('--config', '-c')
def query_sql(table, statement, parameters, database, config):
    tb = _table(table, get_config(config), database=database)
    dump(tb.query_sql(
        statement=statement,
        parameters=parameters,
        limit=1
    )['items'])


cli.add_command(get_item)
cli.add_command(put_item)
cli.add_command(put_items)
cli.add_command(delete_item)
cli.add_command(query)
cli.add_command(query_sql)

if __name__ == '__main__':
    cli()
