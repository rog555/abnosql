import json

import boto3  # type: ignore


def item(hk, rk=None):
    _item = {
        'hk': hk,
        'num': 5,
        'obj': {
            'foo': 'bar',
            'num': 5,
            'list': [1, 2, 3],
        },
        'list': [1, 2, 3],
        'str': 'str'
    }
    if rk is not None:
        _item['rk'] = rk
    return _item


def items(hks=None, rks=None):
    _items = []
    for hk in hks or []:
        if rks:
            for rk in rks:
                _items.append(item(hk, rk))
        else:
            _items.append(item(hk))
    return _items


def create_table(name, hks=None, rks=None, _db=None):

    # sqlite3 backend for query
    if _db is not None:
        di = item(hks[0], rks[0] if rks else None)
        cols = ', '.join([
            k + ' ' + (
                'INTEGER' if isinstance(v, int)
                else 'REAL' if isinstance(v, float)
                else 'TEXT'
            )
            for k, v in di.items()
        ])
        sql = f'CREATE TABLE IF NOT EXISTS {name} ({cols});'
        # print(sql)
        _db.cursor().execute(sql)
        _db.cursor().execute(f'DELETE FROM {name};')
        _db.commit()

        if hks:
            _items = items(hks, rks)
            for _item in _items:
                cols = ', '.join(di.keys())
                placeholders = ', '.join(['?' for _ in di.keys()])
                vals = [
                    json.dumps(val) if type(val) in [dict, list]
                    else val
                    for val in _item.values()
                ]
                sql = f'INSERT INTO {name}({cols}) VALUES ({placeholders});'
                # print(sql)
                _db.cursor().execute(sql, tuple(vals))

        # debug
        if False:
            cur = _db.cursor().execute(f'SELECT * FROM {name};')
            rows = cur.fetchall()
            for row in rows:
                print(row)

        return

    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    key_schema = [
        {'AttributeName': 'hk', 'KeyType': 'HASH'}
    ]
    attr_defs = [
        {'AttributeName': 'hk', 'AttributeType': 'S'}
    ]
    if rks is not None:
        key_schema.append({'AttributeName': 'rk', 'KeyType': 'RANGE'})
        attr_defs.append({'AttributeName': 'rk', 'AttributeType': 'S'})
    params = {
        'TableName': name,
        'KeySchema': key_schema,
        'AttributeDefinitions': attr_defs,
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    }
    dynamodb.create_table(**params)
    _table = dynamodb.Table(name)
    if hks:
        _items = items(hks, rks)
        for _item in _items:
            _table.put_item(Item=_item)


def db_query(db, statement, params):
    if params is not None:
        params = tuple(
            _['value'] if 'value' in _
            else list(_.values())[0]
            for _ in params
        )

    cur = None
    print(f'db_query({statement}) params: {params}')
    if params is not None:
        cur = db.cursor().execute(statement, params)
    else:
        cur = db.cursor().execute(statement)
    cols = [_[0] for _ in cur.description]
    items = []
    for row in cur.fetchall():
        item = dict(zip(cols, row))
        for key in item.keys():
            val = item[key]
            if not isinstance(val, str) or len(val) < 3:
                continue
            if val[0] in '[{' and val[-1] in '}]':
                try:
                    item[key] = json.loads(val)
                except Exception:
                    item[key] = val
        items.append(item)
    return items
