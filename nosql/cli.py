#!/usr/bin/env python
import argparse
import json


from nosql import table


def cli():
    ap = argparse.ArgumentParser()
    ap.add_argument('table')
    ap.add_argument('operation')
    ap.add_argument('--database', '-d', default='dynamodb')
    ap.add_argument('--key', '-k')
    ap.add_argument('--partition-key', '-pk')
    args = ap.parse_args()

    tb = table(args.table, database=args.database)
    print('got table')

    obj = None
    if args.operation == 'get_item':
        obj = tb.get_item(id=args.key, tenantId=args.partition_key)
    else:
        raise Exception('operation not supported')

    print(json.dumps(obj, indent=2))


if __name__ == '__main__':
    cli()
