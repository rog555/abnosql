import json
import typing as t

import boto3  # type: ignore
import sqlglot  # type: ignore
from sqlglot.executor import execute  # type: ignore

from nosql.table import deserialize


# won't need this when moto dynamdob supports execute_statement / partiql
def query_table(
    statement: str,
    parameters: t.Optional[t.List[t.Dict[str, t.Any]]] = None,
    table_name: t.Optional[str] = None
) -> t.List[t.Dict]:
    if parameters is None:
        parameters = []

    def _quote(str):
        return "'" + str.translate(
            str.maketrans({
                "'": "\\'"
            })
        ) + "'"

    # find ? dynamodb placeholders
    if '?' in statement:
        _pparams = []
        for pd in parameters:
            _type, _val = list(pd.items())[0]
            if _type == 'S':
                _val = _quote(_val)
            _pparams.append(_val)
        statement = statement.replace('?', '{}').format(*_pparams)

    # cosmos style placeholders
    elif '@' in statement:
        _nparams = {pd['name']: pd['value'] for pd in parameters}
        for _param, _val in _nparams.items():
            if isinstance(_val, str):
                _val = _quote(_val)
            statement = statement.replace(_param, str(_val))

    # add params into statement
    if table_name is None:
        table_name = str(next(
            sqlglot.parse_one(statement).find_all(  # type: ignore
                sqlglot.exp.Table
            )
        ))

    # scan the table
    table = boto3.resource('dynamodb').Table(table_name)
    items = deserialize(table.scan()['Items'])

    # sqlglot execute can't handle dict or list keys...
    _unpack = {}
    for i in range(len(items)):
        for k, v in items[i].items():
            if type(v) in [dict, list]:
                _unpack[k] = True
                items[i][k] = json.dumps(v)

    # query the data
    resp = execute(statement, tables={table_name: items})
    rows = [
        dict(zip(resp.columns, row))
        for row in resp.rows
    ]

    # unpack
    if len(_unpack):
        for i in range(len(rows)):
            for k in _unpack.keys():
                rows[i][k] = json.loads(rows[i][k])

    return rows
