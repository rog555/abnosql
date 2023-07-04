
import json


def sqlite3_query(db, statement, params):
    if params is not None:
        params = tuple(
            _['value'] if 'value' in _
            else list(_.values())[0]
            for _ in params
        )

    cur = None
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
