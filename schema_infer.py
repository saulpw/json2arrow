#!/usr/bin/env python3

import sys
import json
import gzip

import pyarrow as pa


def parse_jsonl(fp):
    for line in fp:
        if len(line) > 2:
            line = line.rstrip("\n,")
            yield json.loads(line)


def typename(x):
    return type(x).__name__


def merge_schema(obj, schema, path=''):
    if obj is None:
        return schema

    elif isinstance(obj, dict):
        schema = schema or {}
        if not isinstance(schema, dict):
                raise Exception(f'cannot reconcile struct with {schema} ({typename(schema)}) at {path}')

        for k, v in obj.items():
            s = merge_schema(v, schema.get(k, None), f'{path}.{k}')
            if s is not None:
                schema[k] = s

        return schema

    elif isinstance(obj, list):
        if not obj:
            return None   # ignore empty lists

        innerschema = None  # the schema inside obj

        if schema:
            if not isinstance(schema, list):
                raise Exception(f'cannot reconcile list with {schema} ({typename(schema)}) at {path}')

            innerschema = schema[0]

        for i, v in enumerate(obj):
            innerschema = merge_schema(v, innerschema, f'{path}[{i}]')

        if innerschema is None:
            return None  #  ignore if everything inside resolves to empty

        return [innerschema]

    elif schema is None:  # if no existing schema,
        return type(obj)()  # the most basic instance of the type

    elif isinstance(obj, type(schema)):
        return schema   # just what we thought

    else:
        if isinstance(obj, float) and isinstance(schema, int):
            t = float()
        elif isinstance(obj, int) and isinstance(schema, float):
            t = float()
        else:
            t = str()

        if type(t) != type(schema):
            print(f'coercing {typename(obj)} and {typename(schema)} to {typename(t)} at {path}', file=sys.stderr)
            return t

        return schema   # same as it ever was


def schemify(d):
    if isinstance(d, dict):
        return pa.struct({k:schemify(v) for k, v in d.items()})

    elif isinstance(d, list):
        return pa.list_(schemify(d[0]))

    else:
        return pa.string()


def schema_to_arrow(d):
    return pa.schema(schemify(d))


def main():
    schema = None
    for fn in sys.argv[1:]:
        for row in rsd.parse_jsonl(map(bytes.decode, gzip.open(fn))):
            schema = merge_schema(row, schema)

    print(json.dumps(schema))


def test():
    obj = ['a','b','c']
    obj = dict(a='3', b='4')
    obj = dict(a=dict(k='3'), b='4')
    obj = dict(a=dict(), b=[])
    obj = [dict(a='1'), dict(b='2')]
#    obj = [dict(a='1'), dict(a=dict(c='2'))]  # fail
#    obj = [dict(a=['1']), dict(a=dict(c='2'))]  # fail
    print(obj, schemify(merge_schema(obj)))

#test()

if __name__ == '__main__':
    main()
