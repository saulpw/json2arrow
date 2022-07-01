#!/usr/bin/env python3

import sys
import gzip
import json

import schema_infer

import pyarrow.parquet as pq
import pyarrow as pa


def coerce_deep(v, schema, path=''):
    'Return *v* with values coerced according to *schema*.'
    if isinstance(v, list):
        return [coerce_deep(x, schema[0], f'{path}[{i}]') for i, x in enumerate(v)]
    elif isinstance(v, dict):
        return {k:coerce_deep(v.get(k, None), schema[k], f'{path}.{k}') for k in v if k in schema}
    else:
        if v is None:
            return None
        return type(schema)(v)


def pydict_from_rows(schemadict, rows):
    out = {k:list() for k in schemadict}
    for i, row in enumerate(rows):
        for k, v in row.items():
            out[k].append(coerce_deep(v, schemadict[k]))

    return out


def main(schemafn, datafn, outfn):
    schemadict = json.loads(open(schemafn).read())
    schema = pa.schema(schema_infer.schemify(schemadict))
    with pq.ParquetWriter(outfn, schema) as writer:
        rows = schema_infer.parse_jsonl(map(bytes.decode, gzip.open(datafn)))
        out = pydict_from_rows(schemadict, rows)
        batch = pa.RecordBatch.from_pydict(out, schema=schema)
        writer.write_batch(batch)


main(*sys.argv[1:])
