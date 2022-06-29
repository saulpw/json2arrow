#!/usr/bin/env python3

import sys
import gzip
import json

import schema_infer
import readysetdata as rsd

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



def main(schemafn, datafn, outfn):
    schemadict = json.loads(open(schemafn).read())
    schema = pa.schema(schema_infer.schemify(schemadict))
    out = {k:list() for k in schemadict}
    for i, row in enumerate(rsd.parse_jsonl(map(bytes.decode, gzip.open(datafn)))):
        for k, v in row.items():
            out[k].append(coerce_deep(v, schemadict[k]))

    batch = pa.RecordBatch.from_pydict(out, schema=schema)
    writer = pq.ParquetWriter(outfn, schema)
    writer.write_batch(batch)
    writer.close()


main(*sys.argv[1:])
