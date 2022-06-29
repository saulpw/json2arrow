
# Usage

Dump a JSON unified schema from input data (must be json.gz):

    ./schema_infer.py input.json.gz > schema.json

Produce a parquet file:

    ./parser.py schema.json input.json.gz output.parquet

