## Aurora-dsql-dataloader

The `aurora-dsql-loader.py` Python script is designed to load a single data file into an **Aurora-Dsql** table using the high-performance **COPY** protocol. This protocol enables fast and reliable bulk data transfer, making the script ideal for importing large datasets into a **Aurora-Dsql** table. 

There will be ongoing enhanceents to this script so please follow the README.

You can customize the data loading workflow through parameters. This tool features include support for adjustable batch size, multi-threading capability and customizable backoff.

## Pre-requisites:

1. Python 3.8+
2. Psycopg3 module installed in environment (https://www.psycopg.org/psycopg3/docs/basic/install.html)
    Python venv is recommended

3. File should contain one field per line for each of the fields in the target table (in order), unless `--cols` specified
4. Delimiter defaults to SPACE, but can be modified with `--delim` argument
5. The `--cols` argument can be used to specify a subset and/or reordering of columns (eg --cols col3,col1,col17, input file contains 3 values per line)
6. The table should already exist in the database/schema specified and should be empty

## Features
1. `--batchsize`: Batch size can be adjusted to work with differing **Aurora-Dsql** limits. Recommended batch is 1,000 as of today, including index tuples
2. `--threads`: Multiple threads can be specified, which may help increase throughput
3. The usual Postgres environment variables can be used (PGHOST, PGUSER, etc)
4. `--backoff`: Backoff (sleep) between batches

## Executing the script:

```bash
PGUSER=admin \
PGHOST=<endpoint hostname> \
PGPASSWORD="$(generate-db-auth-token --hostname $PGHOST --region <region> --action DbConnectAdmin)" \
PGDATABASE=postgres \
PGSSLMODE=require \
./aurora-dsql-loader.py --filename <_filename_> --schema <_Schema_name_> --tablename <_Table_name_> --threads 10
```

Full usage: 

``` bash
aurora-dsql-loader.py [-h] [--host HOST] [--database DATABASE] [--user USER] [--password PASSWORD | --pwgen PWGEN] --filename FILENAME --tablename TABLENAME [--batchsize BATCHSIZE] [--threads THREADS]  [--schema SCHEMA] [--cols COLS] [--delim DELIM] [--backoff BACKOFF] [--relentless] [--feedback] [--debug]
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

