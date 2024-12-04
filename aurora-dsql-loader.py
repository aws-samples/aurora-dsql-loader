#!/usr/bin/env python3
#
# aurora-dsql-loader.py - Load single datafile into DSQL database using explicit batch size and multiple threads
#
# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import psycopg
import psycopg.sql as sql
import os
import datetime
import argparse
import asyncio
import threading
import sys
import logging


def setup_logging(tablename):
    """Set up logging with a dynamic file name based on the table name and the current timestamp"""

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"load_{tablename}_{timestamp}.log"

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Create a file handler with the dynamic filename
    file_handler = logging.FileHandler(log_filename)
    LOGFORMAT = "%(asctime)s %(levelname)-8s %(message)s"
    file_formatter = logging.Formatter(LOGFORMAT, datefmt="%Y-%m-%d %H:%M:%S")
    file_handler.setFormatter(file_formatter)

    # Add the file handler to the logger
    logger.addHandler(file_handler)

    return logger


async def main():

    global logger

    parse_args(sys.argv[1:])

    logger = setup_logging(args.tablename)

    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")
        logger.debug(args)

    sfr = SafeFileReader(args.filename)
    start_time = datetime.datetime.now()

    (admin_conn, admin_session_id) = await connectDB()
    if args.schema is None:
        args.schema = await get_current_schema(admin_conn)

    # Check that a table of the specified name exists, is empty, and return
    # the number of indexes so we can set the right commit batch size
    index_count = await check_table(admin_conn, args.schema, args.tablename)

    loop = asyncio.get_event_loop()
    tasks = [loop.create_task(loader(sfr)) for _ in range(int(args.threads))]
    results = await asyncio.gather(*tasks)
    all_success = any(tup[1] for tup in results)
    total_rows_loaded = sum(result[0] for result in results if result[0] is not None)
    if total_rows_loaded == 0:
        logger.info("No rows loaded")
        print("\nNo rows loaded")
    else:
        feedback = "Load{} completed, {} rows loaded in {:.2f}s".format(
            "" if all_success else " PARTIALLY",
            total_rows_loaded,
            (datetime.datetime.now() - start_time).total_seconds(),
        )
        logger.info(feedback)
        print("\n")
        print(feedback)


class SafeFileReader:
    def __init__(self, filename):
        self._lock = threading.Lock()
        self.fd = open(filename, "r")

    async def get_n_tuples(self, n_tuples):
        with self._lock:
            lines = []
            try:
                for _ in range(n_tuples):
                    line = next(self.fd).strip().split(args.delim)
                    lines.append(line)
            except StopIteration:
                pass  # End of file reached, return what we have
            return lines

    async def close(self):
        with self._lock:
            self.fd.close()


async def connectDB():
    global args

    # Use the parsed arguments to connect to the database
    conn_ret = await psycopg.AsyncConnection.connect(
        autocommit=False,
        host=args.host,
        dbname=args.database,
        user=args.user,
        password=args.password,
    )
    try:
        # Get session ID, if available
        async with conn_ret.cursor() as sescur:
            await sescur.execute("SELECT SYS.current_session_id();")
            session_id = await sescur.fetchone()
            logger.info("Connected to database, session ID {}".format(session_id[0]))
            session_id_ret = session_id[0]
    except Exception as e:
        logger.error(f"Error retrieving session ID: {e}")
        session_id_ret = ""
    return (conn_ret, session_id_ret)


async def check_table(conn, schema, table_name):
    logger.debug("Checking if table '{}' exists and is empty".format(table_name))
    try:
        tuple_count = await getTableMeta(conn, schema, table_name)
        if tuple_count >= 1:
            logger.debug("Table '{}' exists".format(table_name))
        else:
            logger.error(
                "Table '{}' does not exist in schema '{}'".format(table_name, schema)
            )
            exit(1)
        if not await checkTableEmpty(conn, schema, table_name):
            logger.error("Table '{}' is not empty".format(table_name))
            exit(1)
        else:
            logger.debug("Table '{}' is empty".format(table_name))
        return tuple_count
    except Exception as e:
        logger.error("Error: {}".format(str(e)))
        e.with_traceback()
        exit(1)


async def checkTableEmpty(conn, schema, table_name):
    async with conn.cursor() as cur:
        await cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {}.{} LIMIT 1").format(
                sql.Identifier(schema), sql.Identifier(table_name)
            )
        )
        count = await cur.fetchone()
        return count[0] == 0


async def getTableMeta(conn, schema, table_name):
    async with conn.cursor() as cur:
        await cur.execute(
            # This query will return 1 for the table itself, and one or more
            # for each index defined
            sql.SQL(
                """select count(*)
                    from pg_class c left outer join pg_index i on c.oid = i.indrelid, pg_namespace ns
                    where c.relname='{}' and c.relnamespace = ns.oid and ns.nspname='{}'""".format(
                    table_name, schema
                )
            )
        )
        count = (await cur.fetchone())[0]
        logger.debug("Table has {} indexes, including the table itself".format(count))
        return count


async def get_current_schema(conn):
    async with conn.cursor() as cur:
        await cur.execute("SELECT current_schema();")
        schema = await cur.fetchone()
        logger.debug("Current schema: {}".format(schema[0]))
        return schema[0]


async def loader(sfr):

    (conn, session_id) = await connectDB()
    cur = conn.cursor()

    backoff = int(args.backoff) / 1000

    retry = False
    keep = []
    rowcount = 0

    while True:
        if retry is False:
            batch = await sfr.get_n_tuples(args.batchsize)
        else:
            # Retry only once
            retry = False

        if len(batch) == 0:
            # Reached end of file
            break
        try:
            # Use the COPY command to load the batch into the table
            copycmd = "COPY {} {} FROM STDIN".format(
                "{}", "(" + args.cols + ")" if args.cols else ""
            )
            async with cur.copy(
                sql.SQL(copycmd).format(sql.Identifier(args.schema, args.tablename))
            ) as copy:
                # Check if we've reached the end of the file
                for record in batch:
                    keep = record
                    await copy.write_row(record)
        except psycopg.errors.UniqueViolation as ukv:
            logger.error("Unique key violation, load failed")
            logger.debug("Failed row: ")
            logger.debug(keep)
            conn.close()
            return (rowcount, False)
        except Exception as e:
            if args.relentless and (
                e.sqlstate == "XX000"
                or conn.closed
                or e == psycopg.errors.SerializationFailure
            ):
                logger.debug("Retryable failure at row: {}".format(keep))
                logger.debug(
                    "Thread {}: Exception: {}, PG error: {}, SQLSTATE: {}, session_id: {}".format(
                        str(threading.get_ident()),
                        e,
                        e.pgerror if hasattr(e, "pgerror") else "None",
                        e.sqlstate,
                        session_id,
                    )
                )

                await asyncio.sleep(backoff)

                if conn.closed:
                    # reconnect
                    isconn = False
                    while isconn is False:
                        logger.debug(
                            "Thread {}: Reconnecting".format(str(threading.get_ident()))
                        )
                        try:
                            (conn, session_id) = await connectDB()
                            isconn = True
                        except Exception as new_e:
                            logger.debug(
                                "Thread {}: Exception while reconnecting, will keep trying: {}".format(
                                    str(threading.get_ident()), new_e
                                )
                            )
                            continue
                    cur = conn.cursor()
                elif e == psycopg.errors.SerializationFailure or e.sqlstate == "XX000":
                    # Assume that the previous error:
                    #   (a) did not close the connection, and
                    #   (b) will eventually succeed, and
                    #   (c) needs an explicit rollback to proceed
                    # This caters for OCC conflicts and internal server errors
                    logger.debug("Last exception: {}".format(e))
                    logger.debug(
                        "Previous failure was not a connection error, rolling back"
                    )
                    await conn.rollback()
                else:
                    logger.error(
                        "Thread {}: Unhandled Exception, aborting: {}, PG error: {}, session_id: {}".format(
                            str(threading.get_ident()), e, e.pgerror, session_id
                        )
                    )
                    exit(1)
                retry = True
                continue
            else:
                logger.error(
                    "Thread {}: Load FAILED: Error: {}, SQLSTATE: {} PG Result: {}".format(
                        str(threading.get_ident()), e, e.sqlstate, e.pgresult
                    )
                )
                if not args.relentless and (
                    e.sqlstate == "XX000"
                    or conn.closed
                    or e == psycopg.errors.SerializationFailure
                ):
                    logger.info("Specifying the '--relentless' flag will probably help")
                await conn.close()
                return (rowcount, False)
        # Commit the changes
        try:
            await conn.commit()
            rowcount += len(batch)
            if args.feedback:
                sys.stdout.write(".")
                sys.stdout.flush()
        except Exception as e:
            logger.debug("Commit failure: {}".format(e))
            if args.relentless:
                await asyncio.sleep(backoff)
                logger.info("Retrying the commit batch")
                retry = True
            else:
                await conn.close()
                return (rowcount, False)

    # Close the connection
    await cur.close()
    await conn.close()
    return (rowcount, True)


def parse_args(in_args):
    global args

    # Create the argument parser
    parser = argparse.ArgumentParser()

    # Add the required arguments
    parser.add_argument(
        "--host",
        required=False,
        default=os.environ.get("PGHOST", None),
        help="PostgreSQL host",
    )
    parser.add_argument(
        "--database",
        required=False,
        default=os.environ.get("PGDATABASE", None),
        help="Database name",
    )
    parser.add_argument(
        "--user",
        required=False,
        default=os.environ.get("PGUSER", None),
        help="Database user",
    )
    pwgroup = parser.add_mutually_exclusive_group()
    pwgroup.add_argument(
        "--password",
        required=False,
        default=os.environ.get("PGPASSWORD", None),
        help="Database password",
    )
    parser.add_argument("--filename", required=True, help="Source data file")
    parser.add_argument("--tablename", required=True, help="Target table name")
    parser.add_argument(
        "--batchsize", required=False, default=1000, type=int, help="Commit batch size"
    )
    parser.add_argument(
        "--threads", required=False, default=1, help="Number of threads"
    )
    parser.add_argument(
        "--schema", required=False, default=None, help="Schema for specified table"
    )
    parser.add_argument(
        "--cols",
        required=False,
        help="Comma-separated list of column names to load into",
    )
    parser.add_argument(
        "--delim",
        required=False,
        default=" ",
        help="Field delimiter separating column values within each row",
    )
    parser.add_argument(
        "--backoff",
        required=False,
        default=1000,
        help="Backoff by <BACKOFF> ms between failed batches",
    )
    parser.add_argument(
        "--relentless",
        required=False,
        action="store_true",
        help="If connection errors or OCC errors occur, retry the batch relentlessly until it works",
    )
    parser.add_argument(
        "--feedback",
        required=False,
        action="store_true",
        help="Output single period for each full [batchsize] batch of rows loaded",
    )
    parser.add_argument(
        "--debug",
        required=False,
        action="store_true",
        help="Set logging level to debug",
    )

    # Parse the arguments
    args = parser.parse_args(in_args)


if __name__ == "__main__":
    asyncio.run(main())
