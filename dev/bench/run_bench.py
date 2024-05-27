# #!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import time

import psutil

import adbc_driver_snowflake.dbapi

# import pyodbc
# import snowflake.connector


process = psutil.Process()

SAMPLE_RATE = 10  # record data every SAMPLE_RATE execution

can_draw = True
try:
    import matplotlib.pyplot as plt
except ImportError:
    print("graphs cannot be drawn as matplotlib is not installed")
    can_draw = False


def task_execution_decorator(func, perf_file, memory_file):
    count = 0

    def wrapper(*args, **kwargs):
        start = time.time()
        func(*args, **kwargs)
        memory_usage = (
            process.memory_info().rss / 1024 / 1024
        )  # rss is in bytes, we convert to MB
        period = time.time() - start
        nonlocal count
        if count % SAMPLE_RATE == 0:
            perf_file.write(str(period) + "\n")
            print(f"execution {count}")
            print(f"memory usage: {memory_usage} MB")
            print(f"execution time: {period} s")
            memory_file.write(str(memory_usage) + "\n")
        count += 1

    return wrapper


def task_fetch_arrow_batches(cursor, table_name, row_count_limit=50000):
    ret = cursor.execute(
        f"select * from {table_name} limit {row_count_limit}"
    ).fetch_arrow_batches()  # interface for snowflake-python-connector
    for _ in ret:
        pass


def task_fetch_record_batch(cursor, table_name, row_count_limit=50000):
    cursor.execute(f"select * from {table_name} limit {row_count_limit}")
    ret = cursor.fetch_record_batch()  # interface we provide for dbapi
    for _ in ret:
        pass


def task_fetch_odbc(cursor, table_name, row_count_limit=50000):
    cursor.execute(f"select * from {table_name} limit {row_count_limit}")
    batch_size = row_count_limit / 1000
    while True:
        rows = cursor.fetchmany(batch_size)
        if len(rows) == 0:
            break


def execute_task(task, cursor, table_name, iteration_cnt, row_count_limit=50000):
    for _ in range(iteration_cnt):
        task(cursor, table_name, row_count_limit)


ADBC_CONNECTION_PARAMETERS = {
    "user": "...",
    "password": "...",
    "adbc.snowflake.sql.account": "...",
}
PYODBC_CONNECTION_STR = "DSN=..."
CONNECTION_PARAMETERS = {
    "user": "...",
    "password": "...",
    "account": "...",
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--iteration_cnt",
        type=int,
        default=50,
        help="how many times to run the test function, default is 5000",
    )
    parser.add_argument(
        "--row_count",
        type=int,
        default=10000,
        help="how many rows of data to fetch",
    )
    parser.add_argument(
        "--test_table_name",
        type=str,
        default="SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.LINEITEM",
        hep="an existing test table that has data prepared",
    )
    args = parser.parse_args()

    test_table_name = args.test_table_name
    perf_record_file = "stress_perf_record"
    memory_record_file = "stress_memory_record"

    # with pyodbc.connect(PYODBC_CONNECTION_STR) as conn, conn.cursor() as cursor:
    # with snowflake.connector.connect(
    #  **CONNECTION_PARAMETERS
    # ) as conn, conn.cursor() as cursor:
    with adbc_driver_snowflake.dbapi.connect(
        **ADBC_CONNECTION_PARAMETERS
    ) as conn, conn.cursor() as cursor:
        cursor.adbc.statement.set_options(
            **{
                "adbc.snowflake.rpc.prefetch_concurrency": 4,
                "adbc.rpc.result_queue_size": 100,
            }
        )

        with open(perf_record_file, "w") as perf_file, open(
            memory_record_file, "w"
        ) as memory_file:
            # task = task_execution_decorator( # snowflake python connector
            #  task_fetch_arrow_batches, perf_file, memory_file)
            # task = task_execution_decorator( # pyodbc
            #  task_fetch_odbc, perf_file, memory_file)
            task = task_execution_decorator(
                task_fetch_record_batch, perf_file, memory_file
            )
            execute_task(
                task, cursor, test_table_name, args.iteration_cnt, args.row_count
            )

        if can_draw:
            with open(perf_record_file) as perf_file, open(
                memory_record_file
            ) as memory_file:
                # sample rate
                perf_lines = perf_file.readlines()
                perf_records = [float(line) for line in perf_lines]

                memory_lines = memory_file.readlines()
                memory_records = [float(line) for line in memory_lines]

                plt.plot([i for i in range(len(perf_records))], perf_records)
                plt.title("per iteration execution time")
                plt.show(block=False)
                plt.figure()
                plt.plot([i for i in range(len(memory_records))], memory_records)
                plt.title("memory usage")
                plt.show(block=True)
