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

"""Low-level ADBC bindings for the BigQuery driver."""

import enum
import functools
import typing

import adbc_driver_manager

from ._version import __version__  # noqa:F401

__all__ = ["DatabaseOptions", "StatementOptions", "connect"]


class DatabaseOptions(enum.Enum):
    """Database options specific to the BigQuery driver."""

    #: Specify auth type to use for bigquery connection based on
    #: what is supported by the bigquery driver. Default is
    #: "auth_bigquery" (use AUTH_VALUE_* consts to specify desired
    #: authentication type).
    AUTH_TYPE = "adbc.bigquery.sql.auth_type"

    #: Specify the value for credentials
    #: It should be the path to the JSON credentials file if
    #: AUTH_TYPE is AUTH_VALUE_JSON_CREDENTIAL_FILE
    #:
    #: or, it should be the encoded JSON string if
    #: AUTH_TYPE is AUTH_VALUE_JSON_CREDENTIAL_STRING
    AUTH_CREDENTIALS = "adbc.bigquery.sql.auth.credentials"

    #: Specify the client ID, client secret and refresh_token to
    #: use for bigquery connection if AUTH_TYPE is
    #: AUTH_VALUE_USER_AUTHENTICATION
    AUTH_CLIENT_ID = "adbc.bigquery.sql.auth.client_id"
    AUTH_CLIENT_SECRET = "adbc.bigquery.sql.auth.client_secret"
    AUTH_REFRESH_TOKEN = "adbc.bigquery.sql.auth.refresh_token"

    #: Specify the location to use for bigquery connection.
    LOCATION = "adbc.bigquery.sql.location"

    #: Specify the project ID to use for bigquery connection.
    PROJECT_ID = "adbc.bigquery.sql.project_id"

    #: Specify the dataset ID to use for bigquery connection.
    DATASET_ID = "adbc.bigquery.sql.dataset_id"

    #: Specify the table ID to use for bigquery connection.
    TABLE_ID = "adbc.bigquery.sql.table_id"

    #: Use the default authentication method implemented in
    #: Google Cloud SDK
    AUTH_VALUE_BIGQUERY = "adbc.bigquery.sql.auth_type.auth_bigquery"

    #: Specify to use a JSON credentials file for authentication
    AUTH_VALUE_JSON_CREDENTIAL_FILE = "adbc.bigquery.sql.auth_type.json_credential_file"

    #: Specify to use a JSON credentials string for authentication
    AUTH_VALUE_JSON_CREDENTIAL_STRING = (
        "adbc.bigquery.sql.auth_type.json_credential_string"
    )

    #: Specify to use access token for authentication
    AUTH_VALUE_USER_AUTHENTICATION = "adbc.bigquery.sql.auth_type.user_authentication"


class StatementOptions(enum.Enum):
    """Statement options specific to the BigQuery driver."""

    #: The following options corresponds to the fields in bigquery.QueryConfig
    #: https://pkg.go.dev/cloud.google.com/go/bigquery#QueryConfig

    #: The destination table is the table into which the results
    #: of the query will be written.
    #: If this field is nil, a temporary table will be created.
    DESTINATION_TABLE = "adbc.bigquery.sql.query.destination_table"

    #: DEFAULT_PROJECT_ID and DEFAULT_DATASET_ID specify the dataset to use
    #: for unqualified table names in the query.
    #: If DEFAULT_PROJECT_ID is set, DEFAULT_DATASET_ID must also be set.
    DEFAULT_PROJECT_ID = "adbc.bigquery.sql.query.default_project_id"
    DEFAULT_DATASET_ID = "adbc.bigquery.sql.query.default_dataset_id"

    #: CREATE_DISPOSITION specifies the circumstances under which the
    #: destination table will be created.
    #: The default is `"CREATE_IF_NEEDED"`.
    #: The following values are supported:
    #: - "CREATE_IF_NEEDED": will create the table if it does not already exist
    #:   Tables are created atomically on successful completion of a job.
    #: - "CREATE_NEVER": ensures the table must already exist and will not be
    #:   automatically created.
    CREATE_DISPOSITION = "adbc.bigquery.sql.query.create_disposition"

    #: WRITE_DISPOSITION specifies how existing data in the destination
    #: table is treated.
    #: The default is `"WRITE_EMPTY"`.
    #: The following values are supported:
    #: - "WRITE_APPEND": will append to any existing data in the destination
    #:   table.
    #:   Data is appended atomically on successful completion of a job.
    #: - "WRITE_TRUNCATE": overrides the existing data in the destination table
    #:   Data is overwritten atomically on successful completion of a job.
    #: - "WRITE_EMPTY": fails writes if the destination table already contains
    #:   data.
    WRITE_DISPOSITION = "adbc.bigquery.sql.query.write_disposition"

    #: DISABLE_QUERY_CACHE prevents results being fetched from the query cache.
    #: If this field is false, results are fetched from the cache if they are
    #: available.
    #: The query cache is a best-effort cache that is flushed whenever tables
    #: in the query are modified.
    #: Cached results are only available when TableID is unspecified in the
    #: query's destination Table.
    #: For more information, see
    #: https://cloud.google.com/bigquery/querying-data#querycaching
    DISABLE_QUERY_CACHE = "adbc.bigquery.sql.query.disable_query_cache"

    #: DISABLE_FLATTEN_RESULTS prevents results being flattened.
    #: If this field is false, results from nested and repeated fields are
    #: flattened.
    #: DISABLE_FLATTEN_RESULTS implies ALLOW_LARGE_RESULTS
    #: For more information, see
    #: https://cloud.google.com/bigquery/docs/data#nested
    DISABLE_FLATTEN_RESULTS = "adbc.bigquery.sql.query.disable_flatten_results"

    #: ALLOW_LARGE_RESULTS allows the query to produce arbitrarily large
    #: result tables.
    #: The destination must be a table.
    #: When using this option, queries will take longer to execute, even if
    #: the result set is small.
    #: For additional limitations, see
    #: https://cloud.google.com/bigquery/querying-data#largequeryresults
    ALLOW_LARGE_RESULTS = "adbc.bigquery.sql.query.allow_large_results"

    #: PRIORITY specifies the priority with which to schedule the query.
    #: The default priority is `"INTERACTIVE"`.
    #: For more information, see
    #: https://cloud.google.com/bigquery/querying-data#batchqueries
    #:
    #: The following values are supported:
    #: - "BATCH": BatchPriority specifies that the query should be scheduled
    #:   with the batch priority.  BigQuery queues each batch query on your
    #:   behalf, and starts the query as soon as idle resources are available,
    #:   usually within a few minutes. If BigQuery hasn't started the query
    #:   within 24 hours, BigQuery changes the job priority to interactive.
    #:   Batch queries don't count towards your concurrent rate limit, which
    #:   can make it easier to start many queries at once.
    #:
    #:   More information can be found at
    #:   https://cloud.google.com/bigquery/docs/running-queries#batchqueries.
    #:
    #: - "INTERACTIVE": specifies that the query should be scheduled with
    #:   interactive priority, which means that the query is executed as soon
    #:   as possible. Interactive queries count towards your concurrent rate
    #:   limit and your daily limit. It is the default priority with which
    #:   queries get executed.
    #:
    #:   More information can be found at
    #:   https://cloud.google.com/bigquery/docs/running-queries#queries.
    PRIORITY = "adbc.bigquery.sql.query.priority"

    #: MAX_BILLING_TIER sets the maximum billing tier for a Query.
    #: Queries that have resource usage beyond this tier will fail (without
    #: incurring a charge). If this field is zero, the project default will be
    #: used.
    MAX_BILLING_TIER = "adbc.bigquery.sql.query.max_billing_tier"

    #: MAX_BYTES_BILLED limits the number of bytes billed for
    #: this job.  Queries that would exceed this limit will fail (without
    #: incurring a charge).
    #: If this field is less than 1, the project default will be used.
    MAX_BYTES_BILLED = "adbc.bigquery.sql.query.max_bytes_billed"

    #: USE_LEGACY_SQL causes the query to use legacy SQL.
    USE_LEGACY_SQL = "adbc.bigquery.sql.query.use_legacy_sql"

    #: If true, don't actually run this job. A valid query will return a mostly
    #: empty response with some processing statistics, while an invalid query
    #: will return the same error it would if it wasn't a dry run.
    DRY_RUN = "adbc.bigquery.sql.query.dry_run"

    #: CreateSession will trigger creation of a new session when true.
    CREATE_SESSION = "adbc.bigquery.sql.query.create_session"

    #: Sets a best-effort deadline on a specific job.  If job execution exceeds
    #: this timeout, BigQuery may attempt to cancel this work automatically.
    #:
    #: This deadline cannot be adjusted or removed once the job is created.
    JOB_TIMEOUT = "adbc.bigquery.sql.query.job_timeout"


def connect(
    db_kwargs: typing.Optional[typing.Dict[str, str]] = None,
) -> adbc_driver_manager.AdbcDatabase:
    """
    Create a low level ADBC connection to BigQuery.

    Parameters
    ----------
    db_kwargs : dict, optional
        Initial database connection parameters.
    """
    kwargs = (db_kwargs or {}).copy()
    return adbc_driver_manager.AdbcDatabase(driver=_driver_path(), **kwargs)


@functools.lru_cache
def _driver_path() -> str:
    import pathlib
    import sys

    import importlib_resources

    driver = "adbc_driver_bigquery"

    # Wheels bundle the shared library
    root = importlib_resources.files(driver)
    # The filename is always the same regardless of platform
    entrypoint = root.joinpath(f"lib{driver}.so")
    if entrypoint.is_file():
        return str(entrypoint)

    # Search sys.prefix + '/lib' (Unix, Conda on Unix)
    root = pathlib.Path(sys.prefix)
    for filename in (f"lib{driver}.so", f"lib{driver}.dylib"):
        entrypoint = root.joinpath("lib", filename)
        if entrypoint.is_file():
            return str(entrypoint)

    # Conda on Windows
    entrypoint = root.joinpath("bin", f"{driver}.dll")
    if entrypoint.is_file():
        return str(entrypoint)

    # Let the driver manager fall back to (DY)LD_LIBRARY_PATH/PATH
    # (It will insert 'lib', 'so', etc. as needed)
    return driver
