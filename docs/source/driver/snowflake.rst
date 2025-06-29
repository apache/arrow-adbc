.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..   http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

================
Snowflake Driver
================

**Available for:** C/C++, GLib/Ruby, Go, Python, R

The Snowflake Driver provides access to Snowflake Database Warehouses.

Installation
============

.. tab-set::

   .. tab-item:: C/C++
      :sync: cpp

      For conda-forge users:

      .. code-block:: shell

         mamba install libadbc-driver-snowflake

   .. tab-item:: Go
      :sync: go

      .. code-block:: shell

        go get github.com/apache/arrow-adbc/go/adbc/driver/snowflake

   .. tab-item:: Python
      :sync: python

      .. code-block:: shell

         # For conda-forge
         mamba install adbc-driver-snowflake

         # For pip
         pip install adbc_driver_snowflake

   .. tab-item:: R
      :sync: r

      .. code-block:: shell

         install.packages("adbcsnowflake", repos = "https://community.r-multiverse.org")

Usage
=====

To connect to a Snowflake database you can supply the "uri" parameter when
constructing the :c:struct:`AdbcDatabase`.

.. tab-set::

   .. tab-item:: C++
      :sync: cpp

      .. code-block:: cpp

         #include "arrow-adbc/adbc.h"

         // Ignoring error handling
         struct AdbcDatabase database;
         AdbcDatabaseNew(&database, nullptr);
         AdbcDatabaseSetOption(&database, "driver", "adbc_driver_snowflake", nullptr);
         AdbcDatabaseSetOption(&database, "uri", "<snowflake uri>", nullptr);
         AdbcDatabaseInit(&database, nullptr);

   .. tab-item:: Python
      :sync: python

      .. code-block:: python

         import adbc_driver_snowflake.dbapi

         with adbc_driver_snowflake.dbapi.connect("<snowflake uri>") as conn:
             pass

   .. tab-item:: R
      :sync: r

      .. code-block:: r

         library(adbcdrivermanager)

         # Use the driver manager to connect to a database
         uri <- Sys.getenv("ADBC_SNOWFLAKE_TEST_URI")
         db <- adbc_database_init(adbcsnowflake::adbcsnowflake(), uri = uri)
         con <- adbc_connection_init(db)

   .. tab-item:: Go
      :sync: go

      .. code-block:: go

         import (
            "context"

            "github.com/apache/arrow-adbc/go/adbc"
            "github.com/apache/arrow-adbc/go/adbc/driver/snowflake"
         )

         func main() {
            var drv snowflake.Driver
            db, err := drv.NewDatabase(map[string]string{
                adbc.OptionKeyURI: "<snowflake uri>",
            })
            if err != nil {
                // handle error
            }
            defer db.Close()

            cnxn, err := db.Open(context.Background())
            if err != nil {
                // handle error
            }
            defer cnxn.Close()
         }

URI Format
----------

The Snowflake URI should be of one of the following formats:

- ``user[:password]@account/database/schema[?param1=value1&paramN=valueN]``
- ``user[:password]@account/database[?param1=value1&paramN=valueN]``
- ``user[:password]@host:port/database/schema?account=user_account[&param1=value1&paramN=valueN]``
- ``host:port/database/schema?account=user_account[&param1=value1&paramN=valueN]``

Alternately, instead of providing a full URI, the configuration can
be entirely supplied using the other available options or some combination
of the URI and other options. If a URI is provided, it will be parsed first
and any explicit options provided will override anything parsed from the URI.

Supported Features
==================

The Snowflake driver generally supports features defined in the ADBC API
specification 1.0.0, as well as some additional, custom options.

Authentication
--------------

Snowflake requires some form of authentication to be enabled. By default
it will attempt to use Username/Password authentication. The username and
password can be provided in the URI or via the ``username`` and ``password``
options to the :c:struct:`AdbcDatabase`.

Alternately, other types of authentication can be specified and customized.
See "Client Options" below for details on all the options.

SSO Authentication
~~~~~~~~~~~~~~~~~~

Snowflake supports `single sign-on
<https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-overview>`_.
If your account has been configured with SSO, it can be used with the
Snowflake driver by setting the following options when constructing the
:c:struct:`AdbcDatabase`:

- ``adbc.snowflake.sql.account``: your Snowflake account.  (For example, if
  you log in to ``https://foobar.snowflakecomputing.com``, then your account
  identifier is ``foobar``.)
- ``adbc.snowflake.sql.auth_type``: ``auth_ext_browser``.
- ``username``: your username.  (This should probably be your email,
  e.g. ``jdoe@example.com``.)

A new browser tab or window should appear where you can continue the login.
Once this is complete, you will have a complete ADBC database/connection
object.  Some users have reported needing other configuration options, such as
``adbc.snowflake.sql.region`` and ``adbc.snowflake.sql.uri.*`` (see below for
a listing).

.. tab-set::

   .. tab-item:: Python
      :sync: python

      .. code-block:: python

         import adbc_driver_snowflake.dbapi
         # This will open a new browser tab, and block until you log in.
         adbc_driver_snowflake.dbapi.connect(db_kwargs={
             "adbc.snowflake.sql.account": "foobar",
             "adbc.snowflake.sql.auth_type": "auth_ext_browser",
             "username": "jdoe@example.com",
         })

   .. tab-item:: R
      :sync: r

      .. code-block:: r

         library(adbcdrivermanager)
         db <- adbc_database_init(
           adbcsnowflake::adbcsnowflake(),
           adbc.snowflake.sql.account = 'foobar',
           adbc.snowflake.sql.auth_type = 'auth_ext_browser'
           username = 'jdoe@example.com',
         )
         # This will open a new browser tab, and block until you log in.
         con <- adbc_connection_init(db)

   .. tab-item:: Go
      :sync: go

      .. code-block:: go

         import (
            "context"

            "github.com/apache/arrow-adbc/go/adbc"
            "github.com/apache/arrow-adbc/go/adbc/driver/snowflake"
         )

         func main() {
            var drv snowflake.Driver
            db, err := drv.NewDatabase(map[string]string{
                snowflake.OptionAccount: "foobar",
                snowflake.OptionAuthType: snowflake.OptionValueAuthExternalBrowser,
                adbc.OptionKeyUsername: "jdoe@example.com",
            })
            if err != nil {
                // handle error
            }
            defer db.Close()

            cnxn, err := db.Open(context.Background())
            if err != nil {
                // handle error
            }
            defer cnxn.Close()
         }



Bulk Ingestion
--------------

Bulk ingestion is supported. The mapping from Arrow types to Snowflake types
is provided below.

Bulk ingestion is implemented by writing Arrow data to Parquet file(s) and uploading (via PUT) to a temporary internal stage.
One or more COPY queries are executed in order to load the data into the target table.

In order for the driver to leverage this temporary stage, the user must have
the `CREATE STAGE <https://docs.snowflake.com/en/sql-reference/sql/create-stage>` privilege on the schema. In addition,
the current database and schema for the session must be set. If these are not set, the ``CREATE TEMPORARY STAGE`` command
executed by the driver can fail with the following error:

.. code-block:: sql

   CREATE TEMPORARY STAGE ADBC$BIND FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE BINARY_AS_TEXT = FALSE)
   CANNOT perform CREATE STAGE. This session does not have a current schema. Call 'USE SCHEMA' or use a qualified name.

The following informal benchmark demonstrates expected performance using default ingestion settings::

   Running on GCP e2-standard-4 (4 vCPU, 16GB RAM)
   Snowflake warehouse size M, same GCP region as Snowflake account
   Default ingestion settings

   TPC-H Lineitem (16 Columns):
      Scale Factor 1 (6M Rows): 9.5s
      Scale Factor 10 (60M Rows): 45s

The default settings for ingestion should be well balanced for many real-world configurations. If required, performance
and resource usage may be tuned with the following options on the :c:struct:`AdbcStatement` object:

``adbc.snowflake.statement.ingest_writer_concurrency``
    Number of Parquet files to write in parallel. Default attempts to maximize workers based on logical cores detected,
    but may need to be adjusted if running in a constrained environment. If set to 0, default value is used. Cannot be negative.

``adbc.snowflake.statement.ingest_upload_concurrency``
    Number of Parquet files to upload in parallel. Greater concurrency can smooth out TCP congestion and help make
    use of available network bandwidth, but will increase memory utilization. Default is 8. If set to 0, default value is used.
    Cannot be negative.

``adbc.snowflake.statement.ingest_copy_concurrency``
    Maximum number of COPY operations to run concurrently. Bulk ingestion performance is optimized by executing COPY
    queries as files are still being uploaded. Snowflake COPY speed scales with warehouse size, so smaller warehouses
    may benefit from setting this value higher to ensure long-running COPY queries do not block newly uploaded files
    from being loaded. Default is 4. If set to 0, only a single COPY query will be executed as part of ingestion,
    once all files have finished uploading. Cannot be negative.

``adbc.snowflake.statement.ingest_target_file_size``
    Approximate size of Parquet files written during ingestion. Actual size will be slightly larger, depending on
    size of footer/metadata. Default is 10 MB. If set to 0, file size has no limit. Cannot be negative.

Partitioned Result Sets
-----------------------

Partitioned result sets are not currently supported.

Performance
-----------

When querying Snowflake data, results are potentially fetched in parallel from multiple endpoints.
A limited number of batches are queued per endpoint, though data is always
returned to the client in the order of the endpoints.

To manage the performance of result fetching there are two options to control
buffering and concurrency behavior. These options are only available to be set
on the :c:struct:`AdbcStatement` object:

``adbc.rpc.result_queue_size``
    The number of batches to queue in the record reader. Defaults to 200.
    Must be an integer > 0.

``adbc.snowflake.rpc.prefetch_concurrency``
    The number of concurrent streams being fetched from snowflake at a time.
    Defaults to 10. Must be an integer > 0.

Transactions
------------

Transactions are supported. Keep in mind that Snowflake transactions will
implicitly commit if any DDL statements are run, such as ``CREATE TABLE``.

Client Options
--------------

The options used for creating a Snowflake Database connection can be customized.
These options map 1:1 with the Snowflake `Config object <https://pkg.go.dev/github.com/snowflakedb/gosnowflake#Config>`.

``adbc.snowflake.sql.db``
    The database this session should default to using.

``adbc.snowflake.sql.schema``
    The schema this session should default to using.

``adbc.snowflake.sql.warehouse``
    The warehouse this session should default to using.

``adbc.snowflake.sql.role``
    The role that should be used for authentication.

``adbc.snowflake.sql.region``
    The Snowflake region to use for constructing the connection URI.

``adbc.snowflake.sql.account``
    The Snowflake account that should be used for authentication and building the
    connection URI.

``adbc.snowflake.sql.uri.protocol``
    This should be either `http` or `https`.

``adbc.snowflake.sql.uri.port``
    The port to use for constructing the URI for connection.

``adbc.snowflake.sql.uri.host``
    The explicit host to use for constructing the URL to connect to.

``adbc.snowflake.sql.auth_type``
    Allows specifying alternate types of authentication, the allowed values are:

    - ``auth_snowflake``: General username/password authentication (this is the default)
    - ``auth_oauth``: Use OAuth authentication for the snowflake connection.
    - ``auth_ext_browser``: Use an external browser to access a FED and perform SSO auth.
    - ``auth_okta``: Use a native Okta URL to perform SSO authentication using Okta
    - ``auth_jwt``: Use a provided JWT to perform authentication.
    - ``auth_mfa``: Use a username and password with MFA.

``adbc.snowflake.sql.client_option.auth_token``
    If using OAuth or another form of authentication, this option is how you can
    explicitly specify the token to be used for connection.

``adbc.snowflake.sql.client_option.okta_url``
    If using ``auth_okta``, this option is required in order to specify the
    Okta URL to connect to for SSO authentication.

``adbc.snowflake.sql.client_option.login_timeout``
    Specify login retry timeout *excluding* network roundtrip and reading http responses.
    Value should be formatted as described `here <https://pkg.go.dev/time#ParseDuration>`,
    such as ``300ms``, ``1.5s`` or ``1m30s``. Even though negative values are accepted,
    the absolute value of such a duration will be used.

``adbc.snowflake.sql.client_option.request_timeout``
    Specify request retry timeout *excluding* network roundtrip and reading http responses.
    Value should be formatted as described `here <https://pkg.go.dev/time#ParseDuration>`,
    such as ``300ms``, ``1.5s`` or ``1m30s``. Even though negative values are accepted,
    the absolute value of such a duration will be used.

``adbc.snowflake.sql.client_option.jwt_expire_timeout``
    JWT expiration will occur after this timeout.
    Value should be formatted as described `here <https://pkg.go.dev/time#ParseDuration>`,
    such as ``300ms``, ``1.5s`` or ``1m30s``. Even though negative values are accepted,
    the absolute value of such a duration will be used.

``adbc.snowflake.sql.client_option.client_timeout``
    Specify timeout for network roundtrip and reading http responses.
    Value should be formatted as described `here <https://pkg.go.dev/time#ParseDuration>`,
    such as ``300ms``, ``1.5s`` or ``1m30s``. Even though negative values are accepted,
    the absolute value of such a duration will be used.

``adbc.snowflake.sql.client_option.app_name``
    Allows specifying the Application Name to Snowflake for the connection.

``adbc.snowflake.sql.client_option.tls_skip_verify``
    Disable verification of the server's TLS certificate. Value should be ``true``
    or ``false``.

``adbc.snowflake.sql.client_option.ocsp_fail_open_mode``
    Control the fail open mode for OCSP. Default is ``true``. Value should
    be either ``true`` or ``false``.

``adbc.snowflake.sql.client_option.keep_session_alive``
    Enable the session to persist even after the connection is closed. Value
    should be either ``true`` or ``false``.

``adbc.snowflake.sql.client_option.jwt_private_key``
    Specify the RSA private key which should be used to sign the JWT for
    authentication. This should be a path to a file containing a PKCS1
    private key to be read in and parsed. Commonly encoded in PEM blocks
    of type "RSA PRIVATE KEY".

``adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value``
    Parses an encrypted or unencrypted PKCS #8 private key without having to
    read it from the file system. If using encrypted, the
    ``adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_password`` value
    is required and used to decrypt.

``adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_password``
    Passcode to use when passing an encrypted PKCS #8 value.

``adbc.snowflake.sql.client_option.disable_telemetry``
    The Snowflake driver allows for telemetry information which can be
    disabled by setting this to ``true``. Value should be either ``true``
    or ``false``.

``adbc.snowflake.sql.client_option.config_file``
    Specifies the location of the client configuration JSON file. See the
    [Snowflake Go docs](https://github.com/snowflakedb/gosnowflake/blob/a26ac8a1b9a0dda854ac5db9c2c145f79d5ac4c0/doc.go#L130) for more details.

``adbc.snowflake.sql.client_option.tracing``
    Set the logging level

``adbc.snowflake.sql.client_option.cache_mfa_token``
    When ``true``, the MFA token is cached in the credential manager. Defaults
    to ``true`` on Windows/OSX, ``false`` on Linux.

``adbc.snowflake.sql.client_option.store_temp_creds``
    When ``true``, the ID token is cached in the credential manager. Defaults
    to ``true`` on Windows/OSX, ``false`` on Linux.

``adbc.snowflake.sql.client_option.use_high_precision``
    When ``true``, fixed-point snowflake columns with the type ``NUMBER``
    will be returned as ``Decimal128`` type Arrow columns using the precision
    and scale of the ``NUMBER`` type. When ``false``, ``NUMBER`` columns
    with a scale of 0 will be returned as ``Int64`` typed Arrow columns and
    non-zero scaled columns will be returned as ``Float64`` typed Arrow columns.
    The default is ``true``.

``adbc.snowflake.sql.client_option.max_timestamp_precision``
    Controls the behavior of Timestamp values with Nanosecond precision. Native Go behavior
    is these values will overflow to an unpredictable value when the year is before year 1677 or after 2262.
    This option can control the behavior of the `timestamp_ltz`, `timestamp_ntz`, and `timestamp_tz` types.
    Valid values are
    - ``nanoseconds``: Use default behavior for nanoseconds.
    - ``nanoseconds_error_on_overflow``: Throws an error when the value will overflow to enforce integrity of the data.
    - ``microseconds``: Limits the max Timestamp precision to microseconds, which is safe for all values.

Metadata
--------

When calling :c:func:`AdbcConnectionGetTableSchema`, the returned Arrow Schema
will contain metadata on each field:

``DATA_TYPE``
    This will be a string containing the raw Snowflake data type of this column

``PRIMARY_KEY``
    This will be either ``Y`` or ``N`` to indicate a column is a primary key.

In addition, the schema on the stream of results from a query will contain
the following metadata keys on each field:

``logicalType``
    The Snowflake logical type of this column. Will be one of ``fixed``,
    ``real``, ``text``, ``date``, ``variant``, ``timestamp_ltz``, ``timestamp_ntz``,
    ``timestamp_tz``, ``object``, ``array``, ``binary``, ``time``, ``boolean``.

``precision``
    An integer representing the Snowflake precision of the field.

``scale``
    An integer representing the Snowflake scale of the values in this field.

``charLength``
    If a text field, this will be equivalent to the ``VARCHAR(#)`` parameter ``#``.

``byteLength``
    Will contain the length, in bytes, of the raw data sent back from Snowflake
    regardless of the type of the field in Arrow.

Type Support
------------

Because Snowflake types do not necessary match up 1-to-1 with Arrow types
the following is what should be expected when requesting data. Any conversions
indicated are done to ensure consistency of the stream of record batches.

.. list-table::
   :header-rows: 1

   * - Snowflake Type
     - Arrow Type
     - Notes

   * - integral types
     - number(38, 0)
     - All integral types in Snowflake are stored as numbers for which neither
       precision nor scale can be specified.

   * - float/double
     - float64
     - Snowflake does not distinguish between float or double. Both are 64-bit values.

   * - decimal/numeric
     - numeric
     - Snowflake will respect the precision/scale of the Arrow type. See the
       ``adbc.snowflake.sql.client_option.use_high_precision`` for exceptions to this
       behavior.

   * - time
     - time64[ns]
     - For ingestion, time32 can also be used.

   * - date
     - date32
     - For ingestion, date64 can also be used.

   * - | timestamp_ltz
       | timestamp_ntz
       | timestamp_tz
     - timestamp[ns]
     - Local time zone will be used, except for timestamp_ntz which is not an instant.
       In this case no timezone will be present in the type. Physical values will be
       UTC-normalized.

   * - | variant
       | object
       | array
     - string
     - Snowflake does not provide information about nested
       types. Values will be strings in a format similar to JSON that
       can be parsed. The Arrow type will contain a metadata key
       ``logicalType`` with the Snowflake field type. Arrow Struct and
       Map types will be stored as objects when ingested. List types will
       be stored as arrays.

   * - | geography
       | geometry
     - string
     - There is no current canonical Arrow (extension) type for these
       types, so they will be returned as the string values that
       Snowflake provides.
