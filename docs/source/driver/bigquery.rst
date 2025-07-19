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

=======================
BigQuery (Go) Driver
=======================

.. contents::
  :depth: 2
  :local:

Overview
========

The BigQuery Go driver for ADBC allows Go applications to connect to Google BigQuery and interact with data.

Connection Options
==================

The following options can be set on the `AdbcDatabase` or `AdbcConnection` to configure the BigQuery driver:

.. list-table:: Connection Options
   :header-rows: 1

   * - Option
     - Description
     - Type
     - Default

   * - ``adbc.bigquery.sql.project_id``
     - The Google Cloud Project ID to connect to (analogous to the catalog).
     - string
     - None

   * - ``adbc.bigquery.sql.dataset_id``
     - The Google BigQuery Dataset ID to use (analogous to the default schema).
     - string
     - None

   * - ``adbc.bigquery.sql.query.use_legacy_sql``
     - Whether to use legacy SQL for queries. If false, standard SQL is used.
     - boolean
     - ``false``

   * - ``adbc.bigquery.sql.query.result_buffer_size``
     - The size of the buffer for query results in records.
     - int
     - 1024

   * - ``adbc.bigquery.sql.query.prefetch_concurrency``
     - The number of concurrent streams to use for prefetching query results.
     - int
     - 1

Authentication Options
----------------------

The ``adbc.bigquery.sql.auth_type`` option controls the authentication method. The following values are supported:

.. list-table:: Authentication Methods
   :header-rows: 1

   * - Value
     - Description

   * - ``app_default_credentials``
     - Authenticate using `Application Default Credentials <https://cloud.google.com/docs/authentication/production#adc>`_.

   * - ``json_credential_file``
     - Authenticate using a JSON credentials file provided via ``adbc.bigquery.sql.auth_credentials``.

   * - ``json_credential_string``
     - Authenticate using a JSON credentials string provided via ``adbc.bigquery.sql.auth_credentials``.

   * - ``user_authentication``
     - Authenticate using user OAuth 2.0 credentials, requiring ``adbc.bigquery.sql.auth.client_id``, ``adbc.bigquery.sql.auth.client_secret``, and ``adbc.bigquery.sql.auth.refresh_token``.

Service Account Impersonation
=============================

Service account impersonation is a credential transformation mechanism that works with **any** authentication type. It allows you to impersonate a target service account using your base credentials.

To use service account impersonation, configure your base authentication method (any of the above) and add the following impersonation options:

.. list-table:: Impersonation Options
   :header-rows: 1

   * - Option
     - Description
     - Type
     - Required

   * - ``adbc.google.bigquery.impersonate.target_principal``
     - The email address of the service account to impersonate.
     - string
     - Yes

   * - ``adbc.google.bigquery.impersonate.delegates``
     - A comma-separated list of service account emails in the delegation chain.
     - string (comma-separated)
     - No

   * - ``adbc.google.bigquery.impersonate.scopes``
     - A comma-separated list of OAuth 2.0 scopes to request during impersonation.
     - string (comma-separated)
     - No

   * - ``adbc.google.bigquery.impersonate.lifetime``
     - The desired lifetime of the impersonated token in seconds (e.g., "3600s"). Maximum one hour by default, up to 12 hours with appropriate org policy.
     - string (duration)
     - No

Examples
--------

Impersonation with Application Default Credentials
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: go

   import (
       "context"
       "fmt"
       "log"

       "github.com/apache/arrow-adbc/go/adbc"
       driver "github.com/apache/arrow-adbc/go/adbc/driver/bigquery"
   )

   func main() {
       db, err := adbc.NewDatabase(driver.NewDriver(nil), map[string]string{
           "adbc.bigquery.sql.project_id":                      "your-gcp-project-id",
           "adbc.bigquery.sql.auth_type":                       "app_default_credentials",
           "adbc.google.bigquery.impersonate.target_principal": "target-service@your-gcp-project-id.iam.gserviceaccount.com",
           "adbc.google.bigquery.impersonate.scopes":           "https://www.googleapis.com/auth/cloud-platform",
           "adbc.google.bigquery.impersonate.lifetime":         "3600s",
       })
       if err != nil {
           log.Fatal(err)
       }
       defer db.Close()

       cnxn, err := db.Open(context.Background())
       if err != nil {
           log.Fatal(err)
       }
       defer cnxn.Close()

       fmt.Println("Successfully connected to BigQuery using ADC + impersonation.")
   }

Impersonation with Service Account Key
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: go

   db, err := adbc.NewDatabase(driver.NewDriver(nil), map[string]string{
       "adbc.bigquery.sql.project_id":                      "your-gcp-project-id",
       "adbc.bigquery.sql.auth_type":                       "json_credential_file",
       "adbc.bigquery.sql.auth_credentials":                "/path/to/service-account-key.json",
       "adbc.google.bigquery.impersonate.target_principal": "target-service@your-gcp-project-id.iam.gserviceaccount.com",
       "adbc.google.bigquery.impersonate.scopes":           "https://www.googleapis.com/auth/cloud-platform",
   })

Impersonation with User Authentication
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: go

   db, err := adbc.NewDatabase(driver.NewDriver(nil), map[string]string{
       "adbc.bigquery.sql.project_id":                      "your-gcp-project-id",
       "adbc.bigquery.sql.auth_type":                       "user_authentication",
       "adbc.bigquery.sql.auth.client_id":                  "your-client-id",
       "adbc.bigquery.sql.auth.client_secret":              "your-client-secret",
       "adbc.bigquery.sql.auth.refresh_token":              "your-refresh-token",
       "adbc.google.bigquery.impersonate.target_principal": "target-service@your-gcp-project-id.iam.gserviceaccount.com",
       "adbc.google.bigquery.impersonate.scopes":           "https://www.googleapis.com/auth/cloud-platform",
   })

Impersonation with Delegation Chain
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: go

   db, err := adbc.NewDatabase(driver.NewDriver(nil), map[string]string{
       "adbc.bigquery.sql.project_id":                      "your-gcp-project-id",
       "adbc.bigquery.sql.auth_type":                       "app_default_credentials",
       "adbc.google.bigquery.impersonate.target_principal": "target-service@your-gcp-project-id.iam.gserviceaccount.com",
       "adbc.google.bigquery.impersonate.delegates":        "delegate1@your-gcp-project-id.iam.gserviceaccount.com,delegate2@your-gcp-project-id.iam.gserviceaccount.com",
       "adbc.google.bigquery.impersonate.scopes":           "https://www.googleapis.com/auth/cloud-platform",
       "adbc.google.bigquery.impersonate.lifetime":         "7200s", // 2 hours
   })

Configuration Requirements
~~~~~~~~~~~~~~~~~~~~~~~~~~

To use service account impersonation, ensure that:

1. **Base Authentication**: Your base service account has proper authentication configured
2. **IAM Permissions**: The base service account has the ``Service Account Token Creator`` role on the target service account
3. **Delegation Chain**: If using delegation, each service account in the chain must have the ``Service Account Token Creator`` role on the next service account in the chain
4. **Scopes**: The requested scopes are appropriate for your use case (e.g., ``https://www.googleapis.com/auth/bigquery`` for BigQuery access)

Error Handling
~~~~~~~~~~~~~~

Common impersonation errors and their solutions:

- **403 Forbidden**: Check that the base service account has the ``Service Account Token Creator`` role on the target service account
- **400 Bad Request**: Verify that the target principal email is correctly formatted
- **401 Unauthorized**: Ensure the base authentication is working correctly
- **Invalid lifetime**: Check that the lifetime is within allowed limits (1 hour default, up to 12 hours with org policy)
