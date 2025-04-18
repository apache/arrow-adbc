<!--

 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

-->

# BigQuery
The BigQuery tests are used to validate both the driver and the ADO.NET client library for use with the BigQuery ADBC driver. This includes the ability to read metadata, execute queries and updates, and validate the return types of the data.

## Setup
The environment variable `BIGQUERY_TEST_CONFIG_FILE` must be set to a configuration file for the tests to execute. If it is not, the tests will show as passed with an output message that they are skipped.

## Configuration

The following values can be setup in the configuration

- **testEnvironments** - List of test environments that are used when running tests. Tests can be configured that are not executed.
- **shared** - values that are shared across environments. These are referenced as `$ref:shared.<key>` in the environments.
- **environments** - Dictionary of the configured environments. The key must be listed in `testEnvironments` to execute during a test run.
  - **projectId** - Optional. The project ID of the default BigQuery project to query against.
  - **billingProjectId** - Optional. The billing project ID, also known as the quota project ID, that queries are billed to. Does not need to match the `projectId` value.
  - **clientId** - The client ID that is used during authentication.
  - **clientSecret** - Secret of the application used to generate the refresh token.
  - **refreshToken** - The refresh token obtained from Google used to authorize access to BigQuery.
  - **clientTimeout** - The timeout (in seconds) for the underlying BigQueryClient. Similar to a ConnectionTimeout.
  - **metadata**
    - **database** - Used by metadata tests for which database to target.
    - **schema** - Used by metadata tests for which schema to target.
    - **table** - Used by metadata tests for which table to target.
    - **expectedColumnCount** - Used by metadata tests to validate the number of columns that are returned.
  - **query** - The query to use.
  - **expectedResults** - The expected number of results from the query.
  - **includePublicProjectId** - True/False to indicate if the public projects should be included in the result set.
  - **scopes** - Comma separated list (string) of scopes applied during the test.
  - **queryTimeout** - The timeout (in seconds) for a query. Similar to a CommandTimeout.
  - **maxStreamCount** - The max stream count.
  - **statementType** - When executing multiple statements, limit the type of statement returned.
  - **statementIndex** - When executing multiple statements, specify the result of the statement to be returned.
  - **evaluationKind** - When executing multiple statements, limit the evaluation kind returned.
  - **includeTableConstraints** - Whether to include table constraints in the GetObjects query.
  - **largeResultsDestinationTable** - Sets the [DestinationTable](https://cloud.google.com/dotnet/docs/reference/Google.Cloud.BigQuery.V2/latest/Google.Cloud.BigQuery.V2.QueryOptions#Google_Cloud_BigQuery_V2_QueryOptions_DestinationTable) value of the QueryOptions if configured. Expects the format to be `{projectId}.{datasetId}.{tableId}` to set the corresponding values in the [TableReference](https://github.com/googleapis/google-api-dotnet-client/blob/6c415c73788b848711e47c6dd33c2f93c76faf97/Src/Generated/Google.Apis.Bigquery.v2/Google.Apis.Bigquery.v2.cs#L9348) class.
  - **allowLargeResults** - Whether to allow large results .
  - **runTimeoutTests** - indicates if the timeout tests should be run for this configuration. Default is false.

## Data
This project contains a SQL script to generate BigQuery data in the `resources/BigQueryData.sql` file. This can be used to populate a table in your BigQuery instance with data.

The tests included assume this script has been run.
