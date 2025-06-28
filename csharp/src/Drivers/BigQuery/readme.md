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
The BigQuery ADBC driver wraps a [BigQueryClient](https://cloud.google.com/dotnet/docs/reference/Google.Cloud.BigQuery.V2/latest/Google.Cloud.BigQuery.V2.BigQueryClient) object for working with [Google BigQuery](https://cloud.google.com/bigquery/) data.

# Supported Features

## Authentication

The ADBC driver supports both Service and User accounts for use with [BigQuery authentication](https://cloud.google.com/bigquery/docs/authentication/).

## Authorization

The ADBC driver passes the configured credentials to BigQuery, but you may need to ensure the credentials have proper [authorization](https://cloud.google.com/bigquery/docs/authorization/) to perform operations such as read and write.

## Parameters

The following parameters can be used to configure the driver behavior. The parameters are case sensitive.

**adbc.bigquery.access_token**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Sets the access token to use as the credential. Currently, this is for Microsoft Entra, but this could be used for other OAuth implementations as well.

**adbc.bigquery.audience_uri**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Sets the audience URI for the authentication token. Currently, this is for Microsoft Entra, but this could be used for other OAuth implementations as well.

**adbc.bigquery.allow_large_results**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Sets the [AllowLargeResults](https://cloud.google.com/dotnet/docs/reference/Google.Cloud.BigQuery.V2/latest/Google.Cloud.BigQuery.V2.QueryOptions#Google_Cloud_BigQuery_V2_QueryOptions_AllowLargeResults) value of the QueryOptions to `true` if configured; otherwise, the default is `false`.

**adbc.bigquery.auth_type**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Required. Must be `user`, `aad` (for Microsoft Entra) or `service`.

**adbc.bigquery.billing_project_id**<br>
&nbsp;&nbsp;&nbsp;&nbsp;The [Project ID](https://cloud.google.com/resource-manager/docs/creating-managing-projects) used for accessing billing BigQuery. If not specified, will default to the detected project ID.

**adbc.bigquery.client_id**<br>
&nbsp;&nbsp;&nbsp;&nbsp;The OAuth client ID. Required for `user` authentication.

**adbc.bigquery.client_secret**<br>
&nbsp;&nbsp;&nbsp;&nbsp;The OAuth client secret. Required for `user` authentication.

**adbc.bigquery.client.timeout**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Optional. Sets the timeout (in seconds) for the BigQueryClient. Similar to a ConnectionTimeout.

**adbc.bigquery.auth_json_credential**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Required if using `service` authentication. This value is passed to the [GoogleCredential.FromJson](https://cloud.google.com/dotnet/docs/reference/Google.Apis/latest/Google.Apis.Auth.OAuth2.GoogleCredential#Google_Apis_Auth_OAuth2_GoogleCredential_FromJson_System_String) method.

**adbc.bigquery.get_query_results_options.timeout**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Optional. Sets the timeout (in seconds) for the GetQueryResultsOptions value. If not set, defaults to 5 minutes. Similar to a CommandTimeout.

**adbc.bigquery.maximum_retries**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Optional. The maximum number of retries. Defaults to 5.

**adbc.bigquery.max_fetch_concurrency**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Optional. Sets the [maxStreamCount](https://cloud.google.com/dotnet/docs/reference/Google.Cloud.BigQuery.Storage.V1/latest/Google.Cloud.BigQuery.Storage.V1.BigQueryReadClient#Google_Cloud_BigQuery_Storage_V1_BigQueryReadClient_CreateReadSession_System_String_Google_Cloud_BigQuery_Storage_V1_ReadSession_System_Int32_Google_Api_Gax_Grpc_CallSettings_) for the CreateReadSession method. If not set, defaults to 1.

**adbc.bigquery.multiple_statement.statement_type**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Optional. When executing multiple statements, limit the type of statement returned. If not set, all types of statements are returned.

**adbc.bigquery.multiple_statement.statement_index**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Optional. When executing multiple statements, specify the result of the statement to be returned (Minimum value is 1). If not set, the result of the first statement is returned.

**adbc.bigquery.multiple_statement.evaluation_kind**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Optional. When executing multiple statements, limit the evaluation kind returned. If not set, all evaluation kinds are returned.

**adbc.bigquery.include_constraints_getobjects**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Optional. Some callers do not need the constraint details when they get the table information and can improve the speed of obtaining the results. Setting this value to `"false"` will not include the constraint details. The default value is `"true"`.

**adbc.bigquery.include_public_project_id**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Include the `bigquery-public-data` project ID with the list of project IDs.

**adbc.bigquery.large_results_dataset**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Optional. Sets the dataset ID to use for large results. The dataset needs to be in the same region as the data being queried. If no value is specified, the driver will attempt to use or create `_bqodbc_temp_tables`. A randomly generated table name will be used for the DestinationTable.

**adbc.bigquery.large_results_destination_table**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Optional. Sets the [DestinationTable](https://cloud.google.com/dotnet/docs/reference/Google.Cloud.BigQuery.V2/latest/Google.Cloud.BigQuery.V2.QueryOptions#Google_Cloud_BigQuery_V2_QueryOptions_DestinationTable) value of the QueryOptions if configured. Expects the format to be `{projectId}.{datasetId}.{tableId}` to set the corresponding values in the [TableReference](https://github.com/googleapis/google-api-dotnet-client/blob/6c415c73788b848711e47c6dd33c2f93c76faf97/Src/Generated/Google.Apis.Bigquery.v2/Google.Apis.Bigquery.v2.cs#L9348) class.

**adbc.bigquery.project_id**<br>
&nbsp;&nbsp;&nbsp;&nbsp;The [Project ID](https://cloud.google.com/resource-manager/docs/creating-managing-projects) used for accessing BigQuery. If not specified, will default to detect the projectIds the credentials have access to.

**adbc.bigquery.refresh_token**<br>
&nbsp;&nbsp;&nbsp;&nbsp;The refresh token used for when the generated OAuth token expires. Required for `user` authentication.

**adbc.bigquery.retry_delay_ms**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Optional The delay between retries. Defaults to 200ms. The retries could take up to `adbc.bigquery.maximum_retries` x `adbc.bigquery.retry_delay_ms` to complete.

**adbc.bigquery.scopes**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Optional. Comma separated list of scopes to include for the credential.

**adbc.bigquery.use_legacy_sql**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Sets the [UseLegacySql](https://cloud.google.com/dotnet/docs/reference/Google.Cloud.BigQuery.V2/latest/Google.Cloud.BigQuery.V2.QueryOptions#Google_Cloud_BigQuery_V2_QueryOptions_UseLegacySql) value of the QueryOptions to `true` if configured; otherwise, the default is `false`.


## Type Support

There are some limitations to both C# and the C# Arrow implementation that limit how [BigQuery data types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types) that can be represented by the ADBC driver. For example, the `BIGNUMERIC` type in BigQuery does not have a large value equivalent to C#.

The following table depicts how the BigQuery ADBC driver converts a BigQuery type to an Arrow type.

|  BigQuery Type   |      Arrow Type   | C# Type
|----------|:-------------:|
| BIGNUMERIC |    Decimal256    | string
| BOOL |    Boolean   | bool
| BYTES |    Binary   | byte[]
| DATE |    Date32   | DateTime
| DATETIME |    Timestamp   | DateTime
| FLOAT64 |    Double   | double
| GEOGRAPHY |    String   | string
| INT64 |    Int64   | long
| NUMERIC |    Decimal128   | SqlDecimal
| STRING |    String   | string
| STRUCT |    String+   | string
| TIME |Time64   | long
| TIMESTAMP |    Timestamp   | DateTimeOffset

+A JSON string

See [Arrow Schema Details](https://cloud.google.com/bigquery/docs/reference/storage/#arrow_schema_details) for how BigQuery handles Arrow types.

## Microsoft Entra
The driver supports authenticating with a [Microsoft Entra](https://learn.microsoft.com/en-us/entra/fundamentals/what-is-entra) ID. For long running operations, the Entra token may timeout if the operation takes longer than the Entra token's lifetime. The driver has the ability to perform token refreshes by subscribing to the `UpdateToken` delegate on the `BigQueryConnection`. In this scenario, the driver will attempt to perform an operation. If that operation fails due to an Unauthorized error, then the token will be refreshed via the `UpdateToken` delegate.

Sample code to refresh the token:

```
Dictionary<string,string> properties = ...;
BigQueryConnection connection = new BigQueryConnection(properties);
connection.UpdateToken = () => Task.Run(() =>
{
    connection.SetOption(BigQueryParameters.AccessToken, GetAccessToken());
});
```

In the sample above, when a new token is needed, the delegate is invoked and updates the `adbc.bigquery.access_token` parameter on the connection object.

## Default Project ID

If a `adbc.bigquery.project_id` is not specified, or if it equals `bigquery-public-data`, the driver will query for the first project ID that is associated with the credentials provided. This will be the project ID that is used to perform queries.

## Large Results

If a result set will contain large results, the `adbc.bigquery.allow_large_results` parameter should be set to `"true"`. If this value is set, a destination must be specified.
The caller can either explicitly specify the fully qualified name of the destination table using the `adbc.bigquery.large_results_destination_table` value, or they can specify
a dataset using the `adbc.bigquery.large_results_dataset` parameter.

Behavior:
- If a destination table is explicitly set, the driver will use that value.
- If only a dataset value is set, the driver will attempt to retrieve the dataset. If the dataset does not exist, the driver will attempt to
  create it. The default table expiration will be set to 1 day and a `created_by` label will be included with the driver name and version that created the dataset. For example `created_by : adbc_bigquery_driver_v_0_19_0_0`. A randomly generated name will be used for the table name.
- If a destination table and a dataset are not specified, the driver will attempt to use or create the `_bqodbc_temp_tables` dataset using the same defaults and label specified above. A randomly generated name will be used for the table name.

## Permissions

The ADBC driver uses the BigQuery Client APIs to communicate with BigQuery. The following actions are performed in the driver and require the calling user to have the specified permissions. For more details on the permissions, or what roles may already have the permissions required, please see the additional references section below.

|Action|Permissions Required
|:----------|:-------------|
|Create Dataset<sup>*+</sup>|bigquery.datasets.create|
|Create Query Job|bigquery.jobs.create|
|Create Read Session|bigquery.readsessions.create<br> bigquery.tables.getData|
|Execute Query|bigquery.jobs.create<br> bigquery.jobs.get<br> bigquery.jobs.list|
|Get Dataset<sup>*</sup>|bigquery.datasets.get|
|Get Job|bigquery.jobs.get|
|Get Query Results|bigquery.jobs.get|
|List Jobs|bigquery.jobs.list|
|Read Rows|bigquery.readsessions.getData|

<sup>
*Only for large result sets<br>
+If a specified dataset does not already exist.
</sup>
<br>
<br>

Some environments may also require:
- [VPC Service Controls](https://cloud.google.com/vpc-service-controls/docs/troubleshooting)
- [Service Usage Consumer](https://cloud.google.com/service-usage/docs/access-control#serviceusage.serviceUsageConsumer) permissions

**Additional References**:
- [BigQuery IAM roles and permissions | Google Cloud](https://cloud.google.com/bigquery/docs/access-control)
- [Running jobs programmatically | BigQuery | Google Cloud](https://cloud.google.com/bigquery/docs/running-jobs)
- [Create datasets | BigQuery | Google Cloud](https://cloud.google.com/bigquery/docs/datasets#required_permissions)
- [Use the BigQuery Storage Read API to read table data |  Google Cloud](https://cloud.google.com/bigquery/docs/reference/storage/#permissions)
