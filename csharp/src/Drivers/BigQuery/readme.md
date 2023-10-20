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

**adbc.bigquery.allow_large_results**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Sets the [AllowLargeResults](https://cloud.google.com/dotnet/docs/reference/Google.Cloud.BigQuery.V2/latest/Google.Cloud.BigQuery.V2.QueryOptions#Google_Cloud_BigQuery_V2_QueryOptions_AllowLargeResults) value of the QueryOptions to `true` if configured; otherwise, the default is `false`.

**adbc.bigquery.auth_type**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Required. Must be `user` or `service`

https://cloud.google.com/dotnet/docs/reference/Google.Cloud.BigQuery.V2/latest/Google.Cloud.BigQuery.V2.QueryOptions#Google_Cloud_BigQuery_V2_QueryOptions_AllowLargeResults

**adbc.bigquery.client_id**<br>
&nbsp;&nbsp;&nbsp;&nbsp;The OAuth client ID. Required for `user` authentication.

**adbc.bigquery.client_secret**<br>
&nbsp;&nbsp;&nbsp;&nbsp;The OAuth client secret. Required for `user` authentication.

**adbc.bigquery.auth_json_credential**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Required if using `service` authentication. This value is passed to the [GoogleCredential.FromJson](https://cloud.google.com/dotnet/docs/reference/Google.Apis/latest/Google.Apis.Auth.OAuth2.GoogleCredential#Google_Apis_Auth_OAuth2_GoogleCredential_FromJson_System_String) method.

**adbc.bigquery.project_id**<br>
&nbsp;&nbsp;&nbsp;&nbsp;The [Project ID](https://cloud.google.com/resource-manager/docs/creating-managing-projects) used for accessing BigQuery.

**adbc.bigquery.refresh_token**<br>
&nbsp;&nbsp;&nbsp;&nbsp;The refresh token used for when the generated OAuth token expires. Required for `user` authentication.

**adbc.bigquery.use_legacy_sql**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Sets the [UseLegacySql](https://cloud.google.com/dotnet/docs/reference/Google.Cloud.BigQuery.V2/latest/Google.Cloud.BigQuery.V2.QueryOptions#Google_Cloud_BigQuery_V2_QueryOptions_UseLegacySql) value of the QueryOptions to `true` if configured; otherwise, the default is `false`.


## Type Support

There are some limitations to both C# and the C# Arrow implementation that limit how [BigQuery data types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types) that can be represented by the ADBC driver. For example, the `BIGNUMERIC` type in BigQuery does not have a large value equivalent to C#.

The following table depicts how the BigQuery ADBC driver converts a BigQuery type to an Arrow type.

|  BigQuery Type   |      Arrow Type   | C# Type
|----------|:-------------:|
| BIGNUMERIC |    Decimal256    | decimal / string*
| BOOL |    Boolean   | bool
| BYTES |    Binary   | byte[]
| DATE |    Date64   | DateTime
| DATETIME |    Timestamp   | DateTime
| FLOAT64 |    Double   | double
| GEOGRAPHY |    String   | string
| INT64 |    Int64   | long
| NUMERIC |    Decimal128   | decimal / string*
| STRING |    String   | string
| STRUCT |    String+   | string
| TIME |Time64   | long
| TIMESTAMP |    Timestamp   | DateTimeOffset

*An attempt is made to parse the original value as a `decimal` in C#. If that fails, the driver attempts to parse the overflow exception and return the original value.

+A JSON string

See [Arrow Schema Details](https://cloud.google.com/bigquery/docs/reference/storage/#arrow_schema_details) for how BigQuery handles Arrow types.
