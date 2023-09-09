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

# Data Types

There are some limitations to both C# and the C# Arrow implementation that limit how [BigQuery data types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types) that can be represented by the ADBC driver. For example, the `BIGNUMERIC` type in BigQuery does not have a large value equivalent to C#. Also, the C# Arrow does library does not have a [Map](https://arrow.apache.org/docs/python/generated/pyarrow.map_.html#pyarrow.map_) implementation.

The following table depicts how the BigQuery ADBC driver converts a BigQuery type to an Arrow type.

|  BigQuery Type   |      Arrow Type   |
|----------|:-------------:|
| BIGNUMERIC |    StringType   |
| BOOL |    BooleanType   |
| BYTES |    BinaryType   |
| DATE |    Date64Type   |
| DATETIME |    TimestampType   |
| FLOAT64 |    DoubleType   |
| GEOGRAPHY |    StringType   |
| INT64 |    Int64Type   |
| NUMERIC |    Decimal128Type   |
| STRING |    StringType   |
| STRUCT |    StringType*   |
| TIME |Time64Type   |
| TIMESTAMP |    TimestampType   |

*A JSON string
