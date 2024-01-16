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

# Client
The Client library provides an ADO.NET client over the the top of results from the `ExecuteQuery` and `ExecuteUpdate` calls in the [AdbcStatement](https://github.com/apache/arrow-adbc/blob/main/csharp/src/Apache.Arrow.Adbc/AdbcStatement.cs) class.


## Library Design
The Client is designed to work with any driver that inherits from [AdbcDriver](https://github.com/apache/arrow-adbc/blob/main/csharp/src/Apache.Arrow.Adbc/AdbcDriver.cs), whether they are written in a .NET language or a C-compatible language that can be loaded via Interop.
The driver is injected at runtime during the creation of the `Adbc.Client.AdbcConnection`, seen here:

![Dependency Injection Model](/docs/DependencyInjection.png "Dependency Injection Model")

This enables the client to work with multiple ADBC drivers in the same fashion. When a new client AdbcConnection is created, the driver is just passed in as part of the constructor, like:

```
new Adbc.Client.AdbcConnection()
{
   new DriverA(),
   ...
}

new Adbc.Client.AdbcConnection()
{
   new DriverB(),
   ...
}
```

Since ADO.NET is row-oriented, and ADBC is column-oriented, the row index is tracked through the `Read()` call. When the end of the current record batch is reached, the Client library calls the `ReadNextRecordBatchAsync` method on the QueryResult until no more record batches can be obtained.

This can be thought of as:

![Arrow to DbDataReader](/docs/Arrow-to-DbDataReader.png "Arrow to DbDataReader")

## Connection Properties
The ADO.NET Client is designed so that properties in the connection string map to the properties that are sent to the ADBC driver in the code:

```
AdbcDriver.Open(adbcConnectionParameters);
```

The connection string is parsed using the `DbConnectionStringBuilder` class and the key/value pairs are then added to the properties for the ADBC driver.

For example, when using the [Snowflake ADBC Go Driver](https://arrow.apache.org/adbc/main/driver/snowflake.html#client-options), the connection string will look similar to:

```
 adbc.snowflake.sql.account={account};adbc.snowflake.sql.warehouse={warehouse};username={user};password={password}
```

if using the default user name and password authentication, but look like

```
 adbc.snowflake.sql.account={account};adbc.snowflake.sql.warehouse={warehouse};username={user};password={password};adbc.snowflake.sql.auth_type=snowflake_jwt;adbc.snowflake.sql.client_option.jwt_private_key={private_key_file}
```

when using JWT authentication with an unencrypted key file.

Other ADBC drivers will have different connection parameters, so be sure to check the documentation for each driver.

# Query parsing and multiple queries
ADBC doesn't have very good support for [multiple result sets](https://github.com/apache/arrow-adbc/issues/1358). The `AdbcCommand` supports parsing queries into multiple queries and executing the queries individually.  In this case, the `ExecuteReader` call will return the first result set and the total number of `RecordsAffected` for any other type of calls.

## QueryConfiguration
The default `QueryConfiguration` uses a regular expression from a set of keywords to parse the individual queries. By default, it contains a limited set of keywords that map to several different data backends and the expected return types:

```
CreateKeyword = new KeywordDefinition("CREATE", QueryReturnType.RecordsAffected);
SelectKeyword = new KeywordDefinition("SELECT", QueryReturnType.RecordSet);
UpdateKeyword = new KeywordDefinition("UPDATE", QueryReturnType.RecordsAffected);
DeleteKeyword = new KeywordDefinition("DELETE", QueryReturnType.RecordsAffected);
DropKeyword   = new KeywordDefinition("DROP", QueryReturnType.RecordsAffected);
InsertKeyword = new KeywordDefinition("INSERT", QueryReturnType.RecordsAffected);
```

These may not be an exhaustive list for individual data backends. Callers can specify additional `KeywordDefinition` values and add them to the `Keywords` collection to include additional keywords that can be parsed using the default behavior.

Additionally, `QueryConfiguration` contains a `QueryFunctionDefinition` property called `CustomParser` that can be used to define how multi-statements should be parsed. If a `CustomParser` is defined, it overrides the default behavior and the results are returned from the `CustomParser` instead.
