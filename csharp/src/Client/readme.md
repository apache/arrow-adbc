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

### Connection Keywords
Because the ADO.NET client is designed to work with multiple drivers, callers will need to specify the driver properties that are set for particular values. This can be done either as properties on the objects directly, or can be parsed from the connection string.
These properties are:

- __AdbcConnectionTimeout__ - This specifies the connection timeout value. The value needs to be in the form (driver.property.name, integer, unit) where the unit is one of `s` or `ms`, For example, `AdbcConnectionTimeout=(adbc.snowflake.sql.client_option.client_timeout,30,s)` would set the connection timeout to 30 seconds.
- __AdbcCommandTimeout__ - This specifies the command timeout value. This follows the same pattern as `AdbcConnectionTimeout` and sets the `AdbcCommandTimeoutProperty` and `CommandTimeout` values on the `AdbcCommand` object.
- __StructBehavior__ - This specifies the StructBehavior when working with Arrow Struct arrays. The valid values are `JsonString` (the default) or `Strict` (treat the struct as a native type).
- __DecimalBehavior__ - This specifies the DecimalBehavior when parsing decimal values from Arrow libraries. The valid values are `UseSqlDecimal` or `OverflowDecimalAsString` where values like Decimal256 are treated as strings.
