# Client
The Client library provides an ADO.NET client over the the top of results from the `ExecuteQuery` and `ExecuteUpdate` calls in the [AdbcStatement](https://github.com/apache/arrow-adbc/blob/main/csharp/src/Apache.Arrow.Adbc/AdbcStatement.cs) class.


## Library Design
The Client is designed to work with any driver that inherits from [AdbcDriver](https://github.com/apache/arrow-adbc/blob/main/csharp/src/Apache.Arrow.Adbc/AdbcDriver.cs), whether they are written in a .NET language or a C-compatible language that can be loaded via Interop.
The driver is injected at runtime during the creation of the `Client.AdbcConnection`, seen here:

![Dependency Injection Model](/docs/DependencyInjection.png "Dependency Injection Model")

This enables the client to work with multiple ADBC drivers in the same fashion. When a new client AdbcConnection is created, the driver is just passed in as part of the constructor, like:

```
new Client.AdbcConnection()
{
   new DriverA(),
   ...
}

new Client.AdbcConnection()
{
   new DriverB(),
   ...
}
```

Since ADO.NET is row-oriented, and ADBC is column-oriented, the row index is tracked through the `Read()` call. When the end of the current record batch is reached, the Client library calls the `ReadNextRecordBatchAsync` method on the QueryResult until no more record batches can be obtained.

This can be thought of as:

![Arrow to DbDataReader](/docs/Arrow-to-DbDataReader.png "Arrow to DbDataReader")
