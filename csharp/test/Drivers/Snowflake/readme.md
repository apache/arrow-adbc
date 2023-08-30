# Snowflake
The Snowflake tests leverage the interop nature of the C# ADBC library. These require the use of the [Snowflake Go driver](https://github.com/apache/arrow-adbc/tree/main/go/adbc/driver/snowflake). You will need to compile the Go driver for your platform and place the driver in the correct path in order for the tests to execute correctly.

To compile, navigate to the `go/adbc/pkg` directory of the cloned [arrow-adbc](https://github.com/apache/arrow-adbc) repository then run the `make` command.

## Configuration

The following values can be setup in the configuration

- **driverPath** - The path for the Go library. Can be a relative path if using .NET 5.0 or greater, otherwise, it is an absolute path.
- **driverEntryPoint** - The driver entry point. For Snowflake, this is `SnowflakeDriverInit`. 
- **account** - The `adbc.snowflake.sql.account` value from the [Snowflake Client Options](https://arrow.apache.org/adbc/0.5.1/driver/snowflake.html#client-options).
- **user** - The Snowflake user name.
- **password** - The Snowflake password, if using `auth_snowflake` for the auth type.
- **warehouse** - The `adbc.snowflake.sql.warehouse` value from the [Snowflake Client Options](https://arrow.apache.org/adbc/0.5.1/driver/snowflake.html#client-options).
- **authenticationType** - The `adbc.snowflake.sql.auth_type` value from the [Snowflake Client Options](https://arrow.apache.org/adbc/0.5.1/driver/snowflake.html#client-options).
- **authenticationTokenPath** - The path to the authentication token file, if using `auth_jwt` for the auth type.
- **query** - The query to use.
- **expectedResults** - The expected number of results from the query.
