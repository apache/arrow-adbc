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

# Thrift-based Apache connectors

This library contains code for ADBC drivers built on top of the Thrift protocol with Arrow support:

- Hive
- Impala
- Spark

Each driver is at a different state of implementation.

## Custom generation

Typically, [Thrift](https://thrift.apache.org/) code is generated from the Thrift compiler. And that is mostly true here as well. However, some files were further edited to include Arrow support. These contain the phrase `BUT THIS FILE HAS BEEN HAND EDITED TO SUPPORT ARROW SO REGENERATE AT YOUR OWN RISK` at the top. Some of these files include:

```
arrow-adbc/csharp/src/Drivers/Apache/Thrift/Service/Rpc/Thrift/TBinaryColumn.cs
arrow-adbc/csharp/src/Drivers/Apache/Thrift/Service/Rpc/Thrift/TBoolColumn.cs
arrow-adbc/csharp/src/Drivers/Apache/Thrift/Service/Rpc/Thrift/TByteColumn.cs
arrow-adbc/csharp/src/Drivers/Apache/Thrift/Service/Rpc/Thrift/TDoubleColumn.cs
arrow-adbc/csharp/src/Drivers/Apache/Thrift/Service/Rpc/Thrift/TI16Column.cs
arrow-adbc/csharp/src/Drivers/Apache/Thrift/Service/Rpc/Thrift/TI32Column.cs
arrow-adbc/csharp/src/Drivers/Apache/Thrift/Service/Rpc/Thrift/TI64Column.cs
arrow-adbc/csharp/src/Drivers/Apache/Thrift/Service/Rpc/Thrift/TStringColumn.cs
```

# Hive

The Hive classes serve as the base class for Spark and Impala, since both of those platform implement Hive capabilities.

Core functionality of the Hive classes beyond the base library implementation is under development, has limited functionality, and may produce errors.

# Impala

The Imapala classes are under development, have limited functionality, and may produce errors.

# Spark

The Spark classes are intended for use against native Spark.

For more details, see [Spark Driver](Spark/README.md)

## Known Limitations

1. The API `SparkConnection.GetObjects` is not fully tested at this time
   1. It may not return all catalogs and schema in the server.
   1. It may throw an exception when returning object metadata from multiple catalog and schema.
1. When a `NULL` value is returned for a `BINARY` type it is instead being returned as an empty array instead of the expected `null`.
1. Result set metadata does not provide information about the nullability of each column. They are marked as `nullable`    by default, which may not be accurate.
1. The **Impala** driver is untested and is currently unsupported.
1. The underlying Thrift interface only supports little-endian platforms.
