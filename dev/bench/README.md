<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Simple Python Benchmark script

Connection parameters need to be filled in before the script can be run. The intent
is for this to be a simple enough script to provide iterations on running a `SELECT`
query solely for testing data transfer and memory usage rates for simple queries.

The initial sample here is designed for testing against Snowflake, and so contains
functions for testing the ADBC Snowflake driver, the [snowflake-python-connector](https://pypi.org/project/snowflake-connector-python/), and using ODBC via pyodbc.

If `matplotlib` is installed, it will also draw the timing and memory usage up as
charts which can be saved.
