 -- Licensed to the Apache Software Foundation (ASF) under one or more
 -- contributor license agreements.  See the NOTICE file distributed with
 -- this work for additional information regarding copyright ownership.
 -- The ASF licenses this file to You under the Apache License, Version 2.0
 -- (the "License"); you may not use this file except in compliance with
 -- the License.  You may obtain a copy of the License at

 --    http://www.apache.org/licenses/LICENSE-2.0

 -- Unless required by applicable law or agreed to in writing, software
 -- distributed under the License is distributed on an "AS IS" BASIS,
 -- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 -- See the License for the specific language governing permissions and
 -- limitations under the License.

CREATE TABLE IF NOT EXISTS {TABLE_NAME}
(
    c_tinyint TINYINT,
    c_smallint SMALLINT,
    c_int INT,
    c_bigint BIGINT,
    c_decimal DECIMAL(10,2),
    c_float FLOAT,
    c_double DOUBLE,
    c_string STRING,
    c_binary BINARY,
    c_boolean BOOLEAN,
    c_date DATE,
    c_timestamp TIMESTAMP,
    c_timestamp_ntz TIMESTAMP_NTZ,
    c_interval_ym INTERVAL YEAR TO MONTH,
    c_interval_ds INTERVAL DAY TO SECOND,
    c_array ARRAY<INT>,
    c_map MAP<STRING, INT>,
    c_struct STRUCT<f1: STRING, f2: INT, f3: ARRAY<DOUBLE>>,
    c_variant VARIANT,
    c_void VOID,
    PRIMARY KEY (c_string, c_int),
    FOREIGN KEY (`c_bigint`, `c_boolean`) REFERENCES
    `{CATALOG_NAME}`.`{SCHEMA_NAME}`.`{REF_TABLE_NAME}` (`r_bigint`, `r_boolean`)
)
