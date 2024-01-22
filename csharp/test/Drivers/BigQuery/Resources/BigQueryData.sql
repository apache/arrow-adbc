
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

CREATE OR REPLACE TABLE {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (
  id INT64,
  number FLOAT64,
  decimal NUMERIC(38, 9),
  big_decimal BIGNUMERIC(76, 38),
  is_active BOOL,
  name STRING,
  data BYTES,
  date DATE,
  time TIME,
  datetime DATETIME,
  timestamp TIMESTAMP,
  point geography,
  numbers ARRAY<INT64>,
  person STRUCT <
    name STRING,
    age INT64
  >
);

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (
    id, number, decimal,
    big_decimal,
    is_active,
    name, data,
    date, time, datetime, timestamp,
    point, numbers,
    person
)
VALUES (
    1, 1.23, 4.56,
    7.89,
    TRUE,
    'John Doe',
    -- base64 encoded value `abc123`
    FROM_BASE64('YWJjMTIz'),
    '2023-09-08', '12:34:56', '2023-09-08 12:34:56', '2023-09-08 12:34:56+00:00',
    ST_GEOGPOINT(1, 2),
    ARRAY[1, 2, 3],
    STRUCT('John Doe', 30)
);

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (
    id, number, decimal,
    big_decimal,
    is_active,
    name, data,
    date, time, datetime, timestamp,
    point, numbers,
    person
)
VALUES (
    2, 1.7976931348623157e+308, 9.99999999999999999999999999999999E+28,
    5.7896044618658097711785492504343953926634992332820282019728792003956564819968E+37,
    FALSE,
    'Jane Doe',
    -- base64 encoded `def456`
    FROM_BASE64('ZGVmNDU2'),
    '2023-09-09', '13:45:57', '2023-09-09 13:45:57', '2023-09-09 13:45:57+00:00',
    ST_GEOGPOINT(3, 4),
    ARRAY[4, 5, 6],
    STRUCT('Jane Doe', 40)
);
