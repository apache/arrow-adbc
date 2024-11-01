
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

CREATE OR REPLACE TABLE IF NOT EXISTS {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (
  id LONG,
  byte BYTE,
  short SHORT,
  integer INT,
  float FLOAT,
  number DOUBLE,
  decimal NUMERIC(38, 9),
  is_active BOOLEAN,
  name STRING,
  data BINARY,
  date DATE,
  timestamp TIMESTAMP,
  numbers ARRAY<LONG>,
  person STRUCT <
    name STRING,
    age LONG
  >,
  map MAP <
    INT,
    STRING
  >,
  varchar VARCHAR(255),
  char CHAR(10)
);

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE}
    SELECT
    1,
    2, 3, 4, 7.89, 1.23, 4.56,
    TRUE,
    'John Doe',
    -- hex-encoded value `abc123`
    X'616263313233',
    to_date('2023-09-08'), to_timestamp('2023-09-08T12:34:56.000Z'),
    ARRAY(1, 2, 3),
    STRUCT('John Doe', 30),
    MAP(1, 'John Doe'),
    'John Doe',
    'John Doe'
;

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE}
    SELECT
    2,
    127, 32767, 2147483647, 3.4028234663852886e+38, 1.7976931348623157e+308, 9.99999999999999999999999999999999E+28BD,
    FALSE,
    'Jane Doe',
    -- hex-encoded `def456`
    X'646566343536',
    to_date('2023-09-09'), to_timestamp('2023-09-09T13:45:57.000Z'),
    ARRAY(4, 5, 6),
    STRUCT('Jane Doe', 40),
    MAP(1, 'John Doe'),
    'Jane Doe',
    'Jane Doe'
;

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE}
    SELECT
    3,
    -128, -32768, -2147483648, -3.4028234663852886e+38, -1.7976931348623157e+308, -9.99999999999999999999999999999999E+28BD,
    FALSE,
    'Jack Doe',
    -- hex-encoded `def456`
    X'646566343536',
    to_date('1556-01-02'), to_timestamp('1970-01-01T00:00:00.000Z'),
    ARRAY(7, 8, 9),
    STRUCT('Jack Doe', 50),
    MAP(1, 'John Doe'),
    'Jack Doe',
    'Jack Doe'
;

-- UPDATE {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE}
--     SET short = 0
--     WHERE id = 3;

-- DELETE FROM {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE}
--     WHERE id = 3;
