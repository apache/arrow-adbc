
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

DROP TABLE IF EXISTS {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE};

-- Note:
-- Impala supports complex type (ARRAY, MAP, STRUCT),  BUT,
-- there is no way to dynamically load complex data. It needs to be imported from a PARQUET file.
CREATE TABLE IF NOT EXISTS {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (
  id BIGINT,
  byte TINYINT,
  short SMALLINT,
  intcol INTEGER,
  floatcol FLOAT,
  number DOUBLE,
  deccol DECIMAL(38, 9),
  is_active BOOLEAN,
  name STRING,
  bincol BINARY,
  datacol DATE,
  timestampcol TIMESTAMP,
  --numbers ARRAY<BIGINT>,
  --person STRUCT <
  --  name: STRING,
  --  age: BIGINT
  -->,
  --mapcol MAP <
  --  INTEGER,
  --  STRING
  -->,
  varcharcol VARCHAR(255),
  charcol CHAR(10)
) STORED AS PARQUET;

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (
    id,
    byte, short, intcol, floatcol, number, deccol,
    is_active,
    name, bincol,
    datacol, timestampcol,
    --numbers,
    --person,
    --mapcol,
    varcharcol,
    charcol
)
VALUES (
    1,
    2, 3, 4, 7.89, 1.23, 4.56,
    TRUE,
    'John Doe',
    -- hex-encoded value `abc123`
    --X'616263313233',
    CAST('abc123' as BINARY),
    '2023-09-08', '2023-09-08 12:34:56',
    --ARRAY(1, 2, 3),
    --STRUCT('John Doe', 30),
    --MAP(1, 'John Doe'),
    CAST('John Doe' as VARCHAR(255)),
    CAST('John Doe' as CHAR(10))
);

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (
    id,
    byte, short, intcol, floatcol, number, deccol,
    is_active,
    name, bincol,
    datacol, timestampcol,
    --numbers,
    --person,
    --mapcol,
    varcharcol,
    charcol
)
VALUES (
    2,
    127, 32767, 2147483647, CAST(3.4028234663852886e+38 as FLOAT), 1.7976931348623157e+308, 9.99999999999999999999999999999999E+28BD,
    FALSE,
    'Jane Doe',
    -- hex-encoded `def456`
    --X'646566343536',
    CAST('def456' as BINARY),
    '2023-09-09', '2023-09-09 13:45:57',
    --ARRAY(4, 5, 6),
    --STRUCT('Jane Doe', 40),
    --MAP(1, 'John Doe'),
    CAST('Jane Doe' as VARCHAR(255)),
    CAST('Jane Doe' as CHAR(10))
);

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (
    id,
    byte, short, intcol, floatcol, number, deccol,
    is_active,
    name, bincol,
    datacol, timestampcol,
    --numbers,
    --person,
    --mapcol,
    varcharcol,
    charcol
)
VALUES (
    3,
    -128, -32768, -2147483648, CAST(-3.4028234663852886e+38 as FLOAT), -1.7976931348623157e+308, -9.99999999999999999999999999999999E+28BD,
    FALSE,
    'Jack Doe',
    -- hex-encoded `def456`
    --X'646566343536',
    CAST('def456' as BINARY),
    '1556-01-02', '1970-01-01 00:00:00',
    --ARRAY(7, 8, 9),
    --STRUCT('Jack Doe', 50),
    --MAP(1, 'John Doe'),
    CAST('Jack Doe' as VARCHAR(255)),
    CAST('Jack Doe' as CHAR(10))
);

--UPDATE {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE}
--    SET short = 0
--    WHERE id = 3;

--DELETE FROM {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE}
--    WHERE id = 3;
