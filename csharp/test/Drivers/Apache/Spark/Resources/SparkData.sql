DROP TABLE IF EXISTS {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE};

CREATE TABLE IF NOT EXISTS {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (
  id LONG,
  number DOUBLE,
  decimal NUMERIC(38, 9),
  is_active BOOLEAN,
  name STRING,
  data BINARY,
  date DATE,
  timestamp TIMESTAMP_NTZ,
  timestamp_local TIMESTAMP_LTZ,
  numbers ARRAY<LONG>,
  person STRUCT <
    name STRING,
    age LONG
  >
);

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (
    id, number, decimal,
    is_active,
    name, data,
    date, timestamp, timestamp_local,
    numbers,
    person
)
VALUES (
    1, 1.23, 4.56,
    TRUE,
    'John Doe',
    -- hex-encoded value `abc123`
    X'616263313233',
    '2023-09-08', '2023-09-08 12:34:56', '2023-09-08 12:34:56+00:00',
    ARRAY(1, 2, 3),
    STRUCT('John Doe', 30)
);

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (
    id, number, decimal,
    is_active,
    name, data,
    date, timestamp, timestamp_local,
    numbers,
    person
)
VALUES (
    2, 1.7976931348623157e+308, 9.99999999999999999999999999999999E+28BD,
    FALSE,
    'Jane Doe',
    -- hex-encoded `def456`
    X'646566343536',
    '2023-09-09', '2023-09-09 13:45:57', '2023-09-09 13:45:57+00:00',
    ARRAY(4, 5, 6),
    STRUCT('Jane Doe', 40)
);
