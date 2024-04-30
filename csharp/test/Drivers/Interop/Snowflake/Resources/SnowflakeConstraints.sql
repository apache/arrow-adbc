
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

 CREATE OR REPLACE TABLE {ADBC_CATALOG}.{ADBC_SCHEMA}.{ADBC_CONSTRANT_TABLE_1} (
    id INT,
    name VARCHAR(20),
    PRIMARY KEY (id, name)
);

INSERT INTO {ADBC_CATALOG}.{ADBC_SCHEMA}.{ADBC_CONSTRANT_TABLE_1} (id, name)
VALUES
(1, 'snowflake'),
(2, 'spark');

CREATE OR REPLACE TABLE {ADBC_CATALOG}.{ADBC_SCHEMA}.{ADBC_CONSTRANT_TABLE_2} (
    id INT UNIQUE,
    company_name VARCHAR(30) PRIMARY KEY,
    database_id INT,
    database_name VARCHAR(20),
    CONSTRAINT ADBC_FKEY FOREIGN KEY (database_id, database_name) REFERENCES {ADBC_CATALOG}.{ADBC_SCHEMA}.{ADBC_CONSTRANT_TABLE_1} (id, name)
);

INSERT INTO {ADBC_CATALOG}.{ADBC_SCHEMA}.{ADBC_CONSTRANT_TABLE_2} (id, company_name, database_id, database_name)
VALUES
(6, 'Snowflake Inc', 1, 'snowflake'),
(7, 'The Apache Software Foundation', 2, 'spark');
